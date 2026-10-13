package importer

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Result is what an import did, for the result screen (PROTOCOL §11.5).
type Result struct {
	Imported  Counts  `json:"imported"`
	Skipped   []Entry `json:"skipped"`
	Ambiguous []Entry `json:"ambiguous"`
	Merged    []Entry `json:"merged"`
}

// Counts are the imported totals.
type Counts struct {
	Podcasts  int `json:"podcasts"`
	Episodes  int `json:"episodes"`
	Queue     int `json:"queue"`
	Favorites int `json:"favorites"`
	History   int `json:"history"`
	Inbox     int `json:"inbox"`
}

// Apply applies a plan's podcasts to the account through svc, one server
// commit each (with its credentials), starting at podcast from (a resumed
// import's progress is the number of its import.podcast decisions).
func Apply(ctx context.Context, svc *protocol.Service, accountID string, plan Plan, from int) ([]ops.ImportedPodcast, error) {
	var out []ops.ImportedPodcast
	for i := from; i < len(plan.Podcasts); i++ {
		p := plan.Podcasts[i]
		var got ops.ImportedPodcast
		err := svc.ServerCommit(ctx, accountID, func(c *protocol.ApplyContext) error {
			if p.SecretID != "" {
				if err := svc.StoreSecrets(c.Account, c.AccountID, []protocol.Secret{plan.Secrets[p.SecretID]}); err != nil {
					return err
				}
			}
			var err error
			got, err = ops.ApplyImportPodcast(c, p)
			return err
		})
		if err != nil {
			return out, err
		}
		out = append(out, got)
	}
	return out, nil
}

// Complete records the applied plan's manifest, the import's completing
// decision (where its queue is placed, in the backup's global order:
// PROTOCOL §7.6), as the import job does after Apply, and
// returns the result as WriteManifest does.
func Complete(ctx context.Context, svc *protocol.Service, accountID, importID, fingerprint string, plan Plan) (string, error) {
	res, err := Summarize(ctx, svc.Store, accountID, plan)
	if err != nil {
		return "", err
	}
	return WriteManifest(ctx, svc, accountID, importID, fingerprint, plan, res)
}

// Progress is how many of the plan's podcasts the store already holds.
func Progress(ctx context.Context, store *storage.Store, accountID string) (int, error) {
	n := 0
	err := store.Read(ctx, func(r *storage.ReadTx) error {
		ds, err := r.Account(accountID).DecisionsOfType("import.podcast")
		n = len(ds)
		return err
	})
	return n, err
}

// Summarize builds the import's result from the decisions it made (so a
// resumed import reports everything) and the plan's skipped and merged
// feeds, in the shapes of docs/CONTRACT.md "Import vectors", "result".
func Summarize(ctx context.Context, store *storage.Store, accountID string, plan Plan) (Result, error) {
	res := Result{Skipped: orEmpty(plan.Skipped), Merged: append([]Entry{}, plan.Merged...), Ambiguous: []Entry{}}
	err := store.Read(ctx, func(r *storage.ReadTx) error {
		a := r.Account(accountID)
		podcasts, err := a.DecisionsOfType("import.podcast")
		if err != nil {
			return err
		}
		res.Imported.Podcasts = len(podcasts)
		titles := map[string]string{} // import.podcast decision → podcast title
		for _, raw := range podcasts {
			d, err := decodeDecision(raw)
			if err != nil {
				return err
			}
			result, _ := d["result"].(map[string]any)
			id, _ := d["decision_id"].(string)
			titles[id], _ = result["title"].(string)
		}
		episodes, err := a.DecisionsOfType("import.episode")
		if err != nil {
			return err
		}
		for _, raw := range episodes {
			d, err := decodeDecision(raw)
			if err != nil {
				return err
			}
			result, _ := d["result"].(map[string]any)
			inputs, _ := d["inputs"].(map[string]any)
			im, _ := result["imported"].(map[string]any)
			title, _ := result["title"].(string)
			podcast := ""
			if deps, _ := d["dependencies"].([]any); len(deps) > 0 {
				dep, _ := deps[0].(string)
				podcast = titles[dep]
			}
			res.Imported.Episodes++
			if _, ok := result["queue_key"]; ok {
				res.Imported.Queue++
			}
			if im["favorite"] == true {
				res.Imported.Favorites++
			}
			if im["history"] != nil {
				res.Imported.History++
			}
			if im["play_state"] == "new" {
				res.Imported.Inbox++
			}
			var items []int64
			list, _ := inputs["source_items"].([]any)
			for _, x := range list {
				if n, ok := x.(json.Number); ok {
					v, _ := n.Int64()
					items = append(items, v)
				}
			}
			// Ambiguous: neither GUID nor enclosure (its occurrence kind is
			// "none"). An e0: ID allocated to a release is not ambiguous.
			occurrence, _ := result["occurrence"].(map[string]any)
			if id, _ := result["episode_id"].(string); strings.HasPrefix(id, "e0:") && occurrence["k"] == "none" && len(items) > 0 {
				res.Ambiguous = append(res.Ambiguous, Entry{Reason: "no_guid_no_enclosure", Podcast: podcast, Title: title, SourceItemID: items[0]})
			}
			if len(items) > 1 {
				res.Merged = append(res.Merged, Entry{Kind: "episode", Podcast: podcast, Title: title, SourceItemIDs: items})
			}
		}
		return nil
	})
	return res, err
}

func decodeDecision(raw []byte) (map[string]any, error) {
	v, err := contract.DecodeJSON(raw)
	if err != nil {
		return nil, err
	}
	d, _ := v.(map[string]any)
	return d, nil
}

// WriteManifest records the import's manifest decision (PROTOCOL §11.5) unless
// the store already holds it, and returns the result as canonical JSON.
func WriteManifest(ctx context.Context, svc *protocol.Service, accountID, importID, fingerprint string, plan Plan, res Result) (string, error) {
	value, err := resultValue(res)
	if err != nil {
		return "", err
	}
	b, err := contract.Canonical(value)
	if err != nil {
		return "", err
	}
	err = svc.ServerCommit(ctx, accountID, func(c *protocol.ApplyContext) error {
		if id, err := c.Account.DecisionBySubject("import.manifest", "import"); err != nil {
			return err
		} else if id != "" {
			return nil
		}
		_, err := ops.ApplyImportManifest(c, importID, map[string]any{"import_id": importID, "kind": plan.Kind, "fingerprint": fingerprint,
			"result": value})
		return err
	})
	return string(b), err
}

func orEmpty(e []Entry) []Entry {
	if e == nil {
		return []Entry{}
	}
	return e
}

// resultValue is the result as a canonical JSON value (the manifest's). A
// number out of the contract's range is an error, never a panic.
func resultValue(r Result) (map[string]any, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	v, err := contract.DecodeJSON(b)
	if err != nil {
		return nil, err
	}
	return v.(map[string]any), nil
}
