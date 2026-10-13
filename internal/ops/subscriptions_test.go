package ops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

func subscribeOne(t *testing.T, e *env, d *device) (string, string, map[string]any) {
	t.Helper()
	sub := e.fact(d, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed"})
	e.push(d, sub)
	p := contract.PodcastID(e.namespace, sub["op_id"].(string))
	g := contract.GenerationID(e.namespace, p, sub["op_id"].(string))
	return p, g, sub
}

// The latest generation is the end of the predecessor chain, not the one
// with the highest rank: a re-subscription ordered before the unsubscription
// it observed still follows it.
func TestResolutionFollowsPredecessorChain(t *testing.T) {
	e := newEnv(t)
	a, b := e.device("A"), e.device("B")
	p, g1, _ := subscribeOne(t, e, a)
	e.clock += 100
	e.push(a, e.fact(a, "podcast.unsubscribe", nil, podTarget(p, g1), map[string]any{}))
	e.clock -= 1000
	sub2 := e.fact(b, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed",
		"observed": map[string]any{"podcast_id": p, "generation_id": g1, "state": "removed"}})
	if r := e.push(b, sub2); outcome(r[0]) != "applied" {
		t.Fatal(r)
	}
	g2 := contract.GenerationID(e.namespace, p, sub2["op_id"].(string))
	e.push(b, e.fact(b, "podcast.unsubscribe", nil, podTarget(p, g2), map[string]any{}))
	sub3 := e.fact(b, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed",
		"observed": map[string]any{"podcast_id": p, "generation_id": g2, "state": "removed"}})
	if r := e.push(b, sub3); outcome(r[0]) != "applied" {
		t.Fatalf("re-subscription observing the latest generation was %s; subscription=%s", outcome(r[0]), e.records()["podcast/"+p+"/subscription"])
	}
}

// Credential URLs are refused before anything is stored, whatever the
// envelope's validity or the field, in both transports.
func TestCredentialURLsRefusedBeforeArchiving(t *testing.T) {
	const marker = "test-secret-marker"
	e := newEnv(t)
	a := e.device("A")
	p, g, _ := subscribeOne(t, e, a)
	ref := e.episodeRef(p, g, "one")
	ref["occurrence"] = map[string]any{"k": "enclosure", "v": "https://user:" + marker + "@example.com/one.mp3"}
	malformed := e.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://user:" + marker + "@example.com/feed"})
	malformed["author_seq"] = int64(0)
	cases := map[string]map[string]any{
		"invalid envelope":    malformed,
		"occurrence identity": e.fact(a, "favorite.set", nil, epTarget(ref), map[string]any{"episode": ref, "on": true}),
		"unknown type":        e.fact(a, "future.type", nil, []any{}, map[string]any{"note": "ftp://" + marker + "@host/x"}),
	}
	for name, f := range cases {
		archived, err := protocol.ParseFact(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.rawPush(a, []protocol.PushOp{{InstallSeq: a.seq + 1, Fact: archived}}, nil); err == nil {
			t.Fatalf("%s: credential fact accepted", name)
		}
		canonical, _ := contract.Canonical(f)
		hash, _ := contract.ContentHash(f)
		tx := newID()
		half := len(canonical) / 2
		if r, err := e.rawPush(a, []protocol.PushOp{{InstallSeq: a.seq + 1, Part: &protocol.Part{TransactionID: tx, Index: 0, Count: 2, ContentHash: hash, Data: canonical[:half]}}}, nil); err != nil || r.Results[0].Status != "part_stored" {
			t.Fatalf("%s: first part: %+v %v", name, r, err)
		}
		r, err := e.rawPush(a, []protocol.PushOp{{InstallSeq: a.seq + 1, Part: &protocol.Part{TransactionID: tx, Index: 1, Count: 2, ContentHash: hash, Data: canonical[half:]}}}, nil)
		if err != nil || r.Results[0].Status != "invalid_part" {
			t.Fatalf("%s: completed credential fact: %+v %v", name, r, err)
		}
		if bytes.Contains(e.dataFiles(), []byte(marker)) {
			t.Fatalf("%s: credentials reached the database in clear", name)
		}
	}
}

func TestCredentialURLDetection(t *testing.T) {
	for s, want := range map[string]bool{
		"https://u:p@h/x":                               true,
		"https://u@h":                                   true,
		"see ftp://u@h/x":                               true,
		"https://user:\"pw@example.com/feed":            true, // one URL: quotes do not end it
		" https://u:p@h/feed":                           true,
		"<a href=\"https://u:p@h/\">x</a>":              true,
		"https://h/x@y":                                 false,
		"https://h/?a=b@c":                              false,
		"https://h#a@b":                                 false,
		"mail me@example.com":                           false,
		"://u@h":                                        false,
		"Follow https://social.example/@cast":           false,
		"https://h and me@example.com":                  false,
		"Website https://example.com\n@ourpodcast":      false,
		"<a href='https://example.com'>@ourpodcast</a>": false,
		"Homepage https://example.com\tcontact@h.com":   false,
		"Site https://example.com\u00a0@pod":            false,
	} {
		if got := protocol.ContainsCredentialURL(s); got != want {
			t.Errorf("%q: %v, want %v", s, got, want)
		}
	}
}

// A valid dismissal is applied even when its conditional did not fire.
func TestDismissalAlwaysApplied(t *testing.T) {
	e := newEnv(t)
	a, b := e.device("A"), e.device("B")
	p, g, _ := subscribeOne(t, e, a)
	ref := e.episodeRef(p, g, "one")
	e.clock += 10
	e.push(a, e.fact(a, "episode.restore_state", nil, epTarget(ref), map[string]any{"episode": ref, "play_state": "new", "position": json.Number("0")}))
	e.clock += 20
	e.push(a, e.fact(a, "episode.mark_played", nil, epTarget(ref), map[string]any{"episode": ref, "reset_position": false}))
	e.clock -= 10
	if r := e.push(b, e.fact(b, "inbox.dismiss", nil, epTarget(ref), map[string]any{"episodes": []any{ref}})); outcome(r[0]) != "applied" {
		t.Fatalf("valid dismissal returned %s", outcome(r[0]))
	}
}

// Record provenance does not depend on arrival order.
func TestProvenanceIndependentOfArrival(t *testing.T) {
	base := newEnv(t)
	a, b, c := base.device("A"), base.device("B"), base.device("C")
	p, g, sub := subscribeOne(t, base, a)
	ref := base.episodeRef(p, g, "one")
	identity := base.fact(a, "episode.mark_unplayed", nil, epTarget(ref), map[string]any{"episode": ref})
	base.clock += 10
	first := base.fact(b, "favorite.set", nil, epTarget(ref), map[string]any{"episode": ref, "on": true})
	base.clock += 10
	last := base.fact(c, "favorite.set", nil, epTarget(ref), map[string]any{"episode": ref, "on": true})
	var got []string
	for _, rev := range []bool{false, true} {
		e := newEnvWithAccount(t, base)
		e.clock = base.clock
		e.push(e.device("A"), sub, identity)
		if rev {
			e.push(e.device("C"), last)
			e.push(e.device("B"), first)
		} else {
			e.push(e.device("B"), first)
			e.push(e.device("C"), last)
		}
		e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
			rows, err := r.Account(e.account).Records(nil)
			for _, x := range rows {
				if x.FieldGroup == "favorite" {
					got = append(got, x.Provenance)
				}
			}
			return err
		})
	}
	if got[0] != got[1] {
		t.Fatalf("same final facts yield different provenance: %v", got)
	}
}

// A malformed secret reference is rejected, not waited for.
func TestMalformedSecretReferenceRejected(t *testing.T) {
	e := newEnv(t)
	a := e.device("A")
	p, g, _ := subscribeOne(t, e, a)
	for _, f := range []map[string]any{
		e.fact(a, "podcast.set_credentials", nil, podTarget(p, g), map[string]any{"secret_id": "not-a-uuid"}),
		e.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/other", "credentials_secret": "not-a-uuid"}),
	} {
		if r := e.push(a, f); outcome(r[0]) != "rejected/invalid_payload" {
			t.Fatalf("%s: %+v", f["type"], r[0])
		}
	}
}

// Closing a podcast with many episode scopes stays fast: refolding one scope
// must not walk the account's whole rank index (it took over a minute for
// 5,000 scopes when it did).
func TestUnsubscribeManyEpisodeScopes(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("load test (make check runs it without the race detector)")
	}
	const n = 5000
	e := newEnv(t)
	d := e.device("A")
	e.derivedWithoutArchive()
	p, g, sub := subscribeOne(t, e, d)
	err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		a := tx.Account(e.account)
		prev := sub["op_id"].(string)
		for i := 0; i < n; i++ {
			node := newID()
			ep, _ := contract.EpisodeID(e.namespace, p, contract.KindRSSGUID, fmt.Sprint(i))
			if err := a.InsertNode(node, "fact", e.clock+int64(i+1), []string{prev}); err != nil {
				return err
			}
			prev = node
			if err := a.AddEpisode(ep, p, `{"k":"rss-guid","v":"x"}`, ""); err != nil {
				return err
			}
			if err := a.Contribute(protocol.EpisodeScope(g, ep), node, `{"kind":"mark_played","reset_position":true}`); err != nil {
				return err
			}
			for _, group := range []string{"play_state", "position"} {
				if err := a.PutRecord(storage.RecordRow{Entity: "episode", EntityID: ep, FieldGroup: group, Value: `{}`, Provenance: `[]`}); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	e.clock += n + 100
	start := time.Now()
	r := e.push(d, e.fact(d, "podcast.unsubscribe", nil, podTarget(p, g), map[string]any{}))
	if outcome(r[0]) != "applied" {
		t.Fatal(r)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Fatalf("unsubscribing %d episode scopes took %s", n, elapsed)
	}
}
func TestPublicTextWithURLsAccepted(t *testing.T) {
	for _, title := range []string{
		"Follow https://social.example/@podcast",
		"Website https://example.com\n@ourpodcast",
		"<a href='https://example.com'>@ourpodcast</a>",
		"Homepage https://example.com\tcontact@example.com",
	} {
		t.Run(title, func(t *testing.T) {
			e := newEnv(t)
			d := e.device("A")
			p, g, _ := subscribeOne(t, e, d)
			ref := e.episodeRef(p, g, "one")
			ref["occurrence"].(map[string]any)["title"] = title
			f := e.fact(d, "favorite.set", nil, epTarget(ref), map[string]any{"episode": ref, "on": true})
			a, err := protocol.ParseFact(f)
			if err != nil {
				t.Fatal(err)
			}
			r, err := e.rawPush(d, []protocol.PushOp{{InstallSeq: d.seq + 1, Fact: a}}, nil)
			if err != nil {
				t.Fatalf("credential-free episode title blocks favorite: %v", err)
			}
			if outcome(r.Results[0]) != "applied" {
				t.Fatal(r)
			}
		})
	}
}

func TestQuotedCredentialRefused(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		for _, framed := range []bool{false, true} {
			t.Run(map[bool]string{false: "valid-envelope/", true: "invalid-envelope/"}[malformed]+map[bool]string{false: "unframed", true: "framed"}[framed], func(t *testing.T) {
				e := newEnv(t)
				d := e.device("A")
				const marker = "quoted-password-marker"
				f := e.fact(d, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://user:\"" + marker + "@example.com/feed"})
				if malformed {
					f["author_seq"] = int64(0)
				}
				a, err := protocol.ParseFact(f)
				if err != nil {
					t.Fatal(err)
				}
				op := protocol.PushOp{InstallSeq: 1, Fact: a}
				if framed {
					b, _ := contract.Canonical(f)
					hash, _ := contract.ContentHash(f)
					op = protocol.PushOp{InstallSeq: 1, Part: &protocol.Part{TransactionID: newID(), Index: 0, Count: 1, ContentHash: hash, Data: b}}
				}
				r, err := e.rawPush(d, []protocol.PushOp{op}, nil)
				if bytes.Contains(e.dataFiles(), []byte(marker)) {
					t.Fatalf("password archived in plaintext; framed=%v response=%+v err=%v", framed, r, err)
				}
			})
		}
	}
}

func TestConcurrentResubscriptionsShareGeneration(t *testing.T) {
	e := newEnv(t)
	a, b, c := e.device("A"), e.device("B"), e.device("C")
	p, g1, _ := subscribeOne(t, e, a)
	e.clock += 100
	e.push(a, e.fact(a, "podcast.unsubscribe", nil, podTarget(p, g1), map[string]any{}))
	e.clock -= 1000
	sub2 := e.fact(b, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed", "observed": map[string]any{"podcast_id": p, "generation_id": g1, "state": "removed"}})
	e.push(b, sub2)
	g2 := contract.GenerationID(e.namespace, p, sub2["op_id"].(string))
	e.clock -= 1000
	e.push(c, e.fact(c, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed", "observed": map[string]any{"podcast_id": p, "generation_id": g1, "state": "removed"}}))
	err := e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
		gens, err := tx.Account(e.account).Generations(p)
		if err != nil {
			return err
		}
		if len(gens) != 2 {
			t.Fatalf("concurrent subscribe created %d generations", len(gens))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.records()["podcast/"+p+"/subscription"]; got != js(map[string]any{"generation_id": g2, "state": "subscribed"}) {
		t.Fatal(got)
	}
	e.push(b, e.fact(b, "podcast.unsubscribe", nil, podTarget(p, g2), map[string]any{}))
	retry := e.fact(c, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed", "observed": map[string]any{"podcast_id": p, "generation_id": g2, "state": "removed"}})
	if r := e.push(c, retry); outcome(r[0]) != "applied" {
		t.Fatal(r)
	}
}

func TestProvenancePublication(t *testing.T) {
	e := newEnv(t)
	a, b := e.device("A"), e.device("B")
	p, g, _ := subscribeOne(t, e, a)
	ref := e.episodeRef(p, g, "one")
	e.push(a, e.fact(a, "favorite.set", nil, epTarget(ref), map[string]any{"episode": ref, "on": true}))
	e.clock += 100
	winner := e.fact(a, "favorite.set", nil, epTarget(ref), map[string]any{"episode": ref, "on": true})
	e.push(a, winner)
	check := func(wantRecords int) {
		t.Helper()
		err := e.svc.Store.Read(context.Background(), func(r *storage.ReadTx) error {
			commits, err := r.Account(e.account).Commits(e.head().Seq-1, e.head().Seq, 1)
			if err != nil {
				return err
			}
			v, err := contract.DecodeJSON(commits[0].Canonical)
			if err != nil {
				return err
			}
			records := v.(map[string]any)["records"].([]any)
			if len(records) != wantRecords {
				t.Fatalf("got %d records, expected %d: %v", len(records), wantRecords, records)
			}
			if wantRecords > 0 && js(records[0].(map[string]any)["provenance"]) != js([]any{winner["op_id"]}) {
				t.Fatal(records)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	check(1)
	e.clock -= 50
	e.push(b, e.fact(b, "favorite.set", nil, epTarget(ref), map[string]any{"episode": ref, "on": true}))
	check(0)
	e.push(a, e.fact(a, "podcast.set_policy", nil, podTarget(p, g), map[string]any{"keep_updated": true}))
	e.push(a, e.fact(a, "podcast.archive", nil, podTarget(p, g), map[string]any{}))
	// Refolding the podcast must not republish policy just because map iteration shuffled its writers.
	for i := 0; i < 25; i++ {
		e.push(a, e.fact(a, "podcast.archive", nil, podTarget(p, g), map[string]any{}))
		check(0)
	}
}
