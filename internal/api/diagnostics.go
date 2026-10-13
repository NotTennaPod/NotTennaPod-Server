package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Diagnostics (PROTOCOL §12): what a person needs to understand their
// sync: devices and when they were last seen, recent rejections and why,
// recent recoveries (PROTOCOL §6.11), imports, refresh errors and stored responses a
// re-parse gave up on (PROTOCOL §9.4); and, for one fact, its
// raw and effective author, its derived-branch root, its save identities and
// the cuts covering it. An administrator may look at any account.
// Diagnostics are meant to be shared: every URL in them is cut to
// scheme://host[:port]/….
func (s *Server) diagnosticsRoutes() {
	s.route("/v1/diagnostics", endpoint{method: http.MethodGet, auth: authWeb, handle: (*request).diagnostics})
}

const (
	diagnosticRejections = 50
	diagnosticRecoveries = 10
)

func (req *request) diagnostics() (int, any, error) {
	q, err := req.query("account", "fact")
	if err != nil {
		return 0, nil, err
	}
	account := req.principal.accountID()
	if a := q.Get("account"); a != "" && a != account {
		if !req.principal.cred.IsAdmin {
			return 0, nil, errForbidden
		}
		account = a
	}
	out := map[string]any{"account_id": account}
	err = req.s.cfg.Store.Read(req.ctx, func(r *storage.ReadTx) error {
		// The caller, and for another account the administrator flag, are checked again in the
		// read that serves the diagnostics: a demotion or revocation that landed after
		// authentication wins.
		cred, err := req.revalidateRead(r)
		if err != nil {
			return err
		}
		if account != cred.AccountID && !cred.IsAdmin {
			return errForbidden
		}
		if _, err := r.LoginByAccount(account); errors.Is(err, storage.ErrNotFound) {
			return errNotFound
		} else if err != nil {
			return err
		}
		acct := r.Account(account)
		insts, err := acct.Installations()
		if err != nil {
			return err
		}
		devices := []any{}
		for _, i := range insts {
			devices = append(devices, installationJSON(i, req.principal.cred.InstallID))
		}
		out["devices"] = devices
		rejected, err := acct.RecentRejections(diagnosticRejections)
		if err != nil {
			return err
		}
		list := []any{}
		for _, f := range rejected {
			list = append(list, factSummary(f))
		}
		out["rejections"] = list
		// The recent recoveries (PROTOCOL §6.11): counts, entries that could not be
		// joined (invalid receipts, conflicts) and whether the client believes
		// the recovery complete.
		recoveries, err := acct.RecentRecoveries(diagnosticRecoveries)
		if err != nil {
			return err
		}
		rlist := []any{}
		for _, r := range recoveries {
			rlist = append(rlist, protocol.Summary(r))
		}
		out["recoveries"] = rlist
		imp, err := acct.CurrentImport()
		switch {
		case err == nil:
			out["import"] = map[string]any{"import_id": imp.ImportID, "kind": imp.Kind, "status": imp.Status, "reason": nullable(imp.Reason),
				"file_name": ops.StripUserinfo(imp.FileName), "created_at": imp.CreatedAt}
		case errors.Is(err, storage.ErrNotFound):
			out["import"] = nil
		default:
			return err
		}
		recs, err := acct.Records([]string{"refresh_status", "metadata"})
		if err != nil {
			return err
		}
		titles := map[string]string{}
		for _, rec := range recs {
			if rec.Entity == "podcast" && rec.FieldGroup == "metadata" {
				var m struct{ Title string }
				_ = json.Unmarshal([]byte(rec.Value), &m)
				titles[rec.EntityID] = m.Title
			}
		}
		refresh := []any{}
		for _, rec := range recs {
			if rec.Entity != "podcast" || rec.FieldGroup != "refresh_status" {
				continue
			}
			var v map[string]any
			if err := json.Unmarshal([]byte(rec.Value), &v); err != nil {
				return err
			}
			if v["error"] == nil {
				continue
			}
			refresh = append(refresh, map[string]any{"podcast_id": rec.EntityID, "title": ops.StripUserinfo(titles[rec.EntityID]),
				"status": scrubbed(v)})
		}
		out["refresh_errors"] = refresh
		// Stored responses a re-parse gave up on (PROTOCOL §9.4): their outcome is
		// kept in the clear beside them, so no response is opened for this.
		failures, err := acct.ReparseFailures()
		if err != nil {
			return err
		}
		reparse := []any{}
		for _, f := range failures {
			reparse = append(reparse, map[string]any{"podcast_id": f.PodcastID, "title": ops.StripUserinfo(titles[f.PodcastID]),
				"reason": f.Failure, "timeouts": f.Timeouts, "parser_version": f.Parser})
		}
		out["reparse_failures"] = reparse
		if id := q.Get("fact"); id != "" {
			detail, err := factDiagnostics(acct, id)
			if err != nil {
				return err
			}
			out["fact"] = detail
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	shared, err := reduceAllURLs(out)
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, shared, nil
}

// reduceURLs cuts every URL in s to scheme://host[:port], with "/…" for
// whatever followed: secrets hide in paths and queries as well as in
// userinfo. URLs are found by the server's one rule (ops.FindURLs, the
// userinfo scrub's own scan): character references, scheme-relative starts
// and apostrophes inside a URL included.
func reduceURLs(s string) string {
	urls := ops.FindURLs(s)
	if len(urls) == 0 {
		return s
	}
	var b strings.Builder
	i := 0
	for _, u := range urls {
		if u.Start < i {
			continue // inside the text an earlier URL's cut already took
		}
		b.WriteString(s[i:u.Start])
		if u.Scheme != "" {
			b.WriteString(u.Scheme + ":")
		}
		b.WriteString("//" + u.Host)
		end := literalEnd(s, u)
		if u.HostEnd < end {
			b.WriteString("/…")
		}
		i = end
	}
	b.WriteString(s[i:])
	return b.String()
}

// literalEnd is where a URL's cut ends in the text as written. The scan reads
// character references decoded, so a reference that spells a delimiter (&#32;,
// &#34;, &#60;) ends the URL there, while the text goes on: the rest, a
// fragment's or query's secret included, would stay in the diagnostics. The cut
// therefore runs on to the first delimiter actually written. Taking a few words
// too many is harmless; leaving a secret is not.
func literalEnd(s string, u ops.URL) int {
	before := s[:u.Start]
	quoted := strings.HasSuffix(before, "'") || strings.HasSuffix(before, "&#39;") || strings.HasSuffix(before, "&#x27;") ||
		strings.HasSuffix(before, "&#X27;") || strings.HasSuffix(before, "&apos;")
	end := u.End
	for end < len(s) {
		c := s[end]
		if c <= 0x20 || c == '<' || c == '>' || c == '"' || (quoted && c == '\'') {
			break
		}
		end++
	}
	return end
}

// reduceAllURLs applies reduceURLs to every string of a response, whatever
// its types: it goes through JSON, numbers kept exact.
func reduceAllURLs(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var generic any
	if err := dec.Decode(&generic); err != nil {
		return nil, err
	}
	var walk func(any) any
	walk = func(v any) any {
		switch x := v.(type) {
		case string:
			return reduceURLs(x)
		case map[string]any:
			for k, y := range x {
				x[k] = walk(y)
			}
		case []any:
			for i, y := range x {
				x[i] = walk(y)
			}
		}
		return v
	}
	return walk(generic), nil
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// scrubbed removes URL credentials from every string of a value.
func scrubbed(v any) any {
	switch x := v.(type) {
	case string:
		return ops.StripUserinfo(x)
	case map[string]any:
		out := map[string]any{}
		for k, y := range x {
			out[k] = scrubbed(y)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, y := range x {
			out[i] = scrubbed(y)
		}
		return out
	}
	return v
}

// factSummary is a fact's type, outcome and receipt values, never its payload.
func factSummary(f storage.FactRow) map[string]any {
	var receipt map[string]any
	_ = json.Unmarshal(f.Receipt, &receipt)
	return map[string]any{"op_id": f.OpID, "type": f.Type, "type_version": f.TypeVersion, "outcome": f.Outcome,
		"reason": receipt["reason"], "normalized_time": receipt["normalized_time"], "epoch": f.Epoch, "seq": f.Seq}
}

func factDiagnostics(acct *storage.AccountReader, id string) (map[string]any, error) {
	f, err := acct.ArchivedFact(id)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, fail(http.StatusNotFound, "not_found", "No such fact.")
	}
	if err != nil {
		return nil, err
	}
	out := factSummary(f)
	p, err := acct.Placement(id)
	if err != nil {
		return nil, err
	}
	if p.Placed {
		out["author"] = map[string]any{"raw": p.RawAuthor, "seq": p.AuthorSeq, "effective": p.EffectiveAuthor,
			"branch_parent": nullable(p.BranchParent), "branch_root": nullable(p.BranchRoot)}
	} else {
		out["author"] = nil
	}
	saves, err := acct.SaveIdentities(id)
	if err != nil {
		return nil, err
	}
	list := []any{}
	for _, s := range saves {
		list = append(list, map[string]any{"session_id": s.SessionID, "continuation": s.Continuation, "ordinal": s.Ordinal})
	}
	out["save_identities"] = list
	cuts, err := acct.CutsCovering(id)
	if err != nil {
		return nil, err
	}
	covering := []any{}
	for _, c := range cuts {
		covering = append(covering, factSummary(c))
	}
	out["covered_by"] = covering
	return out, nil
}
