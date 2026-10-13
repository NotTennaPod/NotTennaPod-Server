package ops

import (
	"maps"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// A server fetch at record level (PROTOCOL §9.4; docs/CONTRACT.md "Server
// fetches"), the refresh status rule (PROTOCOL §9.4 point 7) and the parts a fetch
// writes (PROTOCOL §9.4).

// ServerItem is one item of a server fetch as a fetch arrival has it: its
// occurrence (PROTOCOL §3.3: k and v, and the evidence members guid, enclosure_url,
// title and publication_time) and its PROTOCOL §9.4 metadata record fields (without
// revision and fetch_time; URLs without userinfo). The items of one fetch
// come from one feed, so those with a GUID share one kind (rss-guid or
// atom-id); the server derives each item's ID from its evidence (PROTOCOL §9.7).
type ServerItem struct {
	Occurrence map[string]any
	Fields     map[string]any
}

// records is the fetch's parse at record level: the podcast metadata fields
// and the items (nil, nil when the fetch has no parse: a failure or a 304).
func (x ServerFetch) records() (map[string]any, []ServerItem) {
	if x.NotModified {
		return nil, nil
	}
	if x.Feed == nil {
		if x.FeedFields == nil {
			return nil, nil // a failure: no parse
		}
		// The server keeps no URL userinfo, in URLs, GUIDs or text (PROTOCOL §9.4): every string of
		// a record-level parse is cleaned, as a parsed feed's are.
		feedFields, _ := stripAll(normalizeJSON(x.FeedFields)).(map[string]any)
		if x.Items == nil {
			return feedFields, nil
		}
		items := make([]ServerItem, len(x.Items))
		for i, it := range x.Items {
			fields, _ := stripAll(normalizeJSON(it.Fields)).(map[string]any)
			occ, _ := stripAll(normalizeJSON(it.Occurrence)).(map[string]any)
			items[i] = withEvidence(ServerItem{Occurrence: occ, Fields: fields})
		}
		return feedFields, items
	}
	items := make([]ServerItem, 0, len(x.Feed.Items))
	for _, it := range x.Feed.Items {
		items = append(items, ServerItem{Occurrence: itemOccurrence(x.Feed.Type, it), Fields: itemFields(it)})
	}
	return podcastFields(x.Feed), items
}

// withEvidence completes a record-level item's occurrence from its record
// (its strings already without userinfo), as a parse gives it: the GUID as
// sent, the enclosure, publication time and title from the fields, and the
// plain derivation as k and v (the sent k chooses atom-id or rss-guid;
// reference CHOICE).
func withEvidence(it ServerItem) ServerItem {
	occ := map[string]any{}
	guid, enclosure := "", ""
	k, _ := str(it.Occurrence, "k")
	if g, ok := str(it.Occurrence, "guid"); ok && k != contract.KindEnclosure {
		if t, ok := contract.TrimGUID(g); ok {
			guid = t
			occ["guid"] = t
		}
	}
	if m, ok := object(it.Fields["media"]); ok {
		if u, ok := str(m, "url"); ok && u != "" {
			occ["enclosure_url"] = u
			enclosure = contract.NormalizeURL(u)
		}
	}
	if pub, ok := integer(it.Fields["pub_date"]); ok {
		occ["publication_time"] = pub
	}
	if t, ok := str(it.Fields, "title"); ok {
		occ["title"] = t
	}
	switch {
	case guid != "" && k == contract.KindAtomID:
		occ["k"], occ["v"] = contract.KindAtomID, guid
	case guid != "":
		occ["k"], occ["v"] = contract.KindRSSGUID, guid
	case enclosure != "":
		occ["k"], occ["v"] = contract.KindEnclosure, enclosure
	}
	return ServerItem{Occurrence: occ, Fields: it.Fields}
}

// itemOccurrence is a parsed item's occurrence: its trimmed GUID, enclosure,
// publication time and title, with the plain derivation (PROTOCOL §3.3 rules 2–4)
// as k and v; without GUID and enclosure it has no k. The GUID, enclosure and
// title lose their URL userinfo first (identity values too).
func itemOccurrence(feedType string, it *feed.Item) map[string]any {
	occ := map[string]any{}
	guid, enclosure := "", ""
	if it.GUID != nil {
		if g, ok := contract.TrimGUID(StripUserinfo(*it.GUID)); ok {
			guid = g
			occ["guid"] = g
		}
	}
	if it.Media != nil && it.Media.URL != nil && *it.Media.URL != "" {
		enc := StripUserinfo(*it.Media.URL)
		occ["enclosure_url"] = enc
		enclosure = contract.NormalizeURL(enc)
	}
	if it.PubDate != nil {
		occ["publication_time"] = *it.PubDate
	}
	if it.Title != nil {
		occ["title"] = StripUserinfo(*it.Title)
	}
	switch {
	case guid != "" && feedType == "atom":
		occ["k"], occ["v"] = contract.KindAtomID, guid
	case guid != "":
		occ["k"], occ["v"] = contract.KindRSSGUID, guid
	case enclosure != "":
		occ["k"], occ["v"] = contract.KindEnclosure, enclosure
	}
	return occ
}

// serverRef derives a fetched item's episode with the account's evidence
// (PROTOCOL §9.7, §3.3 rules 1–4); the occurrence stays the item's own (its plain
// derivation as k and v). An item with neither GUID nor enclosure has a
// random ID on every device and fetch, so the server leaves it to devices
// (ok is false).
func serverRef(acct *storage.AccountTx, namespace, podcast string, it ServerItem) (EpisodeRef, bool, error) {
	v := newViewOf(acct, namespace)
	id := v.derive(podcast, occurrenceOf(it.Occurrence), guidKind(it.Occurrence), true)
	if v.err != nil || id == "" {
		return EpisodeRef{}, false, v.err
	}
	return EpisodeRef{EpisodeID: id, PodcastID: podcast, Occurrence: maps.Clone(it.Occurrence)}, true, nil
}

// latestGeneration is a podcast's most recently opened generation ("" for
// none): a fetch without a live generation names it (its parts' inputs and a
// move's subject).
func latestGeneration(ctx *protocol.ApplyContext, podcast string) (string, error) {
	rows, err := ctx.Account.Contributions(protocol.PodcastScope(podcast))
	if err != nil {
		return "", err
	}
	var cs []projection.Contribution
	for _, r := range rows {
		e, err := decodeEffect(r.Effect)
		if err != nil {
			return "", err
		}
		if !projection.IsCatalogKind(e.Kind) {
			cs = append(cs, projection.Contribution{Node: r.NodeID, Rank: r.Rank, Effect: e})
		}
	}
	if st := projection.FoldPodcast(cs); st.Latest != nil {
		return st.Latest.ID, nil
	}
	return "", nil
}

// ---- refresh_status (PROTOCOL §9.4 point 7, PROTOCOL §9.4)

// FetchOutcome is what a fetch's refresh status is made of.
type FetchOutcome struct {
	FetchTime   int64          // when the fetch succeeded or failed
	Error       map[string]any // {label, code, detail}; nil for a success (a 304 included)
	NextAttempt *int64         // the next scheduled attempt, if any
	LastSuccess *int64         // the last success before this fetch, which a failure keeps
}

// statusInterval: an unchanged status is published at most once a day.
const statusInterval = 24 * 3600 * 1000

// RefreshStatus is a fetch's refresh_status value and whether publishing it
// is due: when nothing was published yet, when the outcome class (success or
// failure) or the error object differs from the published record's, or once
// a day after the published attempt. current is the podcast's refresh_status
// record (nil for none; RefreshStatusRecord).
func RefreshStatus(current map[string]any, o FetchOutcome) (map[string]any, bool) {
	v := map[string]any{"last_attempt": o.FetchTime, "last_success": nil, "error": nil, "next_attempt": nil}
	if o.Error == nil {
		v["last_success"] = o.FetchTime
	} else {
		v["error"] = o.Error
		if o.LastSuccess != nil {
			v["last_success"] = *o.LastSuccess
		}
	}
	if o.NextAttempt != nil {
		v["next_attempt"] = *o.NextAttempt
	}
	if current == nil {
		return v, true
	}
	cur, _ := current["error"].(map[string]any)
	if (cur == nil) != (o.Error == nil) || (cur != nil && !sameCanonical(cur, o.Error)) {
		return v, true
	}
	last, ok := integer(current["last_attempt"])
	return v, !ok || o.FetchTime-last >= statusInterval
}

// RefreshStatusRecord is a podcast's published refresh_status record (nil
// for none).
func RefreshStatusRecord(acct *storage.AccountTx, podcast string) (map[string]any, error) {
	return recordValue(acct, "podcast", podcast, "refresh_status")
}

func sameCanonical(a, b any) bool {
	x, err1 := contract.Canonical(normalizeJSON(a))
	y, err2 := contract.Canonical(normalizeJSON(b))
	return err1 == nil && err2 == nil && string(x) == string(y)
}
