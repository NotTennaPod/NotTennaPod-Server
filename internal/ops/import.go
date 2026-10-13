package ops

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/queue"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The server-side library import (PROTOCOL §11.5): an Android AntennaPod database backup
// or OPML file made into server decisions. Each imported podcast is one server commit:
// an import.podcast decision that creates the podcast and its generation, the podcast's
// first server fetch made of the backup's items (catalog records, identity evidence and
// the population marker), and one import.episode decision per resulting episode holding
// the imported state and statistics baselines (PROTOCOL §11.5).

// ImportPodcast is one imported podcast.
type ImportPodcast struct {
	ImportID    string
	SourceFeeds []int64 // the backup's Feeds rows that became this podcast (the first creates it, PROTOCOL §11.5)
	URL         string  // the feed URL (userinfo removed)
	Title       string  // the creating feed's title (custom title first), for the result
	State       string  // subscribed, not_subscribed (preview) or archived
	KeepUpdated bool
	NewEpisodes string         // global, inbox, nothing or queue (PROTOCOL §11.5)
	SecretID    string         // the sealed feed credentials ("" for none; the caller stores the secret)
	Feed        map[string]any // the podcast metadata record fields (PROTOCOL §9.4); without items, the record alone (nil: none)
	Rows        []ImportRow    // the backup's items, in source order
	Time        int64          // the import's server time
}

// ImportRow is one source item with its lowest-id media row (Android's join).
type ImportRow struct {
	SourceItem  int64
	SourceMedia int64 // 0 without a media row
	Item        ServerItem
	PlayState   string // played, unplayed or new (Android read 1, 0, -1)
	Position    int64
	Favorite    bool
	LastPlayed  int64  // FeedMedia.last_played_time (0: never)
	History     *int64 // FeedMedia.playback_completion_date when > 0
	PlayedTime  int64  // FeedMedia.played_duration
	Duration    int64  // FeedMedia.duration (device-measured)
	QueueIndex  int    // its place in the Queue table, or -1
	QueueKey    string // the key and position ID allocated for that place
	PositionID  string
}

// ImportedPodcast reports what one imported podcast became.
type ImportedPodcast struct {
	PodcastID, GenerationID string
	Episodes                int
	Merged                  [][]int64        // source items that became one episode (PROTOCOL §11.5)
	EpisodeOf               map[int64]string // source item → episode ID
	Ambiguous               []int64          // source items with neither GUID nor enclosure (persisted e0: IDs)
}

// ImportItem is a backup item as a fetch item: its occurrence and its
// metadata record fields, exactly as a parse of the feed gives them.
func ImportItem(feedType string, it *feed.Item) ServerItem {
	return ServerItem{Occurrence: itemOccurrence(feedType, it), Fields: itemFields(it)}
}

// ImportFeedFields is a backup feed's podcast metadata record fields.
func ImportFeedFields(f *feed.Feed) map[string]any { return podcastFields(f) }

// ImportSecretID is the stable ID of an imported feed's credentials secret.
func ImportSecretID(importID string, sourceFeed int64) string {
	return nameUUID(importID, "secret:"+strconv.FormatInt(sourceFeed, 10))
}

// ImportPositionID is the stable position ID of the import's queue entry at
// index i.
func ImportPositionID(importID string, i int) string {
	return nameUUID(importID, "position:"+strconv.Itoa(i))
}

// ImportOccurrenceID is the persisted occurrence ID of a source item with
// neither GUID nor enclosure (PROTOCOL §3.3 rule 5; §11.5).
func ImportOccurrenceID(importID string, sourceItem int64) string {
	return "e0:" + nameUUID(importID, "item:"+strconv.FormatInt(sourceItem, 10))
}

// ImportCreationID is the stable creation ID of an imported podcast: podcast
// and generation IDs derive from it as from a creating fact's op ID.
func ImportCreationID(importID string, sourceFeed int64) string {
	return nameUUID(importID, "podcast:"+strconv.FormatInt(sourceFeed, 10))
}

// ApplyImportPodcast applies one imported podcast in the caller's server
// commit (protocol.Service.ServerCommit).
func ApplyImportPodcast(ctx *protocol.ApplyContext, x ImportPodcast) (ImportedPodcast, error) {
	out := ImportedPodcast{EpisodeOf: map[int64]string{}}
	acct := ctx.Account
	if len(x.SourceFeeds) == 0 {
		return out, fmt.Errorf("ops: an imported podcast needs a source feed")
	}
	url := StripUserinfo(x.URL)
	normalized := contract.NormalizeURL(url)
	if owner, err := acct.PodcastByAlias(normalized); err != nil {
		return out, err
	} else if owner != "" {
		return out, fmt.Errorf("ops: import into an account that already has %s", normalized)
	}
	creation := ImportCreationID(x.ImportID, x.SourceFeeds[0])
	podcast := contract.PodcastID(ctx.Namespace, creation)
	generation := contract.GenerationID(ctx.Namespace, podcast, creation)
	out.PodcastID, out.GenerationID = podcast, generation
	sources := make([]any, len(x.SourceFeeds))
	for i, f := range x.SourceFeeds {
		sources[i] = f
	}
	var secret any
	if x.SecretID != "" {
		secret = x.SecretID
	}
	d, err := ctx.Decide(protocol.DecisionInput{Type: "import.podcast", Version: 1,
		Subject: "import:" + x.ImportID + ":podcast:" + normalized,
		Inputs:  map[string]any{"import_id": x.ImportID, "source_feeds": sources, "creation_id": creation},
		Result: map[string]any{"podcast_id": podcast, "generation_id": generation, "url": url, "title": x.Title, "state": x.State,
			"keep_updated": x.KeepUpdated, "new_episodes_action": x.NewEpisodes, "secret_id": secret}})
	if err != nil {
		return out, err
	}
	node := d["decision_id"].(string)
	if err := acct.CreatePodcast(podcast, node); err != nil {
		return out, err
	}
	if err := acct.AddAlias(normalized, podcast, url, "import", node); err != nil {
		return out, err
	}
	if _, err := ctx.Decide(protocol.DecisionInput{Type: "podcast.alias", Version: 1, Subject: "alias:" + normalized,
		Dependencies: []string{node}, Result: map[string]any{"podcast_id": podcast}}); err != nil {
		return out, err
	}
	if err := importGeneration(ctx, node, podcast, generation, url, x.State, x.KeepUpdated, x.NewEpisodes, x.SecretID); err != nil {
		return out, err
	}
	refreshID := nameUUID(x.ImportID, "refresh:"+creation)
	if len(x.Rows) == 0 {
		// Unpopulated: its first fetch is a first population (PROTOCOL §11.5). The
		// file's podcast record still names it until that fetch replaces it
		// (PROTOCOL §11.5 "feed metadata"), as an imported podcast with items is named.
		if x.Feed == nil {
			return out, nil
		}
		return out, ApplyServerFetch(ctx, ServerFetch{PodcastID: podcast, GenerationID: generation, RefreshBy: node, RefreshID: refreshID,
			FetchTime: x.Time, FeedFields: x.Feed, MetadataOnly: true, Namespace: ctx.Namespace})
	}
	// The backup's items as the podcast's first server fetch: catalog records,
	// identity evidence and the population marker (PROTOCOL §11.5).
	// In publication-date order (undated last, ties by item), so release
	// groups are created in date order and every row is then placed by the
	// resolver (PROTOCOL §9.7; a reference CHOICE).
	ordered := append([]ImportRow(nil), x.Rows...)
	sort.SliceStable(ordered, func(i, j int) bool {
		pi, iok := integer(ordered[i].Item.Occurrence["publication_time"])
		pj, jok := integer(ordered[j].Item.Occurrence["publication_time"])
		switch {
		case iok != jok:
			return iok
		case iok && pi != pj:
			return pi < pj
		}
		return ordered[i].SourceItem < ordered[j].SourceItem
	})
	items := make([]ServerItem, len(ordered))
	for i, r := range ordered {
		items[i] = r.Item
	}
	feedFields := x.Feed
	if feedFields == nil {
		return out, fmt.Errorf("ops: imported items need the podcast's record")
	}
	if err := ApplyServerFetch(ctx, ServerFetch{PodcastID: podcast, GenerationID: generation, RefreshBy: node,
		RefreshID: refreshID, FetchTime: x.Time, FeedFields: feedFields, Items: items,
		Namespace: ctx.Namespace}); err != nil {
		return out, err
	}
	// Each row's episode under the evidence the fetch recorded; rows that
	// derive the same episode are one episode (PROTOCOL §11.5).
	type group struct {
		ref  EpisodeRef
		rows []ImportRow
	}
	var groups []*group
	byEpisode := map[string]*group{}
	for _, r := range x.Rows {
		ref, ok, err := serverRef(acct, ctx.Namespace, podcast, r.Item)
		if err != nil {
			return out, err
		}
		if !ok {
			// Neither GUID nor enclosure: a persisted occurrence ID, stable for
			// this import (PROTOCOL §3.3 rule 5; §11.5).
			occ := map[string]any{"k": "none", "v": ""}
			for _, k := range []string{"title", "publication_time"} {
				if v, present := r.Item.Occurrence[k]; present {
					occ[k] = v
				}
			}
			ref = EpisodeRef{EpisodeID: ImportOccurrenceID(x.ImportID, r.SourceItem), PodcastID: podcast, Occurrence: occ}
			out.Ambiguous = append(out.Ambiguous, r.SourceItem)
		}
		g := byEpisode[ref.EpisodeID]
		if g == nil {
			g = &group{ref: ref}
			byEpisode[ref.EpisodeID] = g
			groups = append(groups, g)
		}
		g.rows = append(g.rows, r)
		out.EpisodeOf[r.SourceItem] = ref.EpisodeID
	}
	for _, g := range groups {
		if len(g.rows) > 1 {
			ids := make([]int64, len(g.rows))
			for i, r := range g.rows {
				ids[i] = r.SourceItem
			}
			out.Merged = append(out.Merged, ids)
		}
		if err := importEpisode(ctx, x, node, generation, g.ref, g.rows); err != nil {
			return out, err
		}
	}
	out.Episodes = len(groups)
	return out, nil
}

// importGeneration opens an imported podcast's generation with the
// backup's state, policy and credentials, as the import.podcast decision's
// effect.
func importGeneration(ctx *protocol.ApplyContext, node, podcast, generation, url, state string, keep bool, action, secret string) error {
	if err := ctx.Account.CreateGeneration(generation, podcast, node, ""); err != nil {
		return err
	}
	e := projection.Effect{Kind: projection.KindResolve, Generation: generation, Mode: "created", RefreshURL: url,
		Preview: state == "not_subscribed", Archived: state == "archived", KeepUpdated: &keep, NewEpisodes: &action}
	if secret != "" {
		s := secret
		e.SecretID = &s
	}
	return ctx.ContributeAs(node, protocol.PodcastScope(podcast), e)
}

// importEpisode records one episode's imported state (importedEffect). The
// decision (version 1): inputs {import_id, source_items}; result
// {episode_id, generation_id, occurrence (the first row's), title, imported,
// baseline_id, queue_key and position_id (when queued), duration?, catalog?,
// rows?}. rows lists each source row's own occurrence and state when the
// import grouped several (importedRows): its effect follows each row's own
// occurrence.
func importEpisode(ctx *protocol.ApplyContext, x ImportPodcast, podcastNode, generation string, ref EpisodeRef, rows []ImportRow) error {
	acct := ctx.Account
	irows := make([]projection.ImportedRow, len(rows))
	byItem := map[int64]ImportRow{}
	sourceItems := make([]any, 0, len(rows))
	for i, r := range rows {
		irows[i] = importedRow(x.ImportID, generation, ref, r.SourceItem, r.SourceMedia, r.Item.Occurrence, r)
		byItem[r.SourceItem] = r
		sourceItems = append(sourceItems, r.SourceItem)
	}
	e := importedEffect(irows, len(rows) > 1)
	state := byItem[stateRow(irows).SourceItem]
	title, _ := str(state.Item.Fields, "title")
	result := map[string]any{"episode_id": ref.EpisodeID, "generation_id": generation, "occurrence": ref.Occurrence, "title": title,
		"imported": importedValue(e.Imported), "baseline_id": x.ImportID + "/" + strconv.FormatInt(state.SourceMedia, 10)}
	inputs := map[string]any{"import_id": x.ImportID, "source_items": sourceItems}
	if e.QueueKey != "" {
		// Its place in the backup's global Queue order (the earliest queued
		// row's): the import's queue is one replay step at the manifest, in
		// that order (PROTOCOL §7.6); the values later sorts use come
		// from the import (PROTOCOL §11.5).
		queued := -1
		for _, r := range rows {
			if r.QueueIndex >= 0 && (queued < 0 || r.QueueIndex < queued) {
				queued = r.QueueIndex
			}
		}
		result["queue_key"], result["position_id"], result["queue_index"] = e.QueueKey, e.PositionID, int64(queued)
		v := queue.Item{Title: title, PodcastID: ref.PodcastID, PodcastTitle: x.Title}
		if pub, ok := integer(state.Item.Fields["pub_date"]); ok {
			v.PublicationTime = pub
		}
		if m, ok := object(state.Item.Fields["media"]); ok {
			if d, ok := integer(m["duration"]); ok {
				v.Duration = d
			}
		}
		if e.Duration != nil {
			v.Duration = *e.Duration
		}
		result["values"] = []any{valuesObject(ref.EpisodeID, generation, v)}
	}
	if e.Duration != nil {
		result["duration"] = *e.Duration
	}
	if len(rows) > 1 {
		list := make([]any, len(rows))
		for i, r := range rows {
			list[i] = importedRowValue(irows[i], r.Item.Occurrence)
		}
		result["rows"] = list
	}
	var catalog map[string]any
	if _, known, err := acct.CatalogItem(ref.EpisodeID); err != nil {
		return err
	} else if !known {
		// An item the fetch could not identify (no GUID, no enclosure): its
		// catalog record comes with its import decision, which records it
		// (an admit step applies it from the archive).
		catalog = map[string]any{"fields": state.Item.Fields, "fetch_time": x.Time}
		result["catalog"] = catalog
	}
	d, err := ctx.Decide(protocol.DecisionInput{Type: "import.episode", Version: 1,
		Subject: "import:" + x.ImportID + ":episode:" + ref.EpisodeID, Dependencies: []string{podcastNode},
		Inputs: inputs, Result: result})
	if err != nil {
		return err
	}
	return applyImportEpisode(ctx, d, generation, ref, e, catalog)
}

// importedRow is a source row as an import effect carries it: its own state,
// and the episode the import derived with the row's own occurrence.
func importedRow(importID, generation string, ref EpisodeRef, item, media int64, occurrence map[string]any, r ImportRow) projection.ImportedRow {
	rr := ref
	rr.GenerationID, rr.Occurrence = generation, occurrence
	row := projection.ImportedRow{SourceItem: item, SourceMedia: media, Ref: refOf(rr), PlayState: r.PlayState, Position: r.Position,
		Favorite: r.Favorite, LastPlayed: r.LastPlayed, History: r.History, PlayedTime: r.PlayedTime, Duration: r.Duration, QueueIndex: -1}
	if media != 0 {
		row.Unit = importID + "/" + strconv.FormatInt(media, 10)
	}
	if r.QueueIndex >= 0 {
		row.QueueIndex, row.QueueKey, row.PositionID = r.QueueIndex, r.QueueKey, r.PositionID
	}
	return row
}

// stateRow is the row whose state an episode takes: the most recently played
// (ties: the later source item).
func stateRow(rows []projection.ImportedRow) projection.ImportedRow {
	state := rows[0]
	for _, r := range rows[1:] {
		if r.LastPlayed > state.LastPlayed || (r.LastPlayed == state.LastPlayed && r.SourceItem > state.SourceItem) {
			state = r
		}
	}
	return state
}

// importedEffect is the imported state of the source rows that are one
// episode (PROTOCOL §11.5): the state of the most recently played row, the
// union of favorites, the queue place of the earliest queued row, and every
// other row with media as its own statistical unit, as Android counts each
// row. withRows keeps the rows on the effect (an import that grouped
// several), so placement can split them by their occurrences and aggregate
// again.
func importedEffect(rows []projection.ImportedRow, withRows bool) projection.Effect {
	state := stateRow(rows)
	im := &projection.Imported{PlayState: state.PlayState, Position: state.Position, LastPlayed: state.LastPlayed,
		History: state.History, PlayedTime: state.PlayedTime}
	e := projection.Effect{Kind: projection.KindImport, Imported: im}
	queued := -1
	for _, r := range rows {
		if r.Favorite {
			im.Favorite = true
		}
		if r.QueueIndex >= 0 && (queued < 0 || r.QueueIndex < queued) {
			queued, e.QueueKey, e.PositionID = r.QueueIndex, r.QueueKey, r.PositionID
		}
		if r.SourceItem == state.SourceItem || r.SourceMedia == 0 {
			continue
		}
		im.Units = append(im.Units, projection.StatUnit{ID: r.Unit, Media: r.SourceMedia, PlayedTime: r.PlayedTime, LastPlayed: r.LastPlayed,
			History: r.History, Duration: r.Duration, Played: r.PlayState == "played", Position: r.Position})
	}
	sort.Slice(im.Units, func(i, j int) bool { return im.Units[i].Media < im.Units[j].Media })
	if state.Duration > 0 {
		d := state.Duration
		e.Duration = &d
	}
	if withRows {
		im.Rows = append([]projection.ImportedRow(nil), rows...)
		sort.Slice(im.Rows, func(i, j int) bool { return im.Rows[i].SourceItem < im.Rows[j].SourceItem })
	}
	return e
}

// importedRowValue is a row as an import.episode result lists it.
func importedRowValue(r projection.ImportedRow, occurrence map[string]any) map[string]any {
	var history any
	if r.History != nil {
		history = *r.History
	}
	v := map[string]any{"source_item": r.SourceItem, "source_media": r.SourceMedia, "occurrence": occurrence, "play_state": r.PlayState,
		"position": r.Position, "favorite": r.Favorite, "last_played": r.LastPlayed, "history": history, "played_time": r.PlayedTime,
		"duration": r.Duration, "queue_index": int64(r.QueueIndex)}
	if r.QueueIndex >= 0 {
		v["queue_key"], v["position_id"] = r.QueueKey, r.PositionID
	}
	return v
}

// importedRowsFrom reads an import.episode result's rows back
// (importedRowValue's inverse); nil without rows.
func importedRowsFrom(importID, generation string, ref EpisodeRef, list []any) []projection.ImportedRow {
	if len(list) == 0 {
		return nil
	}
	out := make([]projection.ImportedRow, 0, len(list))
	for _, x := range list {
		m, _ := object(x)
		var r ImportRow
		item, _ := integer(m["source_item"])
		media, _ := integer(m["source_media"])
		occ, _ := object(m["occurrence"])
		r.PlayState, _ = str(m, "play_state")
		r.Position, _ = integer(m["position"])
		r.Favorite, _ = boolean(m["favorite"])
		r.LastPlayed, _ = integer(m["last_played"])
		if h, ok := integer(m["history"]); ok {
			r.History = &h
		}
		r.PlayedTime, _ = integer(m["played_time"])
		r.Duration, _ = integer(m["duration"])
		q, _ := integer(m["queue_index"])
		r.QueueIndex = int(q)
		r.QueueKey, _ = str(m, "queue_key")
		r.PositionID, _ = str(m, "position_id")
		out = append(out, importedRow(importID, generation, ref, item, media, occ, r))
	}
	return out
}

// applyImportEpisode applies an import.episode decision (made here, or
// admitted from the archive): the catalog record of an item the fetch could
// not identify, the imported state as the decision's effect, its queue
// membership and its statistics. The effect of one row lives where the
// episode reference routes; the rows of a group each go where their own
// occurrence routes, aggregated per scope.
func applyImportEpisode(ctx *protocol.ApplyContext, d map[string]any, generation string, ref EpisodeRef, e projection.Effect, catalog map[string]any) error {
	acct := ctx.Account
	node, _ := str(d, "decision_id")
	if catalog != nil {
		fv, _ := object(catalog["fields"])
		at, _ := integer(catalog["fetch_time"])
		if _, err := noteCatalogItemAs(ctx, node, ref, fv, at, true); err != nil {
			return err
		}
		if err := ctx.ContributeAs(node, protocol.ItemScope(ref.EpisodeID), projection.Effect{Kind: projection.KindObservation,
			Observed: fv, FetchTime: at}); err != nil {
			return err
		}
	}
	if e.QueueKey != "" {
		if err := acct.AddQueueNode(node); err != nil {
			return err
		}
		// Its membership waits for the import's queue step (the manifest,
		// PROTOCOL §7.6) for a place; its key is a key write at once
		// (PROTOCOL §11.5: an imported key exists before the import completes), which
		// the replay notes at this decision's step.
		if err := ctx.QueueStep(node, ""); err != nil {
			return err
		}
	}
	scope := protocol.EpisodeScope(generation, ref.EpisodeID)
	if e.Imported != nil && len(e.Imported.Rows) > 0 {
		v := newView(ctx)
		byScope := map[string][]projection.ImportedRow{}
		var scopes []string
		for _, r := range e.Imported.Rows {
			t := v.target(projection.Effect{Kind: projection.KindImport, Ref: r.Ref}, scope)
			if _, ok := byScope[t]; !ok {
				scopes = append(scopes, t)
			}
			byScope[t] = append(byScope[t], r)
		}
		if v.err != nil {
			return v.err
		}
		sort.Strings(scopes)
		for _, t := range scopes {
			if err := contributeImported(ctx, node, t, importedAt(t, byScope[t])); err != nil {
				return err
			}
		}
		return nil
	}
	// The imported state is routed by its occurrence evidence, as a pushed
	// fact's effect is: a GUID disputed later moves it to its release.
	ref.GenerationID = generation
	return contributeImported(ctx, node, scope, withRef(e, ref))
}

// importedAt is the effect of a group's rows in one scope: their aggregate,
// naming the episode the import derived when it lives elsewhere.
func importedAt(scope string, rows []projection.ImportedRow) projection.Effect {
	e := importedEffect(rows, true)
	if ep, _ := scopeEpisode(scope); rows[0].Ref != nil && ep != rows[0].Ref.Episode {
		e.Member = rows[0].Ref.Episode
	}
	return e
}

// contributeImported contributes an import effect and indexes it where it
// holds history or statistics: a cut covers the decision as a one-ordinal
// session there (PROTOCOL §11.5).
func contributeImported(ctx *protocol.ApplyContext, node, scope string, e projection.Effect) error {
	if err := indexImported(ctx, node, scope, e.Imported); err != nil {
		return err
	}
	return ctx.ContributeAs(node, scope, e)
}

// indexImported sets an import node's listening index row in one scope: one
// when its imported state there holds history or statistics, none otherwise.
func indexImported(ctx *protocol.ApplyContext, node, scope string, im *projection.Imported) error {
	if im == nil || (im.History == nil && im.PlayedTime <= 0 && len(im.Units) == 0) {
		return ctx.Account.RemoveListeningFact(node, scope)
	}
	return ctx.Account.PutListeningFact(storage.ListeningFact{Node: node, Scope: scope, HistoryEntry: im.History != nil, Session: node, Ordinal: 1})
}

// importedFrom reads an import.episode result's imported value back
// (importedValue's inverse; a unit's media row is its ID's suffix).
func importedFrom(v map[string]any) *projection.Imported {
	im := &projection.Imported{}
	im.PlayState, _ = str(v, "play_state")
	im.Position, _ = integer(v["position"])
	im.Favorite, _ = boolean(v["favorite"])
	im.LastPlayed, _ = integer(v["last_played"])
	im.PlayedTime, _ = integer(v["played_time"])
	if h, ok := integer(v["history"]); ok {
		im.History = &h
	}
	units, _ := v["units"].([]any)
	for _, x := range units {
		m, _ := object(x)
		var u projection.StatUnit
		u.ID, _ = str(m, "id")
		if i := strings.LastIndex(u.ID, "/"); i >= 0 {
			u.Media, _ = strconv.ParseInt(u.ID[i+1:], 10, 64)
		}
		u.PlayedTime, _ = integer(m["played_time"])
		u.LastPlayed, _ = integer(m["last_played"])
		if h, ok := integer(m["history"]); ok {
			u.History = &h
		}
		u.Duration, _ = integer(m["duration"])
		u.Played, _ = boolean(m["played"])
		u.Position, _ = integer(m["position"])
		im.Units = append(im.Units, u)
	}
	return im
}

func importedValue(im *projection.Imported) map[string]any {
	v := map[string]any{"play_state": im.PlayState, "position": im.Position, "favorite": im.Favorite, "last_played": im.LastPlayed,
		"played_time": im.PlayedTime}
	if im.History != nil {
		v["history"] = *im.History
	} else {
		v["history"] = nil
	}
	units := make([]any, len(im.Units))
	for i, u := range im.Units {
		var h any
		if u.History != nil {
			h = *u.History
		}
		units[i] = map[string]any{"id": u.ID, "played_time": u.PlayedTime, "last_played": u.LastPlayed, "history": h, "duration": u.Duration,
			"played": u.Played, "position": u.Position}
	}
	v["units"] = units
	return v
}

// ApplyImportManifest records the import's manifest (PROTOCOL §11.5): what was
// imported, skipped and ambiguous, with the file's fingerprint, so a
// recovered manifest can answer a repeated upload of the same file.
func ApplyImportManifest(ctx *protocol.ApplyContext, importID string, manifest map[string]any) (string, error) {
	// It depends on the import's queued episodes: its place, where the
	// import's queue is one replay step, follows them all, and recovery joins
	// it only once every one is present (PROTOCOL §7.6).
	rows, err := ctx.Account.DecisionsOf("import.episode")
	if err != nil {
		return "", err
	}
	var deps []string
	for _, row := range rows {
		d, err := decodeObject(row.Decision)
		if err != nil {
			return "", err
		}
		_, inputs, result := decisionParts(d)
		if id, _ := str(inputs, "import_id"); id != importID {
			continue
		}
		if _, queued := result["queue_index"]; queued {
			deps = append(deps, row.DecisionID)
		}
	}
	d, err := ctx.Decide(protocol.DecisionInput{Type: "import.manifest", Version: 1, Subject: "import", Dependencies: deps,
		Inputs: map[string]any{"import_id": importID}, Result: manifest})
	if err != nil {
		return "", err
	}
	id := d["decision_id"].(string)
	// The import's whole queue is one replay step at its completing decision
	// (PROTOCOL §7.6).
	return id, ctx.QueueStep(id, "")
}

// nameUUID is a name-based (version 5, SHA-1) UUID in the namespace of a UUID.
func nameUUID(namespace, name string) string {
	ns, err := hex.DecodeString(stripDashes(namespace))
	if err != nil || len(ns) != 16 {
		ns = make([]byte, 16)
	}
	h := sha1.New()
	h.Write(ns)
	h.Write([]byte(name))
	b := h.Sum(nil)[:16]
	b[6] = b[6]&0x0f | 0x50
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func stripDashes(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '-' {
			out = append(out, s[i])
		}
	}
	return string(out)
}
