package api

// Real commits through HTTP on a medium library: the parity backup's
// 16 feeds copied to 128 podcasts and their items to about 6,000 episodes,
// imported for real (a staged publication) after the empty account's views
// were read. A phone and the server change it as they do: every fact type
// (the phone's catalog observations, Inbox proposals, preview registrations
// and its fork as another author included), refreshes (new and dropped
// items, a GUID change that merges two episodes, a mirror URL whose
// redirect merges two podcasts), unsubscribing and subscribing again,
// rebuilds in place and staged, a restore from a backup with the phone's
// recovery, and another account's deletion. After every step pages of
// earlier views are read again (the same rows while the account's lineage
// and generation are
// unchanged, 410 after a change), then 24 list queries of the query matrix and
// the screens are compared with the oracle byte for byte; the whole matrix
// at every 50th step and the last. The first steps go through every kind
// until each changes the library; a kind that never does fails the run,
// and so does a run in which any kind never did. The commit observer checks
// every commit's cached version against a full build (TestMain).
//
// NTP_READMODEL_STEPS sets the steps, NTP_READMODEL_LONG=1 ten times them;
// NTP_READMODEL_SCALE=1 runs one seed of 100 steps on the power user's
// library (128 podcasts, about 50,000 episodes), comparing each list at two
// windows (the first page and one at a random offset), the whole matrix at
// its last step (the views' incremental test on a power user's library
// walks the whole matrix at that scale in process).

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html"
	mrand "math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// enlargedBackup writes a copy of the parity backup with its feeds copied
// feedCopies times and each feed's items itemCopies times (new IDs, URLs and
// GUIDs, publication dates a day apart per copy), its queue four times as
// long.
func enlargedBackup(t *testing.T, feedCopies, itemCopies int) string {
	t.Helper()
	data, err := os.ReadFile("../importer/androiddb/testdata/android-3.12.1/emulator-parity-library.db")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "AntennaPodBackup.db")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	exec := func(q string, args ...any) {
		if _, err := tx.Exec(q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	tagged := func(col, tag string) string {
		return col + " || CASE WHEN instr(" + col + ", '?') > 0 THEN '&" + tag + "' ELSE '?" + tag + "' END"
	}
	for c := range feedCopies {
		if c > 0 {
			exec("CREATE TEMP TABLE f AS SELECT * FROM main.Feeds WHERE id < 1000")
			exec("UPDATE temp.f SET id = id + ?, download_url = "+tagged("download_url", fmt.Sprintf("copy=%d", c)), c*1000)
			exec("INSERT INTO main.Feeds SELECT * FROM temp.f")
			exec("DROP TABLE temp.f")
		}
		for m := range itemCopies {
			k := c*itemCopies + m
			if k == 0 {
				continue
			}
			exec("CREATE TEMP TABLE i AS SELECT * FROM main.FeedItems WHERE id < 1000")
			exec("UPDATE temp.i SET id = id + ?, feed = feed + ?, item_identifier = item_identifier || ?, pubDate = pubDate - ?",
				k*1000, c*1000, fmt.Sprintf("-%d", k), int64(m)*86_400_000)
			exec("INSERT INTO main.FeedItems SELECT * FROM temp.i")
			exec("DROP TABLE temp.i")
			exec("CREATE TEMP TABLE m AS SELECT * FROM main.FeedMedia WHERE id < 1000")
			exec("UPDATE temp.m SET id = id + ?, feeditem = feeditem + ?, download_url = "+tagged("download_url", fmt.Sprintf("item=%d", k)), k*1000, k*1000)
			exec("INSERT INTO main.FeedMedia SELECT * FROM temp.m")
			exec("DROP TABLE temp.m")
			if k < 4 {
				exec("INSERT INTO main.Queue (id, feeditem, feed) SELECT id + ?, feeditem + ?, feed + ? FROM main.Queue WHERE id < 1000", k*1000, k*1000, c*1000)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return path
}

// ---- the library as the oracle has it, for targets

func (s *commitRun) epRef(e *reference.Episode) map[string]any { return s.epRefFrom(s.ref, e) }

// epRefFrom is an episode's reference, as a phone names it.
func (s *commitRun) epRefFrom(_ *reference.Library, e *reference.Episode) map[string]any {
	out := map[string]any{"episode_id": e.ID, "podcast_id": e.Podcast.ID, "generation_id": s.generation(e.Podcast)}
	var id struct {
		Occurrence json.RawMessage `json:"occurrence"`
	}
	json.Unmarshal(e.Records["identity"].Value, &id)
	if len(id.Occurrence) > 0 && string(id.Occurrence) != "null" {
		var occ map[string]any
		dec := json.NewDecoder(bytes.NewReader(id.Occurrence))
		dec.UseNumber()
		dec.Decode(&occ)
		out["occurrence"] = occ
	}
	return out
}

func (s *commitRun) generation(p *reference.Podcast) any {
	var sub struct {
		Generation *string `json:"generation_id"`
	}
	json.Unmarshal(p.Records["subscription"].Value, &sub)
	if sub.Generation == nil {
		return nil
	}
	return *sub.Generation
}

func (s *commitRun) podTarget(p *reference.Podcast) []any {
	return []any{map[string]any{"entity": "podcast", "id": p.ID, "generation": s.generation(p)}}
}

func (s *commitRun) accountTarget() []any {
	return []any{map[string]any{"entity": "account", "id": s.account}}
}

// refreshURL is a podcast's refresh URL.
func refreshURL(p *reference.Podcast) string {
	var id struct {
		URL string `json:"refresh_url"`
	}
	json.Unmarshal(p.Records["identity"].Value, &id)
	return id.URL
}

// episode picks an episode passing pred (nil: any), or nil.
func (s *commitRun) episode(pred func(e *reference.Episode) bool) *reference.Episode {
	var pool []*reference.Episode
	for _, e := range s.ref.Episodes {
		if pred == nil || pred(e) {
			pool = append(pool, e)
		}
	}
	if len(pool) == 0 {
		return nil
	}
	return pool[s.r.IntN(len(pool))]
}

func (s *commitRun) podcast(pred func(p *reference.Podcast) bool) *reference.Podcast {
	var pool []*reference.Podcast
	for _, id := range slices.Sorted(func(yield func(string) bool) {
		for id := range s.ref.Podcasts {
			if !yield(id) {
				return
			}
		}
	}) {
		if p := s.ref.Podcasts[id]; pred == nil || pred(p) {
			pool = append(pool, p)
		}
	}
	if len(pool) == 0 {
		return nil
	}
	return pool[s.r.IntN(len(pool))]
}

func subscribed(p *reference.Podcast) bool { return p.State == "subscribed" }

// pushOne pushes a fact naming one episode, and reports whether it applied.
func (s *commitRun) pushEpisode(typ string, e *reference.Episode, payload map[string]any) bool {
	if e == nil {
		return false
	}
	re := s.epRef(e)
	payload["episode"] = re
	return s.push(s.fact(typ, target(re), payload)) == "applied"
}

func (s *commitRun) headSeq() int64 {
	var h storage.Head
	if err := s.store.Read(context.Background(), func(r *storage.ReadTx) error {
		var err error
		h, err = r.Account(s.account).Head()
		return err
	}); err != nil {
		s.t.Fatal(err)
	}
	return h.Seq
}

// ---- the server's commits

// fetch applies a server fetch of the podcast as the refresher does.
func (s *commitRun) fetch(podcast string, generation any, f *feed.Feed, aliases ...string) {
	s.t.Helper()
	gen, _ := generation.(string)
	x := ops.ServerFetch{PodcastID: podcast, GenerationID: gen, RefreshID: newID(), FetchTime: s.clock.Now().UnixMilli(), Feed: f, Aliases: aliases,
		Namespace: s.ns, Bases: map[string]string{}}
	svc := s.srv.cfg.Protocol
	ctx := context.Background()
	if err := svc.ServerCommit(ctx, s.account, func(c *protocol.ApplyContext) error {
		rc, err := ops.CaptureRefresh(c, podcast)
		x.RefreshBy, x.Bases = rc.RefreshBy, rc.Bases
		return err
	}); err != nil {
		s.t.Fatal(err)
	}
	if err := svc.ServerCommit(ctx, s.account, func(c *protocol.ApplyContext) error { return ops.ApplyServerFetch(c, x) }); err != nil {
		s.t.Fatal(err)
	}
}

type feedItem struct {
	guid, title, enclosure string
	pub                    int64
}

func (s *commitRun) parseFeed(source string, items []feedItem) *feed.Feed {
	s.t.Helper()
	var b strings.Builder
	b.WriteString(`<rss version="2.0"><channel><title>Show</title>`)
	for _, it := range items {
		fmt.Fprintf(&b, `<item><title>%s</title><guid>%s</guid><pubDate>%s</pubDate><enclosure url="%s" length="20000" type="audio/mpeg"/></item>`,
			html.EscapeString(it.title), html.EscapeString(it.guid), time.UnixMilli(it.pub).UTC().Format(time.RFC1123Z), html.EscapeString(it.enclosure))
	}
	b.WriteString(`</channel></rss>`)
	f, err := feed.Parse(context.Background(), []byte(b.String()), source, s.clock.Now())
	if err != nil {
		s.t.Fatal(err)
	}
	return f
}

// items are a podcast's episodes as its feed lists them (those with a GUID).
func (s *commitRun) items(p *reference.Podcast) []feedItem {
	var out []feedItem
	for _, e := range s.ref.Episodes {
		if e.Podcast.ID != p.ID {
			continue
		}
		g, _ := e.Occurrence["guid"].(string)
		enc, _ := e.Occurrence["enclosure_url"].(string)
		if g == "" || enc == "" {
			continue
		}
		title := ""
		if e.Title != nil {
			title = *e.Title
		}
		out = append(out, feedItem{g, title, enc, e.PubDate})
	}
	return out
}

// ---- the kinds of step

type stepKind struct {
	name   string
	weight int
	run    func(s *commitRun) bool
}

// rebuildAfterRecovery makes the sequences rebuild the archive after their
// restore too, so a rebuild runs over recovered listening sessions: the
// queue replay memo, validated by the head alone, must not be reused during
// an archive rebuild (ops/queue_replay.go), or replaying a recovered
// session.start finds nothing. It also runs TestRebuildAfterRecoveredSessions.
const rebuildAfterRecovery = true

// serverKinds are the kinds of step that are the server's commits, not a
// phone's facts.
var serverKinds = map[string]bool{"refresh": true, "refresh with a GUID change": true, "podcast merge": true, "rebuild": true, "staged rebuild": true}

var stepKinds = []stepKind{
	{"favorite.set", 8, func(s *commitRun) bool {
		return s.pushEpisode("favorite.set", s.episode(nil), map[string]any{"on": s.r.IntN(2) == 0})
	}},
	{"episode.mark_played", 8, func(s *commitRun) bool {
		return s.pushEpisode("episode.mark_played", s.episode(func(e *reference.Episode) bool { return e.Read != 1 }), map[string]any{"reset_position": s.r.IntN(2) == 0})
	}},
	{"episode.mark_unplayed", 4, func(s *commitRun) bool {
		return s.pushEpisode("episode.mark_unplayed", s.episode(func(e *reference.Episode) bool { return e.Read == 1 }), map[string]any{})
	}},
	{"episode.reset_position", 3, func(s *commitRun) bool {
		return s.pushEpisode("episode.reset_position", s.episode(func(e *reference.Episode) bool { return e.Position > 0 }), map[string]any{})
	}},
	{"episode.restore_state", 3, func(s *commitRun) bool {
		return s.pushEpisode("episode.restore_state", s.episode(nil), map[string]any{"play_state": []string{"new", "unplayed", "played"}[s.r.IntN(3)],
			"position": int64(s.r.IntN(600)) * 1000})
	}},
	{"inbox.dismiss", 4, func(s *commitRun) bool {
		e := s.episode(func(e *reference.Episode) bool { return e.Read == -1 })
		if e == nil {
			return false
		}
		re := s.epRef(e)
		return s.push(s.fact("inbox.dismiss", target(re), map[string]any{"episodes": []any{re}})) == "applied"
	}},
	{"inbox.dismiss_all", 1, func(s *commitRun) bool {
		var refs []map[string]any
		for range 3 {
			if e := s.episode(func(e *reference.Episode) bool { return e.Read == -1 }); e != nil && !slices.ContainsFunc(refs, func(r map[string]any) bool { return r["episode_id"] == e.ID }) {
				refs = append(refs, s.epRef(e))
			}
		}
		if len(refs) == 0 {
			return false
		}
		slices.SortFunc(refs, func(a, b map[string]any) int {
			return strings.Compare(a["episode_id"].(string), b["episode_id"].(string))
		})
		list := []any{}
		for _, r := range refs {
			list = append(list, r)
		}
		return s.push(s.fact("inbox.dismiss_all", target(refs...), map[string]any{"episodes": list})) == "applied"
	}},
	{"history.remove", 3, func(s *commitRun) bool {
		e := s.episode(func(e *reference.Episode) bool { return e.History > 0 })
		if e == nil {
			return false
		}
		re := s.epRef(e)
		f := s.fact("history.remove", target(re), map[string]any{"episode": re})
		if s.push(f) != "applied" {
			return false
		}
		s.removals = append(s.removals, map[string]any{"op_id": f["op_id"], "episode": e.ID})
		return true
	}},
	{"history.restore", 2, func(s *commitRun) bool {
		if len(s.removals) == 0 {
			return false
		}
		rm := s.removals[len(s.removals)-1]
		s.removals = s.removals[:len(s.removals)-1]
		e := s.ref.ByID[rm["episode"].(string)]
		return s.pushEpisode("history.restore", e, map[string]any{"removal": rm["op_id"], "time": int64(0)})
	}},
	{"history.clear", 1, func(s *commitRun) bool {
		return s.push(s.fact("history.clear", []any{}, map[string]any{})) == "applied"
	}},
	{"statistics.reset", 1, func(s *commitRun) bool {
		return s.push(s.fact("statistics.reset", []any{}, map[string]any{})) == "applied"
	}},
	{"session.end", 6, func(s *commitRun) bool { return s.listen("end") }},
	{"session.finalize", 3, func(s *commitRun) bool { return s.listen("finalize") }},
	{"episode.leave", 2, func(s *commitRun) bool { return s.listen("leave") }},
	{"queue.add", 8, func(s *commitRun) bool {
		e := s.episode(func(e *reference.Episode) bool { return !e.Queued })
		if e == nil {
			return false
		}
		k, id := newQueueKey(s.t, s.lastQueueKey())
		re := s.epRef(e)
		return s.push(s.fact("queue.add", target(re), map[string]any{"members": []any{map[string]any{"episode": re, "key": k, "position_id": id}},
			"place": map[string]any{"kind": "end"}})) == "applied"
	}},
	{"queue.remove", 5, func(s *commitRun) bool {
		e := s.queued()
		if e == nil {
			return false
		}
		re := s.epRef(e)
		return s.push(s.fact("queue.remove", target(re), map[string]any{"episodes": []any{re}, "reason": "user"})) == "applied"
	}},
	{"queue.move", 4, func(s *commitRun) bool {
		e := s.queued()
		if e == nil {
			return false
		}
		k, id := newQueueKey(s.t, "")
		re := s.epRef(e)
		return s.push(s.fact("queue.move", target(re), map[string]any{"episode": re, "key": k, "position_id": id,
			"place": map[string]any{"kind": []string{"start", "end"}[s.r.IntN(2)]}})) == "applied"
	}},
	{"queue.reorder", 2, func(s *commitRun) bool {
		obs := []any{}
		for _, e := range byID(s.ref.Queue) {
			obs = append(obs, s.epRef(e))
		}
		if len(obs) < 2 {
			return false
		}
		order := []string{"episode_title_a_z", "date_new_old", "duration_short_long"}[s.r.IntN(3)]
		return s.push(s.fact("queue.reorder", s.accountTarget(), map[string]any{"sort": map[string]any{"sort_order": order}, "observed": obs})) == "applied"
	}},
	{"queue.restore", 2, func(s *commitRun) bool {
		e := s.episode(func(e *reference.Episode) bool { return !e.Queued })
		if e == nil {
			return false
		}
		k, id := newQueueKey(s.t, "")
		re := s.epRef(e)
		return s.push(s.fact("queue.restore", target(re), map[string]any{"members": []any{map[string]any{"episode": re, "key": k, "position_id": id}},
			"place": map[string]any{"kind": "start"}})) == "applied"
	}},
	{"queue.clear", 1, func(s *commitRun) bool {
		var refs []map[string]any
		for _, e := range byID(s.ref.Queue) {
			refs = append(refs, s.epRef(e))
		}
		if len(refs) == 0 {
			return false
		}
		obs := []any{}
		for _, r := range refs {
			obs = append(obs, r)
		}
		return s.push(s.fact("queue.clear", target(refs...), map[string]any{"observed": obs})) == "applied"
	}},
	{"account.set_queue_policy", 1, func(s *commitRun) bool {
		return s.push(s.fact("account.set_queue_policy", s.accountTarget(), map[string]any{"sorting": map[string]any{"keep_sorted": s.r.IntN(2) == 0,
			"sort_order": "episode_title_a_z"}})) == "applied"
	}},
	{"podcast.set_policy", 3, func(s *commitRun) bool {
		p := s.podcast(subscribed)
		return p != nil && s.push(s.fact("podcast.set_policy", s.podTarget(p), map[string]any{"keep_updated": s.r.IntN(2) == 0})) == "applied"
	}},
	{"podcast.archive", 3, func(s *commitRun) bool {
		p := s.podcast(subscribed)
		return p != nil && s.push(s.fact("podcast.archive", s.podTarget(p), map[string]any{})) == "applied"
	}},
	{"podcast.restore", 3, func(s *commitRun) bool {
		p := s.podcast(func(p *reference.Podcast) bool { return p.State == "archived" })
		return p != nil && s.push(s.fact("podcast.restore", s.podTarget(p), map[string]any{})) == "applied"
	}},
	{"podcast.unsubscribe", 2, func(s *commitRun) bool {
		p := s.podcast(func(p *reference.Podcast) bool { return p.State != "not_subscribed" })
		return p != nil && s.push(s.fact("podcast.unsubscribe", s.podTarget(p), map[string]any{})) == "applied"
	}},
	{"podcast.subscribe", 2, func(s *commitRun) bool { // subscribing again: a new generation
		p := s.podcast(func(p *reference.Podcast) bool { return p.State == "not_subscribed" && refreshURL(p) != "" })
		return p != nil && s.push(s.fact("podcast.subscribe", []any{}, map[string]any{"feed_url": refreshURL(p)})) == "applied"
	}},
	{"podcast.change_url", 1, func(s *commitRun) bool {
		p := s.podcast(subscribed)
		return p != nil && s.push(s.fact("podcast.change_url", s.podTarget(p), map[string]any{"url": fmt.Sprintf("https://example.invalid/commits/moved/%d", s.r.IntN(1e6))})) == "applied"
	}},
	{"podcast.set_credentials", 1, func(s *commitRun) bool {
		p := s.podcast(subscribed)
		if p == nil {
			return false
		}
		payload := map[string]any{"clear": true}
		if s.r.IntN(2) == 0 {
			id := newID()
			payload = map[string]any{"secret_id": id}
			s.secrets = []any{map[string]any{"secret_id": id, "username": "listener", "password": "feed-password"}}
		}
		return s.push(s.fact("podcast.set_credentials", s.podTarget(p), payload)) == "applied"
	}},
	{"account.set_discovery_policy", 2, func(s *commitRun) bool {
		return s.push(s.fact("account.set_discovery_policy", s.accountTarget(),
			map[string]any{"new_episodes_action": []string{"inbox", "nothing", "queue"}[s.r.IntN(3)]})) == "applied"
	}},
	{"catalog.observe", 2, func(s *commitRun) bool { return s.observeNew() != nil }},
	{"discovery.propose", 2, func(s *commitRun) bool { return s.propose() }},
	{"podcast.register_preview", 1, func(s *commitRun) bool {
		return s.push(s.fact("podcast.register_preview", []any{}, map[string]any{"feed_url": fmt.Sprintf("https://example.invalid/commits/preview/%d", s.r.IntN(1e9))})) == "applied"
	}},
	{"installation.fork", 1, func(s *commitRun) bool { return s.fork() }},
	{"refresh", 4, func(s *commitRun) bool { return s.refresh(false) }},
	{"refresh with a GUID change", 1, func(s *commitRun) bool { return s.refresh(true) }},
	{"podcast merge", 1, func(s *commitRun) bool { return s.mergePodcasts() }},
	{"rebuild", 1, func(s *commitRun) bool { return s.rebuild(false) }},
	{"staged rebuild", 1, func(s *commitRun) bool { return s.rebuild(true) }},
}

// byID is episodes sorted by ID, as lists of references are.
func byID(list []*reference.Episode) []*reference.Episode {
	out := slices.Clone(list)
	slices.SortFunc(out, func(a, b *reference.Episode) int { return strings.Compare(a.ID, b.ID) })
	return out
}

func (s *commitRun) queued() *reference.Episode {
	if len(s.ref.Queue) == 0 {
		return nil
	}
	return s.ref.Queue[s.r.IntN(len(s.ref.Queue))]
}

func (s *commitRun) lastQueueKey() string {
	if len(s.ref.Queue) == 0 {
		return ""
	}
	var qp struct {
		Key string `json:"key"`
	}
	json.Unmarshal(s.ref.Queue[len(s.ref.Queue)-1].Records["queue_position"].Value, &qp)
	return qp.Key
}

// observed is an episode a phone observed in its podcast's feed.
type observed struct {
	podcast *reference.Podcast
	ref     map[string]any
	pub     int64
}

// observeNew has the phone observe a new playable item of a subscribed
// podcast's feed (catalog.observe): the catalog gains the episode.
func (s *commitRun) observeNew() *observed {
	p := s.podcast(subscribed)
	if p == nil {
		return nil
	}
	n := s.r.IntN(1e9)
	guid, enc, pub := fmt.Sprintf("commits-observed-%d", n), fmt.Sprintf("https://example.invalid/commits/observed/%d.mp3", n), s.clock.Now().UnixMilli()-3_600_000
	id, err := contract.EpisodeID(s.ns, p.ID, contract.KindRSSGUID, guid)
	if err != nil {
		s.t.Fatal(err)
	}
	ref := map[string]any{"episode_id": id, "podcast_id": p.ID, "generation_id": s.generation(p),
		"occurrence": map[string]any{"k": contract.KindRSSGUID, "v": guid, "guid": guid, "enclosure_url": enc, "publication_time": pub}}
	fields := map[string]any{"title": fmt.Sprintf("Observed %d", n), "pub_date": pub,
		"media": map[string]any{"url": enc, "size": "1000", "mime_type": "audio/mpeg", "duration": int64(0)}}
	for _, k := range []string{"description", "link", "image_url", "payment_link", "chapters", "chapter_url", "transcript_url", "transcript_type",
		"social_interact_url"} {
		fields[k] = nil
	}
	targets := []any{map[string]any{"entity": "episode", "id": id}, map[string]any{"entity": "podcast", "id": p.ID}}
	if s.push(s.fact("catalog.observe", targets, map[string]any{"podcast_id": p.ID, "batch_id": newID(), "parser_version": "phone/1",
		"fetch_time": s.clock.Now().UnixMilli(), "items": []any{map[string]any{"episode": ref, "fields": fields}}})) != "applied" {
		return nil
	}
	return &observed{p, ref, pub}
}

// propose has the phone propose a newly observed episode for the Inbox
// (discovery.propose): it is admitted, new.
func (s *commitRun) propose() bool {
	o := s.observeNew()
	if o == nil {
		return false
	}
	return s.push(s.fact("discovery.propose", target(o.ref), map[string]any{"episode": o.ref, "podcast_id": o.podcast.ID,
		"generation_id": s.generation(o.podcast), "batch_id": newID(), "pub_date": o.pub, "newest_before_batch": o.pub - 86_400_000,
		"action": "inbox", "policy_node": nil})) == "applied"
}

// fork has the phone continue as a copy of itself (installation.fork): a
// new author whose first fact names the copied author's last.
func (s *commitRun) fork() bool {
	old, head := s.p.author, s.last
	if head == "" {
		return false
	}
	s.p.author, s.p.seq, s.last = newID(), 0, ""
	f := s.fact("installation.fork", []any{}, map[string]any{"author": old, "head": head})
	deps := f["dependencies"].([]any)
	if !slices.Contains(deps, any(head)) {
		deps = append(deps, head)
		slices.SortFunc(deps, func(a, b any) int { return strings.Compare(a.(string), b.(string)) })
		f["dependencies"] = deps
	}
	return s.push(f) == "applied"
}

// listen is a listening session: a start, a checkpoint and its end, a
// finalization of the claim, or a leave.
func (s *commitRun) listen(how string) bool {
	e := s.episode(func(e *reference.Episode) bool { return e.HasMedia })
	if e == nil {
		return false
	}
	re := s.epRef(e)
	sid := newID()
	pos := int64(s.r.IntN(600)) * 1000
	start := s.fact("session.start", target(re), map[string]any{"episode": re, "session_id": sid, "start_position": pos, "activity": true})
	if s.push(start) != "applied" {
		return false
	}
	s.clock.Advance(15 * time.Second)
	s.push(s.fact("session.checkpoint", target(re), map[string]any{"episode": re, "session_id": sid, "ordinal": 1, "position": pos + 15_000,
		"contribution": 15_000, "activity": true}))
	completed := s.r.IntN(4) == 0
	switch how {
	case "end":
		return s.push(s.fact("session.end", target(re), map[string]any{"episode": re, "session_id": sid, "ordinal": 2, "position": pos + 20_000,
			"contribution": 5_000, "activity": true, "reason": []string{"ended", "superseded"}[s.r.IntN(2)], "completed": completed,
			"threshold_ms": int64(30_000), "remove_from_queue": false, "history": true})) == "applied"
	case "finalize":
		return s.push(s.fact("session.finalize", target(re), map[string]any{"episode": re, "session_id": sid, "ordinal": 1, // the checkpoint's
			"claim_id": start["op_id"], "completed": completed, "threshold_ms": int64(30_000), "remove_from_queue": false, "history": true})) == "applied"
	default:
		return s.push(s.fact("episode.leave", target(re), map[string]any{"episode": re, "transition_id": newID(), "reason": "switched",
			"position": pos + 16_000, "completed": completed, "threshold_ms": int64(30_000), "remove_from_queue": false, "history": true,
			"observed_claim": start["op_id"]})) == "applied"
	}
}

// refresh fetches a subscribed podcast's feed with two new items and one
// dropped, and, with a GUID change, one item under a new GUID (an episode
// merge).
func (s *commitRun) refresh(guidChange bool) bool {
	p := s.podcast(func(p *reference.Podcast) bool { return subscribed(p) && len(s.items(p)) > 1 })
	if p == nil {
		return false
	}
	items := s.items(p)
	items = slices.Delete(items, 0, 1)
	n := s.r.IntN(1e9)
	for j := range 2 {
		items = append(items, feedItem{fmt.Sprintf("commits-%d-%d", n, j), fmt.Sprintf("New %d", j), fmt.Sprintf("https://example.invalid/commits/%d/%d.mp3", n, j),
			s.clock.Now().UnixMilli() - int64(j)*3_600_000})
	}
	if guidChange {
		items[0].guid = fmt.Sprintf("commits-moved-%d", n)
	}
	before := s.headSeq()
	s.fetch(p.ID, s.generation(p), s.parseFeed(refreshURL(p), items))
	return s.headSeq() > before
}

// mergePodcasts subscribes a mirror URL of a podcast's feed, fetches it,
// then fetches it once more redirected to the podcast's URL: the mirror is
// merged into the podcast.
func (s *commitRun) mergePodcasts() bool {
	p := s.podcast(func(p *reference.Podcast) bool { return subscribed(p) && refreshURL(p) != "" && len(s.items(p)) > 0 })
	if p == nil {
		return false
	}
	s.mirrors++
	mirror := fmt.Sprintf("https://mirror.example.invalid/commits/%d", s.mirrors)
	sub := s.fact("podcast.subscribe", []any{}, map[string]any{"feed_url": mirror})
	if s.push(sub) != "applied" {
		return false
	}
	p2 := contract.PodcastID(s.ns, sub["op_id"].(string))
	g2 := contract.GenerationID(s.ns, p2, sub["op_id"].(string))
	items := s.items(p)
	s.fetch(p2, g2, s.parseFeed(mirror, items))
	s.clock.Advance(time.Minute)
	s.fetch(p2, g2, s.parseFeed(mirror, items), refreshURL(p))
	var merged bool
	s.store.Read(context.Background(), func(r *storage.ReadTx) error {
		rec, err := r.Account(s.account).Record("podcast", p2, "subscription")
		merged = err == nil && strings.Contains(rec.Value, `"merged"`)
		return nil
	})
	return merged
}

// rebuild derives the account again from its archive, in place or staged
// (in a copy, then published); either raises the generation.
func (s *commitRun) rebuild(staged bool) bool {
	svc := s.srv.cfg.Protocol
	before := s.currentIdentity()
	svc.StagingThreshold = map[bool]int{false: -1, true: 1}[staged]
	defer func() { svc.StagingThreshold = 0 }()
	report, err := svc.Rebuild(context.Background(), s.account)
	if err != nil {
		s.t.Fatal(err)
	}
	return report.Staged == staged && s.currentIdentity().Generation > before.Generation
}

// ---- the scheduled transitions

type runBackup struct {
	file, cursor string
	entries      []any
}

// backup takes the administrator's backup and starts recording the
// phone's facts (what it holds that the backup does not).
func (s *commitRun) backup() *runBackup {
	head := s.get("/v1/read/summary").must(http.StatusOK)
	fr := s.get("/v1/read/frontier?cursor=" + url.QueryEscape(head.obj("head")["cursor"].(string))).must(http.StatusOK)
	b := &runBackup{file: s.adminDo(s.readEnv.session, http.MethodPost, "/v1/admin/backup", nil).must(http.StatusOK).str("file"), cursor: s.p.cursor}
	s.recorded, s.frontier = &b.entries, append([]any{}, fr.body["frontier"].([]any)...)
	return b
}

// restore restores the backup with the server stopped, signs in again, and
// has the phone recover what it pushed since: the account's epoch rises.
func (s *commitRun) restore(b *runBackup) bool {
	ctx := context.Background()
	before := s.currentIdentity()
	bundle := filepath.Join(s.t.TempDir(), "keys.json")
	pass := []byte("a long passphrase")
	if err := s.store.ExportKeys(ctx, bundle, pass); err != nil {
		s.t.Fatal(err)
	}
	data, _ := os.ReadFile(bundle)
	s.store.Close()
	if _, err := storage.Restore(ctx, storage.RestoreOptions{DataDir: s.dir, Backup: filepath.Join(s.dir, "backups", b.file), Bundle: data,
		Passphrase: pass}); err != nil {
		s.t.Fatal(err)
	}
	s.reopen(withReads(s.t))
	s.srv.libraries.HookForTesting(views.CacheHooks{Published: s.countPublication}) // the new server's
	session := s.env.session("owner", "correct horse battery")
	s.registerWeb(session, newID(), newID()).must(http.StatusOK)
	s.readEnv.session, s.web = session, browser(session)
	// The phone, a successor installation, recovers its facts.
	s.p, s.last, s.recorded, s.frontier = s.readPhone(), "", nil, nil
	recoveryID := newID()
	snap := s.do(http.MethodPost, "/v1/snapshot", map[string]any{"mode": "recovery", "recovery_id": recoveryID, "install_id": s.p.install,
		"acked_install_seq": 0, "cursor": b.cursor}, s.p.cred).must(http.StatusOK)
	for i := 0; i < len(b.entries); i += 500 {
		body := map[string]any{"recovery_id": recoveryID, "entries": b.entries[i:min(len(b.entries), i+500)]}
		up := s.do(http.MethodPost, "/v1/sync/recovery/upload", body, s.p.cred)
		for up.status == http.StatusServiceUnavailable {
			// The upload's job outlasted the request (PROTOCOL §6.11): as a
			// phone does, the identical request is sent again (once the job
			// ended, rather than after retry_after).
			up.expect(http.StatusServiceUnavailable, "server_busy")
			<-s.env.srv.cfg.Protocol.RecoveryJobDoneForTest(s.account)
			up = s.do(http.MethodPost, "/v1/sync/recovery/upload", body, s.p.cred)
		}
		up.must(http.StatusOK)
		for _, r := range up.body["results"].([]any) {
			if st := r.(map[string]any)["status"]; st != "joined" && st != "known" {
				s.t.Fatalf("recovery upload: %s", up.raw)
			}
		}
	}
	s.do(http.MethodPost, "/v1/sync/recovery/finish", map[string]any{"recovery_id": recoveryID, "complete": true}, s.p.cred).must(http.StatusOK)
	s.p.cursor = snap.str("cursor")
	s.p.attachHead()
	return s.currentIdentity().Epoch > before.Epoch
}

// deleteOther deletes another account whose views were read: its tokens
// are refused, and this account's views are untouched (the held pages).
func (s *commitRun) deleteOther() bool {
	admin := s.readEnv.session
	id := s.createAccount(admin, "listener2", "listener2-password", false)
	session := s.env.session("listener2", "listener2-password")
	s.registerWeb(session, newID(), newID()).must(http.StatusOK)
	tok := s.do(http.MethodGet, "/v1/read/queue?anchor_index=0&limit=1", nil, browser(session)).must(http.StatusOK).str("start_page_token")
	s.adminDo(admin, http.MethodPost, "/v1/admin/accounts/"+id+"/delete", map[string]any{"confirm_username": "listener2"}).must(http.StatusOK)
	return s.do(http.MethodGet, "/v1/read/queue?limit=1&page_token="+tok, nil, browser(session)).status == http.StatusUnauthorized
}

// ---- the checks

// holdPages reads the first page of a few lists, and holds tokens of their
// start and next pages with the oracle's rows.
func (s *commitRun) holdPages(ref *reference.Library, qs []readQuery) {
	ident := s.currentIdentity()
	for _, rq := range qs {
		if rq.path == "/v1/read/subscriptions" {
			continue
		}
		path := rq.path
		if rq.podcast != "" {
			path = "/v1/read/podcasts/{id}/episodes"
		}
		want, _, ok := ref.ListItems(path, rq.podcast, rq.q)
		if !ok {
			continue
		}
		limit := 1 + s.r.IntN(40)
		q := url.Values{}
		for k, v := range rq.q {
			q[k] = v
		}
		q.Set("limit", strconv.Itoa(limit))
		at := s.r.IntN(len(want) + 1)
		first := s.get(rq.path + "?" + q.Encode() + "&anchor_index=" + strconv.Itoa(at)).must(http.StatusOK)
		pub := s.pubs.Load() // its view's version is the one published last
		for tok, from := range map[string]int{first.str("start_page_token"): 0, first.str("next_page_token"): min(len(want), at+limit)} {
			if tok == "" {
				continue
			}
			s.held = append(s.held, heldPage{url: rq.path + "?" + q.Encode() + "&page_token=" + tok, ident: ident, pub: pub, want: want[from:min(len(want), from+limit)]})
		}
	}
	if len(s.held) > 8 {
		s.held = s.held[len(s.held)-8:]
	}
}

// checkHeld reads the held pages again.
func (s *commitRun) checkHeld(kind string) {
	cur := s.currentIdentity()
	var kept []heldPage
	for _, h := range s.held {
		res := s.get(h.url)
		if cur != h.ident {
			if res.status != http.StatusGone || bytes.Contains(res.raw, []byte(`"items"`)) {
				s.t.Fatalf("after %s (%+v to %+v): a page of an earlier view: %d %s", kind, h.ident, cur, res.status, res.raw)
			}
			continue
		}
		// Beyond the version before the published one, a view lives only
		// under a lease (at most two versions, the most recently read; the
		// registry's tests check which): its page is the same rows, or 410.
		if n := s.pubs.Load() - h.pub; n > 1 && res.status == http.StatusGone {
			if bytes.Contains(res.raw, []byte(`"items"`)) {
				s.t.Fatalf("after %s: a page of a view %d versions old: %d %s", kind, n, res.status, res.raw)
			}
			continue
		}
		if res.status != http.StatusOK {
			s.t.Fatalf("after %s (%+v, unchanged): a page of an earlier view %s: %d %s", kind, cur, h.url, res.status, res.raw)
		}
		sameRows(s.t, "after "+kind+": a page of an earlier view "+h.url, pageItems(s.t, res), h.want)
		kept = append(kept, h)
	}
	s.held = kept
}

// compareWindows compares a list's first page and a page at a random
// offset (its view's token) with the oracle's.
func (s *commitRun) compareWindows(rq readQuery, ref *reference.Library) {
	path := rq.path
	if rq.podcast != "" {
		path = "/v1/read/podcasts/{id}/episodes"
	}
	want, counters, ok := ref.ListItems(path, rq.podcast, rq.q)
	q := url.Values{}
	for k, v := range rq.q {
		q[k] = v
	}
	limit := 1 + s.r.IntN(500)
	q.Set("limit", strconv.Itoa(limit))
	res := s.get(rq.path + "?" + q.Encode())
	if !ok {
		if res.status == http.StatusOK {
			s.t.Fatalf("%s: answered where the old server refused", rq.url())
		}
		return
	}
	res.must(http.StatusOK)
	body := bodyValues(s.t, res)
	s.checkHead(rq.url(), body["head"], ref)
	if !bytes.Equal(body["view"], counters) || string(body["total"]) != strconv.Itoa(len(want)) {
		s.t.Fatalf("%s: view %s, total %s; the oracle %s, %d", rq.url(), body["view"], body["total"], counters, len(want))
	}
	sameRows(s.t, rq.url(), pageItems(s.t, res), want[:min(limit, len(want))])
	next := res.str("next_page_token")
	if next == "" {
		return
	}
	id, _, err := views.ParsePageToken(next)
	if err != nil {
		s.t.Fatal(err)
	}
	at := s.r.IntN(len(want))
	q.Set("page_token", views.PageToken(id, at))
	page := s.get(rq.path + "?" + q.Encode()).must(http.StatusOK)
	sameRows(s.t, fmt.Sprintf("%s at %d", rq.url(), at), pageItems(s.t, page), want[at:min(len(want), at+limit)])
}

// matrix is the query matrix's lists over the library's podcasts.
func (s *commitRun) matrix(ref *reference.Library) []readQuery {
	var pods []string
	for id := range ref.Podcasts {
		pods = append(pods, id)
	}
	var out []readQuery
	for _, q := range viewstest.QueryMatrix(pods) {
		out = append(out, readQuery{q.Path, q.Podcast, q.Q})
	}
	return out
}

// check compares the served lists and screens with the oracle's: the
// whole matrix, or 24 of its queries.
func (s *commitRun) check(full bool) int {
	ref := s.snapshotRef()
	s.ref = ref
	qs := s.matrix(ref)
	if !full {
		s.r.Shuffle(len(qs), func(i, j int) { qs[i], qs[j] = qs[j], qs[i] })
		qs = qs[:24]
	} else {
		// The whole matrix makes more views than an account keeps (64):
		// the held ones go, as the cap says.
		s.held = nil
	}
	for _, rq := range qs {
		if s.scale { // every list at two windows; the views' own test walks them whole
			s.compareWindows(rq, ref)
		} else {
			s.compareList(rq, ref)
		}
	}
	s.compareScreens(ref, full)
	s.holdPages(ref, qs[:3])
	return len(qs)
}

// ---- the sequences

func newCommitRun(t *testing.T, seed uint64, scale bool) *commitRun {
	r := newReadEnv(t, withImports(t))
	s := &commitRun{readEnv: r, t: t, r: mrand.New(mrand.NewPCG(seed, 8)), bins: map[string]int{}, outcomes: map[string]int{}, applied: map[string]int{}, scale: scale}
	if err := r.store.Read(context.Background(), func(x *storage.ReadTx) error {
		a, err := x.GetAccount(r.account)
		s.ns = a.Namespace
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// The empty account's views, then the import's publication.
	s.check(false)
	items := 13
	if scale {
		items = 108
	}
	start := time.Now()
	before := s.currentIdentity()
	r.importOverHTTP(enlargedBackup(t, 8, items))
	if s.currentIdentity().Generation > before.Generation {
		s.applied["import"]++
	}
	s.checkHeld("the import")
	s.p = r.readPhone()
	s.ref = s.snapshotRef()
	t.Logf("imported %d podcasts, %d episodes in %.1f s", len(s.ref.Podcasts), len(s.ref.Episodes), time.Since(start).Seconds())
	return s
}

func (s *commitRun) run(steps int) {
	t := s.t
	total := 0
	for _, k := range stepKinds {
		total += k.weight
	}
	pick := func() stepKind {
		n := s.r.IntN(total)
		for _, k := range stepKinds {
			if n -= k.weight; n < 0 {
				return k
			}
		}
		return stepKinds[0]
	}
	var b *runBackup
	queried := 0
	start := time.Now()
	// The transitions, among the random steps after every kind's first.
	forced := len(stepKinds)
	rest := steps - forced
	deletion, backup, restore := forced+rest/3, forced+2*rest/3-8, forced+2*rest/3
	for i := range steps {
		var kind string
		var ok bool
		switch {
		case i < len(stepKinds): // every kind first, until it changes the library
			k := stepKinds[i]
			kind = k.name
			for try := 0; !ok && try < 20; try++ {
				ok = k.run(s)
			}
			if !ok {
				t.Fatalf("step %d: %s never changed the library; outcomes %v", i, kind, s.outcomes)
			}
		case i == deletion:
			kind, ok = "account deletion", s.deleteOther()
		case i == backup:
			kind, b, ok = "backup", s.backup(), true
		case i == restore:
			kind, ok = "restore with recovery", s.restore(b)
			b = nil
		case rebuildAfterRecovery && i == restore+1: // the recovered archive rebuilt, the caches warm
			kind, ok = "rebuild after recovery", s.rebuild(false)
		case rebuildAfterRecovery && i == restore+2:
			kind, ok = "staged rebuild after recovery", s.rebuild(true)
		default:
			k := pick()
			// The phone's facts only while a backup is held; after the
			// restore, no rebuild unless rebuildAfterRecovery.
			for b != nil && serverKinds[k.name] || i > restore && !rebuildAfterRecovery && strings.Contains(k.name, "rebuild") {
				k = pick()
			}
			kind, ok = k.name, k.run(s)
		}
		s.bins[kind]++
		if ok {
			s.applied[kind]++
		}
		s.clock.Advance(time.Second)
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("step %d (%s): %v", i, kind, p)
				}
			}()
			s.checkHeld(kind)
			full := (i+1)%50 == 0 && !s.scale || i == steps-1
			queried += s.check(full)
			if full || (i+1)%25 == 0 {
				t.Logf("step %d (%s): %.0f s, %d list queries compared", i, kind, time.Since(start).Seconds(), queried)
			}
		}()
	}
	required := []string{"import", "account deletion", "restore with recovery"}
	if rebuildAfterRecovery {
		required = append(required, "rebuild after recovery", "staged rebuild after recovery")
	}
	for _, k := range append(required, func() (out []string) {
		for _, k := range stepKinds {
			out = append(out, k.name)
		}
		return
	}()...) {
		if s.applied[k] == 0 {
			t.Errorf("%s never changed the library (%d steps of it)", k, s.bins[k])
		}
	}
	t.Logf("%d steps in %.0f s, %d list queries compared; applied %v; outcomes %v", steps, time.Since(start).Seconds(), queried, s.applied, s.outcomes)
}

func TestReadModelRandomCommits(t *testing.T) {
	steps, seeds, scale := 300, []uint64{1, 2}, os.Getenv("NTP_READMODEL_SCALE") != ""
	if scale {
		steps, seeds = 100, []uint64{11}
	}
	if v, err := strconv.Atoi(os.Getenv("NTP_READMODEL_STEPS")); err == nil {
		steps = v
	} else if os.Getenv("NTP_READMODEL_LONG") != "" {
		steps *= 10
	}
	if steps < len(stepKinds)+30 {
		t.Fatalf("%d steps: too few for every kind and the transitions", steps)
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprint("seed ", seed), func(t *testing.T) {
			t.Parallel()
			newCommitRun(t, seed, scale).run(steps)
		})
	}
}

// A rebuild after a recovery, reduced: a backup, then the phone queues an
// episode, listens and sets the queue policy; after the restore its
// recovery joins those facts, and a rebuild of the archive (in place, the
// cache warm) must succeed (the queue replay memo is not reused during an
// archive rebuild; rebuildAfterRecovery).
func TestRebuildAfterRecoveredSessions(t *testing.T) {
	if !rebuildAfterRecovery {
		t.Skip("rebuildAfterRecovery is off: no rebuild of an archive holding recovered listening sessions " +
			"(the queue replay memo during an archive rebuild, ops/queue_replay.go)")
	}
	for seed := uint64(1); seed <= 3; seed++ {
		r := newReadEnv(t)
		r.importInto(r.account, "emulator-parity-library.db")
		s := &commitRun{readEnv: r, t: t, r: mrand.New(mrand.NewPCG(seed, 8)), p: r.readPhone(), bins: map[string]int{}, outcomes: map[string]int{}, applied: map[string]int{}}
		s.ref = s.snapshotRef()
		b := s.backup()
		for _, name := range []string{"queue.add", "session.end", "account.set_queue_policy"} {
			k := stepKinds[slices.IndexFunc(stepKinds, func(k stepKind) bool { return k.name == name })]
			if !k.run(s) {
				t.Fatalf("seed %d: %s did not apply", seed, name)
			}
			s.ref = s.snapshotRef()
		}
		if !s.restore(b) {
			t.Fatalf("seed %d: the restore did not move the epoch", seed)
		}
		if _, err := s.srv.cfg.Protocol.Rebuild(context.Background(), s.account); err != nil {
			t.Errorf("seed %d: a rebuild after the recovery: %v", seed, err)
		}
	}
}
