package viewstest

import (
	"context"
	"fmt"
	"math/big"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// Spec describes a generated library: synthetic, with the shapes of the
// records an Android import and the server's folds publish.
type Spec struct {
	Seed     uint64
	Podcasts []int // the episodes of each podcast
	Queue    int   // queued episodes
	Now      int64 // ms
	Months   int   // the span of the listening history, in months before Now
	Prefix   string
}

// Library names what Generate wrote.
type Library struct {
	Account  string
	Podcasts []string
	Nodes    []string   // each podcast's creating node
	Episodes [][]string // by podcast
	Spec     Spec
}

// Day is a day in ms.
const Day = int64(24 * 3600 * 1000)

// QueueKeyAt returns a valid queue key, increasing with i.
func QueueKeyAt(i int) string {
	return contract.QueueKey{N: 30, M: big.NewInt(int64(2*i + 1))}.String()
}

// Generate writes the library into the account in one transaction with the
// read model's capture suspended, as an import's publication does.
func Generate(ctx context.Context, s *storage.Store, account string, spec Spec) (*Library, error) {
	lib := &Library{Account: account, Spec: spec}
	r := rand.New(rand.NewPCG(spec.Seed, 31))
	err := s.Write(ctx, func(tx *storage.WriteTx) error {
		if err := tx.SuspendViewCapture(account); err != nil {
			return err
		}
		a := tx.Account(account)
		queued := 0
		total := 0
		for _, n := range spec.Podcasts {
			total += n
		}
		for i, n := range spec.Podcasts {
			p, node := fmt.Sprintf("p1:%s%04d", spec.Prefix, i), fmt.Sprintf("%08x-%04x-4000-8000-%012x", spec.Seed, i, 0xc0de)
			lib.Podcasts, lib.Nodes = append(lib.Podcasts, p), append(lib.Nodes, node)
			if err := a.InsertNode(node, "decision", int64(i), nil); err != nil {
				return err
			}
			if err := a.CreatePodcast(p, node); err != nil {
				return err
			}
			for _, rec := range PodcastRecords(r, i, p) {
				if err := a.PutRecord(rec); err != nil {
					return err
				}
			}
			var eps []string
			for j := range n {
				e := fmt.Sprintf("e1:%s%04d-%05d", spec.Prefix, i, j)
				eps = append(eps, e)
				queue := ""
				if queued < spec.Queue && r.IntN(max(1, total/max(1, spec.Queue))) == 0 {
					queue = QueueKeyAt(queued)
					queued++
				}
				recs, registered, cat := EpisodeRecords(r, spec, p, e, i, j, queue)
				if registered {
					if err := a.AddEpisode(e, p, `{}`, node); err != nil {
						return err
					}
				}
				if cat != nil {
					if err := a.PutCatalogItem(*cat); err != nil {
						return err
					}
				}
				for _, rec := range recs {
					if err := a.PutRecord(rec); err != nil {
						return err
					}
				}
			}
			lib.Episodes = append(lib.Episodes, eps)
		}
		for _, rec := range AccountRecords(account) {
			if err := a.PutRecord(rec); err != nil {
				return err
			}
		}
		return nil
	})
	return lib, err
}

func jsonString(s string) string { return fmt.Sprintf("%q", s) }

// PodcastRecords are a podcast's records: its subscription (mostly
// subscribed), metadata, identity, policy and sometimes credentials.
func PodcastRecords(r *rand.Rand, i int, p string) []storage.RecordRow {
	state := []string{"subscribed", "subscribed", "subscribed", "subscribed", "archived", "not_subscribed"}[r.IntN(6)]
	gen := jsonString(fmt.Sprintf("g1:%016x", r.Uint64()))
	if r.IntN(20) == 0 {
		gen = "null"
	}
	url := fmt.Sprintf("https://example.invalid/feed/%d", i)
	// Titles with characters JSON may escape (rows keep their bytes).
	title := []string{fmt.Sprintf("Podcast %03d", i), fmt.Sprintf("podcast %03d", i), "Émile", "alpha", "", "Q&A <live>"}[r.IntN(6)]
	titleJSON := jsonString(title)
	if r.IntN(12) == 0 {
		titleJSON = "null"
	}
	image := "null"
	if r.IntN(3) > 0 {
		image = jsonString(fmt.Sprintf("https://example.invalid/art/%d.jpg", i))
	}
	out := []storage.RecordRow{
		{Entity: "podcast", EntityID: p, FieldGroup: "subscription", Value: fmt.Sprintf(`{"generation_id":%s,"state":%q}`, gen, state), Provenance: `["n"]`},
		{Entity: "podcast", EntityID: p, FieldGroup: "metadata", Value: fmt.Sprintf(`{"author":null,"description":"About the show","image_url":%s,"link":null,"title":%s}`, image, titleJSON), Provenance: `["n"]`},
		{Entity: "podcast", EntityID: p, FieldGroup: "identity", Value: fmt.Sprintf(`{"aliases":[{"normalized":%q,"url":%q}],"refresh_url":%q}`, url, url, url), Provenance: `["n"]`},
		{Entity: "podcast", EntityID: p, FieldGroup: "policy", Value: fmt.Sprintf(`{"keep_updated":%v,"new_episodes_action":"global"}`, r.IntN(4) > 0), Provenance: `["n"]`},
	}
	switch r.IntN(10) {
	case 0:
		out = append(out, storage.RecordRow{Entity: "podcast", EntityID: p, FieldGroup: "credentials", Value: fmt.Sprintf(`{"secret_id":"s%d","username":"u"}`, i), Provenance: `["n"]`})
	case 1:
		out = append(out, storage.RecordRow{Entity: "podcast", EntityID: p, FieldGroup: "credentials", Value: `{"secret_id":null,"username":"u"}`, Provenance: `["n"]`})
	}
	return out
}

// AccountRecords are the account's policies.
func AccountRecords(account string) []storage.RecordRow {
	return []storage.RecordRow{
		{Entity: "account", EntityID: account, FieldGroup: "discovery_policy", Value: `{"new_episodes_action":"inbox"}`, Provenance: `["n"]`},
		{Entity: "account", EntityID: account, FieldGroup: "queue_policy", Value: `{"enqueue_location":"back","keep_sorted":false,"sort_order":"date_new_old"}`, Provenance: `["n"]`},
	}
}

// EpisodeRecords are an episode's records, whether it is registered, and
// its catalog item (nil for none): states, positions, history,
// favourites, queue membership, measured durations and statistics units
// in distinct months, over the spec's span, with ties in titles, dates,
// durations and sizes.
func EpisodeRecords(r *rand.Rand, spec Spec, p, e string, i, j int, queueKey string) (recs []storage.RecordRow, registered bool, cat *storage.CatalogItem) {
	span := int64(max(1, spec.Months)) * 30 * Day
	when := func() int64 { return spec.Now - r.Int64N(span) }
	pub := spec.Now - int64(r.IntN(max(1, spec.Months*30)))*Day
	if r.IntN(8) == 0 {
		pub = spec.Now - int64(j)*Day // dates that tie across podcasts
	}
	rec := func(group, value string) {
		recs = append(recs, storage.RecordRow{Entity: "episode", EntityID: e, FieldGroup: group, Value: value, Provenance: fmt.Sprintf(`["o%d"]`, r.IntN(9))})
	}
	enclosure := fmt.Sprintf("https://example.invalid/%d/%d.mp3", i, j)
	identity := fmt.Sprintf(`{"occurrence":{"enclosure_url":%q,"guid":"g%d-%d","k":"rss-guid","publication_time":%d,"title":"Episode %d","v":"g%d-%d"},"podcast_id":%q}`,
		enclosure, i, j, pub, j, i, j, p)
	if r.IntN(200) == 0 {
		identity = fmt.Sprintf(`{"merged_into":"e1:%s%04d-%05d","podcast_id":%q}`, spec.Prefix, i, max(0, j-1), p)
	}
	rec("identity", identity)
	if r.IntN(25) > 0 { // else a placeholder: the occurrence stands in
		title := []string{fmt.Sprintf("Episode %d", j), "alpha", "Alpha", "beta", "Émile", "10 things", "2 things", "Q&A <part 2>"}[r.IntN(8)]
		media := "null"
		if r.IntN(15) > 0 {
			media = fmt.Sprintf(`{"duration":%d,"mime_type":"audio/mpeg","size":"%d","url":%q}`, int64(r.IntN(8))*600_000, int64(r.IntN(5))*1_000_000, enclosure)
		}
		link := "null"
		if r.IntN(3) == 0 {
			link = jsonString([]string{"https://example.invalid/a", "https://example.invalid/B", "https://example.invalid/b"}[r.IntN(3)])
		}
		// Chapters and transcripts: some set, the others null.
		chapterURL, transcriptURL, transcriptType := "null", "null", "null"
		if r.IntN(4) == 0 {
			chapterURL = jsonString(fmt.Sprintf("https://example.invalid/c/%d/%d.json", i, j))
		}
		if r.IntN(5) == 0 {
			transcriptURL = jsonString(fmt.Sprintf("https://example.invalid/t/%d/%d.vtt", i, j))
			transcriptType = jsonString([]string{"text/vtt", "application/srt", "application/json"}[r.IntN(3)])
		}
		rec("metadata", fmt.Sprintf(`{"chapter_url":%s,"chapters":[{"start":0,"title":"one"}],"description":"<p>notes %d</p>","image_url":%s,"link":%s,"media":%s,"pub_date":%d,"title":%q,"transcript_type":%s,"transcript_url":%s}`,
			chapterURL, j, map[bool]string{true: jsonString(fmt.Sprintf("https://example.invalid/e/%d/%d.jpg", i, j)), false: "null"}[r.IntN(4) == 0], link, media, pub, title,
			transcriptType, transcriptURL))
	}
	read := []string{"new", "unplayed", "unplayed", "played", "played"}[r.IntN(5)]
	if queueKey != "" && read == "new" {
		read = "unplayed" // queueing clears new
	}
	rec("play_state", fmt.Sprintf(`{"generation_id":"g","play_state":%q}`, read))
	if r.IntN(3) == 0 {
		rec("position", fmt.Sprintf(`{"claim_id":"c","generation_id":"g","position":%d}`, []int64{0, 500, 1000, 1500, 1_200_000}[r.IntN(5)]))
	}
	if r.IntN(3) == 0 {
		last := when()
		if r.IntN(40) == 0 {
			last = -int64(r.IntN(5) + 1) // a negative last played
		}
		rec("last_played", fmt.Sprintf(`{"time":%d}`, last))
		rec("played_time", fmt.Sprintf(`{"played_ms":%d}`, int64(r.IntN(60))*60_000))
		if r.IntN(2) == 0 {
			rec("history", fmt.Sprintf(`{"time":%d}`, last))
		}
	}
	if r.IntN(5) == 0 {
		rec("favorite", `{"on":true}`)
	}
	if queueKey != "" {
		rec("queue_membership", `{"in_queue":true}`)
		rec("queue_position", fmt.Sprintf(`{"key":%q}`, queueKey))
	}
	if r.IntN(6) == 0 {
		rec("measured_duration", fmt.Sprintf(`{"duration_ms":%d}`, int64(r.IntN(90)+1)*60_000))
	}
	if r.IntN(30) == 0 { // merged statistics units, each in its own month
		var units []string
		for k := range 1 + r.IntN(3) {
			units = append(units, fmt.Sprintf(`{"duration_ms":%d,"history":%d,"last_played":%d,"played":%v,"played_ms":%d,"position":0}`,
				600_000, spec.Now-int64(k+1)*31*Day, spec.Now-int64(k+1)*31*Day, r.IntN(2) == 0, int64(r.IntN(30))*60_000))
		}
		rec("statistics_units", `{"units":[`+strings.Join(units, ",")+`]}`)
	}
	registered = r.IntN(40) > 0
	if r.IntN(30) > 0 {
		seen := pub // the catalog's date: when the feed first showed it
		if r.IntN(3) == 0 {
			seen += int64(1+r.IntN(48)) * 3_600_000
		}
		cat = &storage.CatalogItem{EpisodeID: e, PodcastID: p, FirstNode: "n", PubDate: seen}
	}
	return recs, registered, cat
}

// Shape is one podcast of a shaped library.
type Shape struct {
	Episodes int
	Queued   int    // its first Queued episodes are queued
	Months   int    // its episodes are played, one month each, over this many UTC months up to Now's (0: none played)
	State    string // its subscription state ("" for subscribed)
}

// GenerateShaped writes a library of exactly these podcasts (deterministic,
// with no ties to speak of), in one suspended transaction, for the budget
// and boundary tests.
func GenerateShaped(ctx context.Context, s *storage.Store, account, prefix string, now int64, shapes []Shape) (*Library, error) {
	lib := &Library{Account: account}
	start := time.UnixMilli(now).UTC()
	queued := 0
	err := s.Write(ctx, func(tx *storage.WriteTx) error {
		if err := tx.SuspendViewCapture(account); err != nil {
			return err
		}
		a := tx.Account(account)
		for i, sh := range shapes {
			p, node := fmt.Sprintf("p1:%s%04d", prefix, i), fmt.Sprintf("5ead%04x-%04x-4000-8000-%012x", len(prefix), i, 0xbeef)
			lib.Podcasts, lib.Nodes = append(lib.Podcasts, p), append(lib.Nodes, node)
			if err := a.InsertNode(node, "decision", int64(i), nil); err != nil {
				return err
			}
			if err := a.CreatePodcast(p, node); err != nil {
				return err
			}
			state := sh.State
			if state == "" {
				state = "subscribed"
			}
			for _, rec := range []storage.RecordRow{
				{Entity: "podcast", EntityID: p, FieldGroup: "subscription", Value: fmt.Sprintf(`{"generation_id":"g1:%016x","state":%q}`, i, state), Provenance: `["n"]`},
				{Entity: "podcast", EntityID: p, FieldGroup: "metadata", Value: fmt.Sprintf(`{"author":null,"description":null,"image_url":null,"link":null,"title":"Podcast %04d"}`, i), Provenance: `["n"]`},
			} {
				if err := a.PutRecord(rec); err != nil {
					return err
				}
			}
			var eps []string
			for j := range sh.Episodes {
				e := fmt.Sprintf("e1:%s%04d-%06d", prefix, i, j)
				eps = append(eps, e)
				pub := now - int64(j)*Day
				enclosure := fmt.Sprintf("https://example.invalid/%s/%d/%d.mp3", prefix, i, j)
				recs := []storage.RecordRow{
					{Entity: "episode", EntityID: e, FieldGroup: "identity", Value: fmt.Sprintf(`{"occurrence":{"enclosure_url":%q,"guid":"g%d-%d","k":"rss-guid","publication_time":%d,"title":"Episode %d","v":"g%d-%d"},"podcast_id":%q}`,
						enclosure, i, j, pub, j, i, j, p), Provenance: `["n"]`},
					{Entity: "episode", EntityID: e, FieldGroup: "metadata", Value: fmt.Sprintf(`{"chapter_url":null,"description":null,"image_url":null,"link":null,"media":{"duration":1800000,"mime_type":"audio/mpeg","size":"1000000","url":%q},"pub_date":%d,"title":"Episode %d","transcript_type":null,"transcript_url":null}`,
						enclosure, pub, j), Provenance: `["n"]`},
					{Entity: "episode", EntityID: e, FieldGroup: "play_state", Value: `{"generation_id":"g","play_state":"unplayed"}`, Provenance: `["n"]`},
				}
				if j < sh.Queued {
					recs = append(recs,
						storage.RecordRow{Entity: "episode", EntityID: e, FieldGroup: "queue_membership", Value: `{"in_queue":true}`, Provenance: `["n"]`},
						storage.RecordRow{Entity: "episode", EntityID: e, FieldGroup: "queue_position", Value: fmt.Sprintf(`{"key":%q}`, QueueKeyAt(queued)), Provenance: `["n"]`})
					queued++
				}
				if sh.Months > 0 {
					m := start.AddDate(0, -(j % sh.Months), 0)
					last := time.Date(m.Year(), m.Month(), 2, 12, 0, 0, 0, time.UTC).UnixMilli() + int64(j)
					recs = append(recs,
						storage.RecordRow{Entity: "episode", EntityID: e, FieldGroup: "last_played", Value: fmt.Sprintf(`{"time":%d}`, last), Provenance: `["n"]`},
						storage.RecordRow{Entity: "episode", EntityID: e, FieldGroup: "played_time", Value: `{"played_ms":60000}`, Provenance: `["n"]`})
				}
				if err := a.AddEpisode(e, p, `{}`, node); err != nil {
					return err
				}
				if err := a.PutCatalogItem(storage.CatalogItem{EpisodeID: e, PodcastID: p, FirstNode: "n", PubDate: pub}); err != nil {
					return err
				}
				for _, rec := range recs {
					if err := a.PutRecord(rec); err != nil {
						return err
					}
				}
			}
			lib.Episodes = append(lib.Episodes, eps)
		}
		for _, rec := range AccountRecords(account) {
			if err := a.PutRecord(rec); err != nil {
				return err
			}
		}
		return nil
	})
	return lib, err
}
