package views

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/ptree"
)

// Fingerprint hashes everything a version holds: a retained version must
// never change (the immutability invariant).
func Fingerprint(v *Version) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s %+v %+v %+v\n", v.Account, v.Head, v.Clock, v.AccountRecords)
	write := func(name string, walk func(io.Writer)) {
		fmt.Fprintln(h, name)
		walk(h)
	}
	write("podcasts", func(w io.Writer) {
		for k, p := range v.podcasts.All() {
			fmt.Fprintf(w, "%s %+v %s\n", k, *p, deref(p.Title))
		}
	})
	write("stored", func(w io.Writer) {
		for k, e := range v.stored.All() {
			fmt.Fprintf(w, "%s %+v %s %s %v\n", k, *e, deref(e.Title), deref(e.Link), e.QueueKey.M)
		}
	})
	write("agg", func(w io.Writer) {
		for k, a := range v.agg.All() {
			fmt.Fprintf(w, "%s %+v\n", k, *a)
		}
	})
	for i := range v.orders {
		write(OrderNames[i], func(w io.Writer) {
			for k := range v.orders[i].All() {
				fmt.Fprintf(w, "%+v\n", k)
			}
		})
	}
	for i, t := range []interface {
		All() func(func(queueKey, *EpisodeItem) bool)
	}{v.queue, v.pos, v.recent, v.zero, v.timeZero, v.neg} {
		write(fmt.Sprint("queue ", i), func(w io.Writer) {
			for k := range t.All() {
				fmt.Fprintln(w, showQueueKey(k))
			}
		})
	}
	write("surprise", func(w io.Writer) {
		for k := range v.surprise.All() {
			fmt.Fprintf(w, "%+v\n", k)
		}
	})
	write("played", func(w io.Writer) {
		for k := range v.recentPlayed.All() {
			fmt.Fprintf(w, "%+v\n", k)
		}
	})
	write("stats", func(w io.Writer) {
		for k, x := range v.stats.All() {
			fmt.Fprintf(w, "%+v %+v\n", k, x)
		}
	})
	write("podMonths", func(w io.Writer) {
		for k, x := range v.podMonths.All() {
			fmt.Fprintf(w, "%+v %d\n", k, x)
		}
	})
	write("months", func(w io.Writer) {
		for k, x := range v.months.All() {
			fmt.Fprintf(w, "%d %d\n", k, x)
		}
	})
	return hex.EncodeToString(h.Sum(nil))
}

// SharedStructures lists the structures two versions share whole (the
// same root): a change that should not touch them leaves them shared.
func SharedStructures(a, b *Version) map[string]bool {
	out := map[string]bool{
		"queue": a.queue.SameRoot(b.queue), "pos": a.pos.SameRoot(b.pos), "recent": a.recent.SameRoot(b.recent),
		"zero": a.zero.SameRoot(b.zero), "timeZero": a.timeZero.SameRoot(b.timeZero), "neg": a.neg.SameRoot(b.neg),
		"stored": a.stored.SameRoot(b.stored), "agg": a.agg.SameRoot(b.agg), "surprise": a.surprise.SameRoot(b.surprise),
		"recentPlayed": a.recentPlayed.SameRoot(b.recentPlayed), "stats": a.stats.SameRoot(b.stats),
		"podMonths": a.podMonths.SameRoot(b.podMonths), "months": a.months.SameRoot(b.months), "podcasts": a.podcasts.SameRoot(b.podcasts),
	}
	for i := range a.orders {
		out[OrderNames[i]] = a.orders[i].SameRoot(b.orders[i])
	}
	return out
}

// MonthCount is the number of months with played time.
func MonthCount(v *Version) int { return v.months.Len() }

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

// QueueEntry is an entry of one of the version's queue-keyed trees, as the
// tree holds it.
type QueueEntry struct {
	Podcast    string
	LastPlayed int64 // the key's last played in the trees ordered by it, else 0
	Key        contract.QueueKey
	ID         string
	Stored     bool // the entry's episode is the version's stored one
	Episode    *EpisodeItem
}

// QueueTrees walks the version's queue and Continue partitions, by name.
func QueueTrees(v *Version) map[string][]QueueEntry {
	out := map[string][]QueueEntry{}
	for name, t := range map[string]ptree.Tree[queueKey, *EpisodeItem]{"queue": v.queue, "pos": v.pos, "recent": v.recent, "zero": v.zero,
		"timeZero": v.timeZero, "neg": v.neg} {
		list := []QueueEntry{}
		for k, e := range t.All() {
			s, _ := v.stored.Get(e.ID)
			list = append(list, QueueEntry{Podcast: k.podcast, LastPlayed: -k.last, Key: k.key, ID: k.id, Stored: s == e && e.ID == k.id, Episode: e})
		}
		out[name] = list
	}
	return out
}

// CandidateEntry is an entry of one of the version's Surprise trees.
type CandidateEntry struct {
	Podcast    string
	U          uint64 // the never-played tree's score base
	LastPlayed int64  // the played tree's last played
	ID         string
	Stored     bool
	Episode    *EpisodeItem
}

// SurpriseTrees walks the never-played and the played Surprise candidates,
// and returns the never-played tree's maximum publication date.
func SurpriseTrees(v *Version) (never, played []CandidateEntry, maxPubDate int64) {
	never, played = []CandidateEntry{}, []CandidateEntry{}
	for k, e := range v.surprise.All() {
		s, _ := v.stored.Get(e.ID)
		never = append(never, CandidateEntry{Podcast: k.podcast, U: k.u, ID: k.id, Stored: s == e && e.ID == k.id, Episode: e})
	}
	for k, e := range v.recentPlayed.All() {
		s, _ := v.stored.Get(e.ID)
		played = append(played, CandidateEntry{Podcast: k.podcast, LastPlayed: k.last, ID: k.id, Stored: s == e && e.ID == k.id, Episode: e})
	}
	return never, played, v.surprise.Agg().Max
}

// Corruptions are deliberately wrong copies of a version, each one way
// EqualVersions must reject: a stale payload under an index's key, a
// changed record or derived field, a missing or extra entry, a changed key
// or aggregate.
func Corruptions(v *Version) map[string]*Version {
	out := map[string]*Version{}
	cp := func() *Version { c := *v; return &c }
	stale := func(e *EpisodeItem) *EpisodeItem {
		s := *e
		s.Position++ // the same ID and key, an older payload
		return &s
	}
	// A stale payload in each secondary index.
	queueTrees := map[string]func(*Version) *ptree.Tree[queueKey, *EpisodeItem]{
		"queue":    func(c *Version) *ptree.Tree[queueKey, *EpisodeItem] { return &c.queue },
		"pos":      func(c *Version) *ptree.Tree[queueKey, *EpisodeItem] { return &c.pos },
		"recent":   func(c *Version) *ptree.Tree[queueKey, *EpisodeItem] { return &c.recent },
		"zero":     func(c *Version) *ptree.Tree[queueKey, *EpisodeItem] { return &c.zero },
		"timeZero": func(c *Version) *ptree.Tree[queueKey, *EpisodeItem] { return &c.timeZero },
		"neg":      func(c *Version) *ptree.Tree[queueKey, *EpisodeItem] { return &c.neg },
	}
	for name, field := range queueTrees {
		if k, e, ok := field(v).First().Next(); ok {
			c := cp()
			*field(c) = field(c).Set(k, stale(e))
			out["stale "+name+" payload"] = c
			c = cp()
			*field(c) = field(c).Delete(k)
			out["missing "+name+" entry"] = c
		}
	}
	for i := range v.orders {
		if k, e, ok := v.orders[i].First().Next(); ok {
			c := cp()
			c.orders[i] = c.orders[i].Set(k, stale(e))
			out["stale "+OrderNames[i]+" payload"] = c
			c = cp()
			k2 := k
			k2.rank++
			c.orders[i] = c.orders[i].Delete(k).Set(k2, e)
			out["changed "+OrderNames[i]+" key"] = c
		}
	}
	if k, e, ok := v.surprise.First().Next(); ok {
		c := cp()
		c.surprise = c.surprise.Set(k, stale(e))
		out["stale surprise payload"] = c
	}
	if k, e, ok := v.recentPlayed.First().Next(); ok {
		c := cp()
		c.recentPlayed = c.recentPlayed.Set(k, stale(e))
		out["stale recently played payload"] = c
	}
	// A stored episode changed, every index pointing to the change: only
	// a record or a derived field differs.
	if id, e, ok := v.stored.First().Next(); ok {
		replace := func(name string, n *EpisodeItem) {
			c := cp()
			c.stored = c.stored.Set(id, n)
			for i, d := range orderDefs {
				c.orders[i] = c.orders[i].Set(d.key(e), n)
			}
			out[name] = c
		}
		recs := maps.Clone(e.Records)
		for g, r := range recs {
			r.Value = append(json.RawMessage(nil), `{"changed":true}`...)
			recs[g] = r
			break
		}
		n := *e
		n.Records = recs
		replace("changed raw value", &n)
		recs = maps.Clone(e.Records)
		for g, r := range recs {
			r.Provenance = json.RawMessage(`["changed"]`)
			recs[g] = r
			break
		}
		n = *e
		n.Records = recs
		replace("changed provenance", &n)
		n = *e
		n.ChapterURL += "x"
		replace("changed derived field", &n)
		c := cp()
		c.stored = c.stored.Delete(id)
		out["missing stored episode"] = c
		c = cp()
		extra := *e
		extra.ID += "-extra"
		c.stored = c.stored.Set(extra.ID, &extra)
		out["extra stored episode"] = c
	}
	if id, a, ok := v.agg.First().Next(); ok {
		c := cp()
		w := *a
		w.Hist[0]++
		c.agg = c.agg.Set(id, &w)
		out["wrong class count"] = c
		c = cp()
		w = *a
		w.TotalMS++
		c.agg = c.agg.Set(id, &w)
		out["wrong total"] = c
	}
	if k, x, ok := v.stats.First().Next(); ok {
		c := cp()
		x.played++
		c.stats = c.stats.Set(k, x)
		out["wrong statistics sum"] = c
	}
	if k, x, ok := v.months.First().Next(); ok {
		c := cp()
		c.months = c.months.Set(k, x+1)
		out["wrong month"] = c
	}
	if k, x, ok := v.podMonths.First().Next(); ok {
		c := cp()
		c.podMonths = c.podMonths.Set(k, x+1)
		out["wrong podcast month"] = c
	}
	if id, p, ok := v.podcasts.First().Next(); ok {
		c := cp()
		n := *p
		n.SecretID += "x"
		c.podcasts = c.podcasts.Set(id, &n)
		out["changed podcast field"] = c
	}
	c := cp()
	c.AccountRecords = map[string]RawRecord{"queue_policy": {Value: json.RawMessage(`{}`), Provenance: json.RawMessage(`[]`)}}
	out["changed account records"] = c
	return out
}

// LibraryIncarnation and LibraryPublication are a library's incarnation and
// publication number in the cache (0 for a library made elsewhere).
func LibraryIncarnation(l *Library) uint64 { return l.inc }
func LibraryPublication(l *Library) uint64 { return l.pub }

// MaxVersions is the structural bound on an account's reachable library
// versions: the published one, the one before it, maxLeased leased ones, a
// build in progress with its verification's full build, and one per
// admitted response.
const MaxVersions = 2 + maxLeased + 2 + maxHolds

// Reachable counts the distinct library versions the account's state keeps
// reachable: its published version, its views' (leased ones included), the
// libraries pinned by responses being made, and a build in progress (two
// when it is verified).
func Reachable(c *Cache, account string) int {
	pubs, others := c.spool.pinned(account)
	building := 0
	c.mu.Lock()
	if a := c.accounts[account]; a != nil {
		if a.current != nil {
			pubs[a.current.pub] = true
		}
		if a.flight != nil {
			building = 1
			if a.flight.verify {
				building = 2
			}
		}
	}
	c.mu.Unlock()
	return len(pubs) + others + building
}

// SetHoldWaitForTesting sets how long a page read waits for admission.
func SetHoldWaitForTesting(s *Spool, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holdWait = d
}

// Suspect is the account's count of verified applies still needed.
func Suspect(c *Cache, account string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if a := c.accounts[account]; a != nil {
		return a.suspect
	}
	return 0
}

// HeldAccounts counts the accounts whose library is held: cached, or
// released with a response still being made.
func HeldAccounts(c *Cache) int {
	held := c.spool.heldAccounts()
	c.mu.Lock()
	defer c.mu.Unlock()
	for id := range c.accounts {
		held[id] = true
	}
	return len(held)
}

// SetCaptureForTesting makes the cache build in full for every read, as for
// a store without change capture.
func SetCaptureForTesting(c *Cache, capture bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.capture = capture
}
