package views_test

// The maintained queue, Continue and Surprise structures against
// expectations derived independently of the model's helpers: membership
// from the frozen rules (getPausedQueue's recent, getRandomEpisodesCursor's
// eligibility), order from the contract's queue-key comparison, keys from
// an own copy of the hash; and EqualVersions against versions that are
// deliberately wrong.

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// frozenRecent is getPausedQueue's recent, as the frozen screens compute
// it: an episode with media counts by its last played when its position is
// at least a second, or when it was played in the last 30 s.
func frozenRecent(e *views.EpisodeItem, now int64) int64 {
	if e.HasMedia && (e.Position >= 1000 || e.LastPlayed >= now-30_000) {
		return e.LastPlayed
	}
	return 0
}

// ownHash is SplitMix64 of FNV-1a of the ID, written out again.
func ownHash(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	h += 0x9e3779b97f4a7c15
	h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
	h = (h ^ (h >> 27)) * 0x94d049bb133111eb
	return h ^ (h >> 31)
}

// expectedStructures derives every queue-keyed and Surprise tree's entries
// from the stored episodes, by the frozen rules.
func expectedStructures(v *views.Version) (queues map[string][]views.QueueEntry, never, played []views.CandidateEntry, maxPub int64) {
	queues = map[string][]views.QueueEntry{"queue": {}, "pos": {}, "recent": {}, "zero": {}, "timeZero": {}, "neg": {}}
	never, played, maxPub = []views.CandidateEntry{}, []views.CandidateEntry{}, math.MinInt64
	for _, e := range v.AllEpisodes() {
		if e.Queued {
			plain := views.QueueEntry{Podcast: e.PodcastID, Key: e.QueueKey, ID: e.ID, Stored: true}
			byLast := plain
			byLast.LastPlayed = e.LastPlayed
			queues["queue"] = append(queues["queue"], plain)
			// The recent at any later time, and at the time of the play.
			far, near := frozenRecent(e, math.MaxInt64/2), frozenRecent(e, e.LastPlayed)
			switch {
			case far > 0:
				queues["pos"] = append(queues["pos"], byLast)
			case far < 0:
				queues["neg"] = append(queues["neg"], byLast)
			case near > 0: // positive only within the window
				queues["recent"] = append(queues["recent"], byLast)
				queues["timeZero"] = append(queues["timeZero"], plain)
			default:
				queues["zero"] = append(queues["zero"], plain)
			}
		}
		if (e.Read == -1 || e.Read == 0) && e.HasMedia {
			switch {
			case e.LastPlayed == 0:
				never = append(never, views.CandidateEntry{Podcast: e.PodcastID, U: ownHash(e.ID), ID: e.ID, Stored: true})
				maxPub = max(maxPub, e.PubDate)
			case e.LastPlayed > 0:
				played = append(played, views.CandidateEntry{Podcast: e.PodcastID, LastPlayed: e.LastPlayed, ID: e.ID, Stored: true})
			}
		}
	}
	plainOrder := func(a, b views.QueueEntry) int {
		return cmp.Or(strings.Compare(a.Podcast, b.Podcast), contract.CompareQueueKeys(a.Key, b.Key), strings.Compare(a.ID, b.ID))
	}
	lastOrder := func(a, b views.QueueEntry) int {
		return cmp.Or(strings.Compare(a.Podcast, b.Podcast), -cmp.Compare(a.LastPlayed, b.LastPlayed), contract.CompareQueueKeys(a.Key, b.Key),
			strings.Compare(a.ID, b.ID))
	}
	for name, list := range queues {
		if name == "pos" || name == "recent" || name == "neg" {
			slices.SortFunc(list, lastOrder)
		} else {
			slices.SortFunc(list, plainOrder)
		}
	}
	slices.SortFunc(never, func(a, b views.CandidateEntry) int {
		return cmp.Or(strings.Compare(a.Podcast, b.Podcast), cmp.Compare(a.U, b.U), strings.Compare(a.ID, b.ID))
	})
	slices.SortFunc(played, func(a, b views.CandidateEntry) int {
		return cmp.Or(strings.Compare(a.Podcast, b.Podcast), cmp.Compare(a.LastPlayed, b.LastPlayed), strings.Compare(a.ID, b.ID))
	})
	return queues, never, played, maxPub
}

// checkStructures walks the version's actual trees and compares them with
// the expectations: entries, keys, order, payloads and the publication
// dates' maximum.
func checkStructures(t *testing.T, v *views.Version) {
	t.Helper()
	wantQ, wantNever, wantPlayed, wantMax := expectedStructures(v)
	gotQ := views.QueueTrees(v)
	sameQueue := func(a, b views.QueueEntry) bool {
		return a.Podcast == b.Podcast && a.LastPlayed == b.LastPlayed && contract.CompareQueueKeys(a.Key, b.Key) == 0 && a.ID == b.ID && a.Stored == b.Stored
	}
	for name, want := range wantQ {
		got := gotQ[name]
		if !slices.EqualFunc(got, want, sameQueue) {
			t.Fatalf("the %s tree: %s, expected %s", name, showQueue(got), showQueue(want))
		}
	}
	gotNever, gotPlayed, gotMax := views.SurpriseTrees(v)
	sameCandidate := func(a, b views.CandidateEntry) bool {
		return a.Podcast == b.Podcast && a.U == b.U && a.LastPlayed == b.LastPlayed && a.ID == b.ID && a.Stored == b.Stored
	}
	if !slices.EqualFunc(gotNever, wantNever, sameCandidate) {
		t.Fatalf("the never-played Surprise tree: %v, expected %v", ids(gotNever), ids(wantNever))
	}
	if !slices.EqualFunc(gotPlayed, wantPlayed, sameCandidate) {
		t.Fatalf("the played Surprise tree: %v, expected %v", ids(gotPlayed), ids(wantPlayed))
	}
	if gotMax != wantMax {
		t.Fatalf("the never-played tree's latest publication %d, expected %d", gotMax, wantMax)
	}
}

func showQueue(list []views.QueueEntry) string {
	var b strings.Builder
	for _, e := range list {
		fmt.Fprintf(&b, "[%s %d %s %s %v] ", e.Podcast, e.LastPlayed, e.Key, e.ID, e.Stored)
	}
	return b.String()
}

func ids(list []views.CandidateEntry) []string {
	var out []string
	for _, e := range list {
		out = append(out, e.ID)
	}
	return out
}

// witnessLibrary is a store with two podcasts of three episodes, nothing
// queued or played, for transitions committed one by one.
func witnessLibrary(t *testing.T) *shaped {
	return openShaped(t, "w", []viewstest.Shape{{Episodes: 3}, {Episodes: 3}})
}

// treesOf lists the queue-keyed and Surprise trees holding the episode.
func treesOf(v *views.Version, id string) []string {
	var out []string
	for name, list := range views.QueueTrees(v) {
		for _, e := range list {
			if e.ID == id {
				out = append(out, name)
			}
		}
	}
	never, played, _ := views.SurpriseTrees(v)
	for _, e := range never {
		if e.ID == id {
			out = append(out, "never played")
		}
	}
	for _, e := range played {
		if e.ID == id {
			out = append(out, "played")
		}
	}
	slices.Sort(out)
	return out
}

// Deterministic witnesses: one queued episode taken through each Continue
// partition boundary (positions 999, 1000 and 1001; positive, zero and
// negative last played; media lost and back), its Surprise states (new,
// unplayed, played; never played and played), unqueued, with its podcast
// hidden and shown and the episode moved; and two episodes queued in the
// reverse of their keys' order. After each commit the applied version
// equals a full build, its trees are as derived independently, and the
// episode is in exactly the trees listed.
func TestQueueAndHomeStructures(t *testing.T) {
	s := witnessLibrary(t)
	// other is of the podcast the episode moves to: the two share a queue
	// range at the end.
	e, other := s.lib.Episodes[0][0], s.lib.Episodes[1][0]
	a, b := s.lib.Podcasts[0], s.lib.Podcasts[1]
	now := viewstest.FixedNow
	rec := func(group, value string) func(*storage.AccountTx) error {
		return func(a *storage.AccountTx) error { return put(a, "episode", e, group, value) }
	}
	all := func(fs ...func(*storage.AccountTx) error) func(*storage.AccountTx) error {
		return func(a *storage.AccountTx) error {
			for _, f := range fs {
				if err := f(a); err != nil {
					return err
				}
			}
			return nil
		}
	}
	at := func(position, last int64) func(*storage.AccountTx) error {
		return all(rec("position", fmt.Sprintf(`{"claim_id":"c","generation_id":"g","position":%d}`, position)),
			rec("last_played", fmt.Sprintf(`{"time":%d}`, last)))
	}
	queue := all(rec("queue_membership", `{"in_queue":true}`), rec("queue_position", fmt.Sprintf(`{"key":%q}`, viewstest.QueueKeyAt(5))))
	media := func(on bool) func(*storage.AccountTx) error {
		m := `null`
		if on {
			m = `{"duration":1800000,"mime_type":"audio/mpeg","size":"1000000","url":"https://example.invalid/w.mp3"}`
		}
		return rec("metadata", fmt.Sprintf(`{"chapter_url":null,"description":null,"image_url":null,"link":null,"media":%s,"pub_date":%d,"title":"W","transcript_type":null,"transcript_url":null}`, m, now))
	}
	state := func(p, st string) func(*storage.AccountTx) error {
		return func(x *storage.AccountTx) error {
			return put(x, "podcast", p, "subscription", fmt.Sprintf(`{"generation_id":"g1:%s","state":%q}`, p, st))
		}
	}
	steps := []struct {
		name  string
		write func(*storage.AccountTx) error
		in    []string
	}{
		{"queued, not played", queue, []string{"never played", "queue", "zero"}},
		{"position 999, played", at(999, 1), []string{"played", "queue", "recent", "timeZero"}},
		{"position 1000", at(1000, 1), []string{"played", "pos", "queue"}},
		{"position 1001, negative last played", at(1001, -3), []string{"neg", "queue"}},
		{"position 1000, never played", at(1000, 0), []string{"never played", "queue", "zero"}},
		{"position 999, negative last played", at(999, -3), []string{"queue", "zero"}},
		{"played just now at 2000", at(2000, now-29_999), []string{"played", "pos", "queue"}},
		{"media lost", media(false), []string{"queue", "zero"}},
		{"media back", media(true), []string{"played", "pos", "queue"}},
		{"played", rec("play_state", `{"generation_id":"g","play_state":"played"}`), []string{"pos", "queue"}},
		{"new", rec("play_state", `{"generation_id":"g","play_state":"new"}`), []string{"played", "pos", "queue"}},
		{"podcast hidden", state(a, "removed"), []string{"played", "pos", "queue"}},
		{"moved to the other podcast", func(x *storage.AccountTx) error {
			return edit(x, "episode", e, "identity", map[string]any{"podcast_id": b}, `["w"]`)
		}, []string{"played", "pos", "queue"}},
		{"podcast shown", state(a, "subscribed"), []string{"played", "pos", "queue"}},
		{"unqueued", rec("queue_membership", `{"in_queue":false}`), []string{"played"}},
		{"unplayed, never played", all(rec("play_state", `{"generation_id":"g","play_state":"unplayed"}`), at(0, 0)), []string{"never played"}},
		{"two queued in the reverse of their keys", all(queue,
			func(x *storage.AccountTx) error {
				if err := put(x, "episode", other, "queue_membership", `{"in_queue":true}`); err != nil {
					return err
				}
				return put(x, "episode", other, "queue_position", fmt.Sprintf(`{"key":%q}`, viewstest.QueueKeyAt(1)))
			}), []string{"never played", "queue", "zero"}},
	}
	for _, st := range steps {
		if _, path := s.commit(st.write); path != views.PathApply {
			t.Fatalf("%s: the %s", st.name, path)
		}
		checkStructures(t, s.v)
		if got := treesOf(s.v, e); !slices.Equal(got, st.in) {
			t.Fatalf("%s: the episode is in %v, expected %v", st.name, got, st.in)
		}
	}
	q := views.QueueTrees(s.v)["queue"]
	if len(q) != 2 || q[0].ID != other || q[1].ID != e {
		t.Fatalf("the queue %s, expected %s then %s", showQueue(q), other, e)
	}
}

// EqualVersions rejects versions that differ: in each case a copy of a
// correct version with one thing wrong.
func TestEqualVersionsRejectsWrongVersions(t *testing.T) {
	s := openShaped(t, "e", []viewstest.Shape{{Episodes: 30, Queued: 20, Months: 3}, {Episodes: 10, Queued: 2, Months: 1}})
	// A Continue partition of each kind, played and never-played
	// candidates, statistics and credentials.
	q := s.lib.Episodes[0]
	s.commit(func(a *storage.AccountTx) error {
		for i, pl := range []struct{ position, last int64 }{{2000, 5}, {500, 5}, {0, 0}, {2000, -3}} {
			if err := put(a, "episode", q[i], "position", fmt.Sprintf(`{"claim_id":"c","generation_id":"g","position":%d}`, pl.position)); err != nil {
				return err
			}
			if err := put(a, "episode", q[i], "last_played", fmt.Sprintf(`{"time":%d}`, pl.last)); err != nil {
				return err
			}
		}
		for _, e := range q[20:25] {
			if err := put(a, "episode", e, "last_played", `{"time":0}`); err != nil {
				return err
			}
		}
		return put(a, "podcast", s.lib.Podcasts[0], "credentials", `{"secret_id":"s1","username":"u"}`)
	})
	checkStructures(t, s.v)
	if err := views.EqualVersions(s.v, s.v); err != nil {
		t.Fatalf("a version differs from itself: %v", err)
	}
	cases := views.Corruptions(s.v)
	for _, want := range []string{"stale queue payload", "stale pos payload", "stale recent payload", "stale zero payload", "stale timeZero payload",
		"stale neg payload", "stale surprise payload", "stale recently played payload", "stale date_new_old payload", "changed date_new_old key",
		"missing queue entry", "changed raw value", "changed provenance", "changed derived field", "missing stored episode", "extra stored episode",
		"wrong class count", "wrong total", "wrong statistics sum", "wrong month", "wrong podcast month", "changed podcast field", "changed account records"} {
		if cases[want] == nil {
			t.Errorf("no %q case: the version lacks the entries for it", want)
		}
	}
	for name, c := range cases {
		if err := views.EqualVersions(c, s.v); err == nil {
			t.Errorf("%s: EqualVersions accepted it", name)
		}
		if err := views.EqualVersions(s.v, c); err == nil {
			t.Errorf("%s: EqualVersions accepted it (the other way)", name)
		}
	}
	t.Logf("%d wrong versions rejected", len(cases))
}

// The applied and the built versions carry the snapshot's identity: its
// account, its full head and its clock, through a commit with a view
// change and a log commit, a log commit alone (the view's tick unchanged),
// a new generation and a new lineage.
func TestVersionIdentityIsTheSnapshots(t *testing.T) {
	s := witnessLibrary(t)
	e := s.lib.Episodes[0][0]
	ctx := context.Background()
	for _, st := range []struct {
		name  string
		write func(tx *storage.WriteTx) error
		path  views.Path
	}{
		{"a view change with a commit", func(tx *storage.WriteTx) error {
			a := tx.Account(s.account)
			if err := put(a, "episode", e, "favorite", `{"on":true}`); err != nil {
				return err
			}
			_, _, err := a.AppendCommit(storage.CommitBody{})
			return err
		}, views.PathApply},
		{"a commit alone", func(tx *storage.WriteTx) error {
			_, _, err := tx.Account(s.account).AppendCommit(storage.CommitBody{})
			return err
		}, views.PathExact},
		{"a new generation", func(tx *storage.WriteTx) error {
			if err := tx.SuspendViewCapture(s.account); err != nil {
				return err
			}
			return put(tx.Account(s.account), "episode", e, "favorite", `{"on":false}`)
		}, views.PathGeneration},
		{"a new lineage", func(tx *storage.WriteTx) error { return tx.RestoreTransition("a test") }, views.PathGeneration},
	} {
		if err := s.store.Write(ctx, st.write); err != nil {
			t.Fatalf("%s: %v", st.name, err)
		}
		if err := s.store.Read(ctx, func(r *storage.ReadTx) error {
			acct := r.Account(s.account)
			head, err := acct.Head()
			if err != nil {
				return err
			}
			clock, err := acct.ViewClock()
			if err != nil {
				return err
			}
			next, path, err := views.Advance(s.v, acct, s.account)
			if err != nil {
				return err
			}
			full, err := views.BuildVersion(acct, s.account)
			if err != nil {
				return err
			}
			for what, v := range map[string]*views.Version{"advanced": next, "built": full} {
				if msg := viewstest.IdentityMismatch(v, s.account, head, clock); msg != "" {
					t.Errorf("%s: the %s version %s", st.name, what, msg)
				}
			}
			if path != st.path {
				t.Errorf("%s: the %s, expected the %s", st.name, path, st.path)
			}
			if err := views.EqualVersions(next, full); err != nil {
				t.Errorf("%s: %v", st.name, err)
			}
			s.v = next
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}
