package storage

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
)

// Refresh requests (PROTOCOL §9.6) are three sequences in the schedule row:
// written only by RecordRefreshRequest, FinishRefreshAttempt and
// ResolveRefreshRequests, never by PutRefreshSchedule.

// seedRefreshPodcasts writes n podcasts of an account with subscription,
// policy and identity records and a schedule row each.
func seedRefreshPodcasts(t *testing.T, s *Store, account string, n int) []string {
	t.Helper()
	var ids []string
	if err := s.Write(context.Background(), func(tx *WriteTx) error {
		a := tx.Account(account)
		for i := range n {
			p := fmt.Sprintf("p1:%064x", i+1)
			ids = append(ids, p)
			if _, err := tx.tx.Exec(`INSERT INTO podcasts (account_id, podcast_id, creating_op) VALUES (?, ?, 'op')`, account, p); err != nil {
				return err
			}
			state := []string{"subscribed", "archived", "not_subscribed", "removed", "merged"}[i%5]
			for group, value := range map[string]string{
				"subscription": fmt.Sprintf(`{"generation_id":"g-%d","state":%q}`, i, state),
				"policy":       fmt.Sprintf(`{"keep_updated":%t}`, i%3 != 0),
				"identity":     fmt.Sprintf(`{"refresh_url":"https://feeds.example.com/%d"}`, i),
			} {
				if err := a.PutRecord(RecordRow{Entity: "podcast", EntityID: p, FieldGroup: group, Value: value, Provenance: "[]"}); err != nil {
					return err
				}
			}
			if i%4 == 0 {
				continue // no schedule row yet
			}
			if err := a.PutRefreshSchedule(RefreshRow{PodcastID: p, NextAt: int64(1000 + i), Failures: int64(i % 3), LastAttempt: int64(900 + i)}); err != nil {
				return err
			}
			if i%2 == 1 {
				if _, err := a.RecordRefreshRequest(p, i%3 == 1); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return ids
}

// The sequences: a request counts up; a single podcast's request is the
// newest forced one and stays forced across later ordinary ones; an attempt
// answers what it covers; a forced request made during an ordinary attempt
// stays pending; nothing goes back.
func TestRefreshRequestSequences(t *testing.T) {
	s := openTest(t, config{dataDir: t.TempDir()})
	account := createAccounts(t, s, 1)[0]
	p := seedRefreshPodcasts(t, s, account, 2)[0] // no requests, a schedule row is written below
	step := func(f func(a *AccountTx) error) RefreshRow {
		t.Helper()
		var row RefreshRow
		if err := s.Write(context.Background(), func(tx *WriteTx) error {
			a := tx.Account(account)
			if err := f(a); err != nil {
				return err
			}
			var err error
			row, _, err = a.RefreshSchedule(p)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return row
	}
	if _, err := func() (int64, error) {
		var seq int64
		err := s.Write(context.Background(), func(tx *WriteTx) error {
			var err error
			seq, err = tx.Account(account).RecordRefreshRequest(p, false)
			return err
		})
		return seq, err
	}(); err == nil {
		t.Fatal("a request recorded without a schedule row")
	}
	step(func(a *AccountTx) error { return a.PutRefreshSchedule(RefreshRow{PodcastID: p, NextAt: 1}) })
	record := func(force bool) func(a *AccountTx) error {
		return func(a *AccountTx) error { _, err := a.RecordRefreshRequest(p, force); return err }
	}
	seqs := func(r RefreshRow) [3]int64 { return [3]int64{r.RequestSeq, r.RequestForceSeq, r.DoneSeq} }
	for _, c := range []struct {
		name string
		f    func(a *AccountTx) error
		want [3]int64
	}{
		{"single", record(true), [3]int64{1, 1, 0}},
		{"then all: still forced", record(false), [3]int64{2, 1, 0}},
		{"a forced attempt started at 2 answers both", func(a *AccountTx) error { return a.FinishRefreshAttempt(p, 2, true) }, [3]int64{2, 1, 2}},
		{"all", record(false), [3]int64{3, 1, 2}},
		{"single during that ordinary attempt", record(true), [3]int64{4, 4, 2}},
		{"all again", record(false), [3]int64{5, 4, 2}},
		{"the ordinary attempt started at 3 answers only what it started with", func(a *AccountTx) error { return a.FinishRefreshAttempt(p, 3, false) }, [3]int64{5, 4, 3}},
		{"the forced follow-up answers everything", func(a *AccountTx) error { return a.FinishRefreshAttempt(p, 5, true) }, [3]int64{5, 4, 5}},
		{"an older outcome never goes back", func(a *AccountTx) error { return a.FinishRefreshAttempt(p, 1, false) }, [3]int64{5, 4, 5}},
		{"all", record(false), [3]int64{6, 4, 5}},
		{"an ordinary attempt answers ordinary requests made during it", func(a *AccountTx) error { return a.FinishRefreshAttempt(p, 5, false) }, [3]int64{6, 4, 6}},
		{"all", record(false), [3]int64{7, 4, 6}},
		{"resolving up to a seen sequence", func(a *AccountTx) error { return a.ResolveRefreshRequests(p, 7) }, [3]int64{7, 4, 7}},
		// Two forced requests and an ordinary one during an ordinary attempt that started at 7: it answers
		// only what it started with, neither forced request (only the newest is kept).
		{"single during an ordinary attempt", record(true), [3]int64{8, 8, 7}},
		{"all", record(false), [3]int64{9, 8, 7}},
		{"single again", record(true), [3]int64{10, 10, 7}},
		{"the ordinary attempt answers neither forced request", func(a *AccountTx) error { return a.FinishRefreshAttempt(p, 7, false) }, [3]int64{10, 10, 7}},
		{"the forced follow-up answers both", func(a *AccountTx) error { return a.FinishRefreshAttempt(p, 10, true) }, [3]int64{10, 10, 10}},
		{"resolving never goes past the requests", func(a *AccountTx) error { return a.ResolveRefreshRequests(p, 99) }, [3]int64{10, 10, 10}},
	} {
		if got := seqs(step(c.f)); got != c.want {
			t.Fatalf("%s: (request, force, done) = %v, want %v", c.name, got, c.want)
		}
	}
}

// Writing back a schedule row read before a request (a fetch's
// outcome, a pass of the schedule) keeps the request: PutRefreshSchedule's
// column list leaves the sequences out, on insert and on update.
func TestPutRefreshScheduleKeepsRequests(t *testing.T) {
	s := openTest(t, config{dataDir: t.TempDir()})
	account := createAccounts(t, s, 1)[0]
	p := seedRefreshPodcasts(t, s, account, 2)[1] // a schedule row and a forced request
	if err := s.Write(context.Background(), func(tx *WriteTx) error {
		a := tx.Account(account)
		captured, _, err := a.RefreshSchedule(p)
		if err != nil {
			return err
		}
		if _, err := a.RecordRefreshRequest(p, false); err != nil {
			return err
		}
		captured.NextAt, captured.RequestSeq, captured.RequestForceSeq, captured.DoneSeq = 77, 0, 0, 99
		if err := a.PutRefreshSchedule(captured); err != nil {
			return err
		}
		row, _, err := a.RefreshSchedule(p)
		if err != nil {
			return err
		}
		if row.NextAt != 77 || row.RequestSeq != 2 || row.RequestForceSeq != 1 || row.DoneSeq != 0 {
			t.Errorf("after a write-back: %+v", row)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// The status read of named podcasts reads each by its keys: the
// plan searches every table it reads, so its work stays the same as
// unrelated podcasts grow from 10 to 1,000, with and without the planner's
// statistics; and it reads exactly what RefreshCandidates reads for them.
func TestRefreshStatusForReadsOnlyTheNamedPodcasts(t *testing.T) {
	for _, n := range []int{10, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			dir := t.TempDir()
			s := openTest(t, config{dataDir: dir})
			accounts := createAccounts(t, s, 2)
			ids := seedRefreshPodcasts(t, s, accounts[0], n)
			seedRefreshPodcasts(t, s, accounts[1], 3)
			check := func(state string, s *Store) {
				plan := queryPlan(t, s, refreshCandidateQuery, accounts[0], ids[1])
				for _, line := range plan {
					// The household's few accounts may be scanned; no library table is.
					if strings.HasPrefix(line, "SCAN") && line != "SCAN a" {
						t.Errorf("%s, %d podcasts: the status read scans: %q\nplan:\n%s", state, n, line, strings.Join(plan, "\n"))
					}
				}
			}
			check("no statistics", s)
			analyze(t, s)
			check("analyzed", s)
			s.Close()
			s = openTest(t, config{dataDir: dir})
			check("reopened analyzed", s)

			r := rand.New(rand.NewPCG(uint64(n), 7))
			if err := s.Read(context.Background(), func(tx *ReadTx) error {
				all, err := tx.RefreshCandidates(accounts[0])
				if err != nil {
					return err
				}
				byID := map[string]RefreshCandidate{}
				for _, c := range all {
					byID[c.PodcastID] = c
				}
				for range 20 {
					var named []string
					var want []RefreshCandidate
					seen := map[string]bool{}
					for range 1 + r.IntN(8) {
						id := ids[r.IntN(len(ids))]
						if r.IntN(5) == 0 {
							id = fmt.Sprintf("p1:%064x", 1_000_000+r.IntN(5)) // not the account's
						}
						named = append(named, id)
						if c, ok := byID[id]; ok && !seen[id] {
							want = append(want, c)
						}
						seen[id] = true
					}
					got, err := tx.RefreshStatusFor(accounts[0], named)
					if err != nil {
						return err
					}
					if len(got) != len(want) || (len(want) > 0 && !reflect.DeepEqual(got, want)) {
						t.Fatalf("RefreshStatusFor(%v):\n%+v\nwant\n%+v", named, got, want)
					}
				}
				other, err := tx.RefreshStatusFor(accounts[1], ids[5:8]) // account 1 has only the first three
				if err != nil || len(other) != 0 {
					t.Errorf("another account's podcasts: %+v %v", other, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Migration 0027 keeps every schedule row as it was, without requests.
func TestMigration0027UpgradesSchedules(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, config{dataDir: dir, migrations: embeddedMigrations[:26]})
	account := createAccounts(t, s, 1)[0]
	const columns = `account_id, podcast_id, next_at, failures, not_before, last_attempt, last_success, last_error,
		status_published_at, status_key, context, quiet_since`
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for p := range 3 {
			podcast := fmt.Sprintf("p1:%d", p)
			if _, err := tx.tx.ExecContext(ctx, `INSERT INTO podcasts (account_id, podcast_id, creating_op) VALUES (?, ?, 'op')`, account, podcast); err != nil {
				return err
			}
			if _, err := tx.tx.ExecContext(ctx, `INSERT INTO refresh_schedule (`+columns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				account, podcast, 1000+p, p, 2000, nil, 900+p, `{"label":"io"}`, 800, "ok", "g\x00u\x00", 500+p); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = openTest(t, config{dataDir: dir})
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for p := range 3 {
			podcast := fmt.Sprintf("p1:%d", p)
			row, ok, err := tx.Account(account).RefreshSchedule(podcast)
			want := RefreshRow{PodcastID: podcast, NextAt: int64(1000 + p), Failures: int64(p), NotBefore: 2000, LastSuccess: int64(900 + p),
				LastError: `{"label":"io"}`, StatusPublishedAt: 800, StatusKey: "ok", Context: "g\x00u\x00", QuietSince: int64(500 + p)}
			if err != nil || !ok || row != want {
				t.Errorf("%s after the upgrade: %+v %v %v", podcast, row, ok, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
