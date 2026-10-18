package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// observeAlso installs f for the test, before the observer the TestMain
// installed (the read model's, which goes on checking every commit), and
// puts that one back after the test.
func observeAlso(t *testing.T, f CommitObserver) {
	t.Helper()
	prev := commitObserver.Load()
	ObserveCommitsForTesting(func(s *Store, accounts map[string]CommittedAccount) {
		f(s, accounts)
		if prev != nil {
			(*prev)(s, accounts)
		}
	})
	t.Cleanup(func() { commitObserver.Store(prev) })
}

// The commit observer sees each commit with every account it touched (whose
// view inputs it changed, whose log it advanced, that it declared, created
// or deleted) as the commit left them, while the write gate keeps every
// other write out: the newest snapshot is the commit's.
func TestCommitObserverSeesEachCommitAlone(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: newCaptureDir(t)})
	var mu sync.Mutex
	var seen []map[string]CommittedAccount
	var allProblems []string
	observeAlso(t, func(st *Store, accounts map[string]CommittedAccount) {
		if st != s {
			return
		}
		var problems []string
		defer func() {
			mu.Lock()
			allProblems = append(allProblems, problems...)
			mu.Unlock()
		}()
		if st.gate.TryLock() {
			st.gate.Unlock()
			problems = append(problems, "the gate is free while the observer runs")
		}
		for id, want := range accounts {
			var got CommittedAccount
			if err := st.Read(ctx, func(r *ReadTx) error {
				h, err := r.Account(id).Head()
				if errors.Is(err, ErrNoAccount) {
					return nil
				}
				if err != nil {
					return err
				}
				got.Exists, got.Head = true, h
				got.Clock, err = r.Account(id).ViewClock()
				return err
			}); err != nil {
				problems = append(problems, err.Error())
			}
			if got.Exists != want.Exists || got.Head.Seq != want.Head.Seq || got.Head.LogID != want.Head.LogID || got.Head.Epoch != want.Head.Epoch ||
				!bytes.Equal(got.Head.Digest, want.Head.Digest) || got.Clock != want.Clock {
				problems = append(problems, fmt.Sprintf("account %s: the snapshot has %+v, the commit %+v", id, got, want))
			}
		}
		mu.Lock()
		seen = append(seen, accounts)
		mu.Unlock()
	})
	write := func(fn func(tx *WriteTx) error) map[string]CommittedAccount {
		t.Helper()
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if err := s.Write(ctx, fn); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(seen) != n+1 {
			t.Fatalf("%d commits observed, want 1", len(seen)-n)
		}
		return seen[n]
	}
	only := func(got map[string]CommittedAccount, ids ...string) {
		t.Helper()
		if len(got) != len(ids) {
			t.Fatalf("touched %v, want %v", got, ids)
		}
		for _, id := range ids {
			if _, ok := got[id]; !ok {
				t.Fatalf("touched %v, want %v", got, ids)
			}
		}
	}
	// A change of view inputs.
	only(write(func(tx *WriteTx) error {
		return tx.Account(capA).PutRecord(RecordRow{"episode", "e:000", "state", `{"o":1}`, `[]`})
	}), capA)
	// A commit appended with no view input changed.
	only(write(func(tx *WriteTx) error {
		_, _, err := tx.Account(capB).AppendCommit(record(capB, 1))
		return err
	}), capB)
	// A declared bulk rewrite.
	got := write(func(tx *WriteTx) error { return bulkRewrite(tx, capA, func(string) error { return nil }) })
	only(got, capA)
	if got[capA].Clock.Generation != 1 {
		t.Fatalf("the declared account as committed: %+v", got[capA])
	}
	// A created and a deleted account.
	var created string
	got = write(func(tx *WriteTx) error {
		a, err := tx.CreateAccount()
		created = a.ID
		if err != nil {
			return err
		}
		return tx.DeleteAccount(capB)
	})
	only(got, created, capB)
	if !got[created].Exists || got[capB].Exists {
		t.Fatalf("created and deleted: %+v", got)
	}
	// A write that changes nothing of an account touches none.
	only(write(func(tx *WriteTx) error { return tx.SetMeta("observer", "x") }))
	// Concurrent writers: each commit is observed alone.
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 10 {
				if err := s.Write(ctx, func(tx *WriteTx) error {
					return tx.Account(capA).PutRecord(RecordRow{"episode", fmt.Sprintf("e:%03d", i), "state", fmt.Sprintf(`{"w":%d}`, j), `[]`})
				}); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(allProblems) > 0 {
		t.Fatalf("%d problems, first %s", len(allProblems), allProblems[0])
	}
}
