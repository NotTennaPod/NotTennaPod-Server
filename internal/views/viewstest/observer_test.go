package viewstest_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

func TestMain(m *testing.M) {
	storage.AuditViewCaptureForTesting()
	os.Exit(viewstest.Main(m))
}

func TestReadModelObserverActive(t *testing.T) { viewstest.RequireActive(t) }

// observedLibrary is a store whose account holds a small generated library,
// checked by the observer from its first commit.
func observedLibrary(t *testing.T) (*storage.Store, string, string) {
	t.Helper()
	ctx := context.Background()
	s, err := storage.Open(ctx, filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	var account string
	if err := s.Write(ctx, func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		account = a.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	lib, err := viewstest.Generate(ctx, s, account, viewstest.Spec{Seed: 3, Podcasts: []int{6, 6}, Queue: 3, Now: viewstest.FixedNow, Months: 6, Prefix: "o"})
	if err != nil {
		t.Fatal(err)
	}
	return s, account, lib.Episodes[0][1]
}

func setPlayState(s *storage.Store, account, episode, state string) error {
	return s.Write(context.Background(), func(tx *storage.WriteTx) error {
		return tx.Account(account).PutRecord(storage.RecordRow{Entity: "episode", EntityID: episode, FieldGroup: "play_state",
			Value: fmt.Sprintf(`{"generation_id":"g","play_state":%q}`, state), Provenance: `["t"]`})
	})
}

// omitted is an advance that read none of the commit's changes: the base's
// content at the snapshot's identity.
func omitted(base, v *views.Version) *views.Version {
	o := *base
	o.Head, o.Clock = v.Head, v.Clock
	return &o
}

// writeRecovering runs a write and returns what it panicked with.
func writeRecovering(f func() error) (p any, err error) {
	defer func() { p = recover() }()
	return nil, f()
}

// An advance that misses a commit's change (an omitted capture) fails the
// writing goroutine with the account, the snapshot and the difference.
func TestObserverFindsAnOmittedCapture(t *testing.T) {
	s, account, episode := observedLibrary(t)
	viewstest.CorruptNextForTesting(s, account, omitted)
	p, err := writeRecovering(func() error { return setPlayState(s, account, episode, "played") })
	m, ok := p.(viewstest.Mismatch)
	if !ok {
		t.Fatalf("the write returned %v and panicked with %v, want a mismatch", err, p)
	}
	if !strings.Contains(m.Message, account) || !strings.Contains(m.Message, "differs from a full build") || !strings.Contains(m.Message, episode) {
		t.Fatalf("the mismatch does not name the account, the difference and the episode: %s", m.Message)
	}
	// The observer goes on from the version it last verified.
	if err := setPlayState(s, account, episode, "unplayed"); err != nil {
		t.Fatal(err)
	}
}

// Commit A's change goes missing in the advance (an omitted capture),
// and commit B restores the value. The observer of A is paused before it
// reads; B cannot begin until A's check is over, so the check sees A's
// snapshot, where the missing change shows, and not B's, where it would not.
func TestObserverChecksEachCommitBeforeTheNext(t *testing.T) {
	s, account, episode := observedLibrary(t)
	var before string
	if err := s.Read(context.Background(), func(r *storage.ReadTx) error {
		rec, err := r.Account(account).Record("episode", episode, "play_state")
		before = rec.Value
		return err
	}); err != nil {
		t.Fatal(err)
	}
	after := `{"generation_id":"g","play_state":"played"}`
	if before == after {
		after = `{"generation_id":"g","play_state":"new"}`
	}
	paused, release := make(chan struct{}), make(chan struct{})
	viewstest.PauseNextForTesting(s, func() { close(paused); <-release })
	viewstest.CorruptNextForTesting(s, account, omitted)
	put := func(value string, began chan struct{}) error {
		return s.Write(context.Background(), func(tx *storage.WriteTx) error {
			if began != nil {
				close(began)
			}
			return tx.Account(account).PutRecord(storage.RecordRow{Entity: "episode", EntityID: episode, FieldGroup: "play_state", Value: value, Provenance: `["t"]`})
		})
	}
	type outcome struct {
		p   any
		err error
	}
	aDone := make(chan outcome, 1)
	go func() {
		p, err := writeRecovering(func() error { return put(after, nil) })
		aDone <- outcome{p, err}
	}()
	<-paused
	bBegan, bDone := make(chan struct{}), make(chan outcome, 1)
	go func() {
		p, err := writeRecovering(func() error { return put(before, bBegan) })
		bDone <- outcome{p, err}
	}()
	// B must stay out for as long as A's check lasts; it would begin at
	// once without the gate.
	select {
	case <-bBegan:
		t.Error("commit B began while the observer was checking commit A")
	case <-time.After(2 * time.Second):
	}
	close(release)
	a := <-aDone
	m, ok := a.p.(viewstest.Mismatch)
	if !ok || !strings.Contains(m.Message, "differs from a full build") {
		t.Errorf("commit A's check: %v (%v), want its missing change found", a.p, a.err)
	}
	if b := <-bDone; b.p != nil || b.err != nil {
		t.Errorf("commit B: %v, %v", b.p, b.err)
	}
}

// The observer checks a commit on a connection of its own: with every pool
// reader held by a reader that waits for the commit, the commit and its
// check still finish.
func TestObserverChecksUnderReaderPressure(t *testing.T) {
	s, account, episode := observedLibrary(t)
	release := make(chan struct{})
	var held sync.WaitGroup
	readers := make(chan error, 4)
	for range 4 { // the pool's size
		held.Add(1)
		go func() {
			readers <- s.Read(context.Background(), func(r *storage.ReadTx) error {
				held.Done()
				<-release // until the commit is over
				_, err := r.Account(account).Head()
				return err
			})
		}()
	}
	held.Wait()
	done := make(chan error, 1)
	go func() { done <- setPlayState(s, account, episode, "played") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		close(release)
	case <-time.After(30 * time.Second):
		t.Error("the commit did not finish while every pool reader was held")
		close(release) // the readers end, and then the commit
		<-done
	}
	for range 4 {
		if err := <-readers; err != nil {
			t.Fatal(err)
		}
	}
}

// A snapshot that neither the full build nor the advance can read is exempt
// only when the oracle cannot load it either.
func TestObserverExemptsOnlyLibrariesTheOracleRefuses(t *testing.T) {
	s, account, episode := observedLibrary(t)
	failure := errors.New("a build failure")
	if err := s.Read(context.Background(), func(r *storage.ReadTx) error {
		if err := viewstest.PairedFailure(r, account, failure); err == nil {
			t.Error("a paired failure on a library the oracle loads was exempt")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// A record that is not an object: neither builds, nor does the oracle.
	viewstest.Skip(s)
	if err := s.Write(context.Background(), func(tx *storage.WriteTx) error {
		return tx.Account(account).PutRecord(storage.RecordRow{Entity: "episode", EntityID: episode, FieldGroup: "favorite", Value: `[true]`, Provenance: `["t"]`})
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Read(context.Background(), func(r *storage.ReadTx) error {
		if _, err := views.BuildVersion(r.Account(account), account); err == nil {
			t.Error("a record that is not an object built")
		}
		if err := viewstest.PairedFailure(r, account, failure); err != nil {
			t.Errorf("an invalid library was not exempt: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// The observer checks each version's identity: a version with the right
// content and a stale head fails the commit.
func TestObserverChecksTheVersionsIdentity(t *testing.T) {
	s, account, episode := observedLibrary(t)
	viewstest.CorruptNextForTesting(s, account, func(base, v *views.Version) *views.Version {
		stale := *v
		stale.Head = base.Head
		stale.Head.Seq-- // as if the version were older
		return &stale
	})
	p, err := writeRecovering(func() error {
		return s.Write(context.Background(), func(tx *storage.WriteTx) error {
			a := tx.Account(account)
			if err := a.PutRecord(storage.RecordRow{Entity: "episode", EntityID: episode, FieldGroup: "favorite", Value: `{"on":true}`, Provenance: `["t"]`}); err != nil {
				return err
			}
			_, _, err := a.AppendCommit(storage.CommitBody{})
			return err
		})
	})
	if m, ok := p.(viewstest.Mismatch); !ok || !strings.Contains(m.Message, "has head") {
		t.Fatalf("a stale head: %v (%v)", p, err)
	}
}

// Commits that move the head, with a view change and without, are verified
// with the head the snapshot has.
func TestObserverVerifiesMovingHeads(t *testing.T) {
	s, account, episode := observedLibrary(t)
	commit := func(view bool) {
		t.Helper()
		before := viewstest.Report().Verified
		if err := s.Write(context.Background(), func(tx *storage.WriteTx) error {
			a := tx.Account(account)
			if view {
				if err := a.PutRecord(storage.RecordRow{Entity: "episode", EntityID: episode, FieldGroup: "favorite", Value: `{"on":true}`, Provenance: `["m"]`}); err != nil {
					return err
				}
			}
			_, _, err := a.AppendCommit(storage.CommitBody{})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if viewstest.Report().Verified <= before {
			t.Fatal("the commit was not verified")
		}
	}
	commit(true)  // an apply at a new head
	commit(false) // the same view at a newer head
}
