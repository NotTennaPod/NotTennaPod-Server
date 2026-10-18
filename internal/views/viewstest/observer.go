// Package viewstest holds the read model's test harness: the commit
// observer that checks the incremental model after every commit of every
// test (Main), and a generator of realistic libraries (Generate).
package viewstest

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/reference"
)

// The commit observer (storage.ObserveCommitsForTesting): per store and
// account it keeps the version the last commit left. Each commit advances
// that version by exactly the commit, in the snapshot the commit produced
// (an apply, an exact hit, or a full build after a lineage or generation
// change), and compares it with a full build in that snapshot. A difference
// panics in the writing goroutine, so the test that made the commit fails
// with its stack.

type cacheKey struct {
	store   *storage.Store
	account string
}

// Counts are what the observer verified.
type Counts struct {
	Commits     int64 // observations
	Accounts    int64 // account identities observed
	Verified    int64 // versions equal to their full build
	Paths       [4]int64
	Deleted     int64 // accounts a commit deleted
	Unbuildable int64 // libraries neither path can build (a record that is not an object)
	Skipped     int64 // observations of stores whose tests check on their own
	Corrupted   int64 // deliberately wrong advances found (CorruptNextForTesting)
}

var (
	mu        sync.Mutex
	installed bool
	cache     = map[cacheKey]*views.Version{}
	counts    Counts
	skipped   sync.Map // *storage.Store → true
	corrupt   *corruption
	probing   = map[*storage.Store]bool{} // RequireActive's stores
	probes    = map[cacheKey]bool{}       // their verified accounts
	pauses    = map[*storage.Store]func(){}
)

// FixedNow is the generated libraries' "now" (ms): a fixed instant, so a
// seed replays exactly.
const FixedNow = int64(1_781_524_800_000) // 2026-06-15T12:00:00Z

// PauseNextForTesting runs f at the start of the observer's next check of
// the store's commits, before it reads, while the store's write gate is held
// (the observer's own tests).
func PauseNextForTesting(s *storage.Store, f func()) {
	mu.Lock()
	defer mu.Unlock()
	pauses[s] = f
}

type corruption struct {
	at cacheKey
	f  func(base, v *views.Version) *views.Version
}

// maxCached bounds the versions kept (stores of finished tests stay
// reachable): past it the cache starts again, and the next commit of each
// account is a first build.
const maxCached = 512

// Install installs the observer for every store of the process.
func Install() {
	mu.Lock()
	defer mu.Unlock()
	if !installed {
		installed = true
		storage.ObserveCommitsForTesting(observe)
	}
}

// Installed reports whether the observer is installed.
func Installed() bool {
	mu.Lock()
	defer mu.Unlock()
	return installed
}

// Skip leaves a store's commits to its test's own checks (a test that
// compares every step itself, at scale).
func Skip(s *storage.Store) { skipped.Store(s, true) }

// Report returns the counts so far.
func Report() Counts {
	mu.Lock()
	defer mu.Unlock()
	return counts
}

// CorruptNextForTesting replaces the observer's next advance of the store's
// account by f(base, advanced): returning base is an omitted capture (the
// observer's own tests). f runs where the check does, holding the store's
// write gate.
func CorruptNextForTesting(s *storage.Store, account string, f func(base, v *views.Version) *views.Version) {
	mu.Lock()
	defer mu.Unlock()
	corrupt = &corruption{cacheKey{s, account}, f}
}

// RequireActive fails the test unless the observer is installed and checks
// commits: it commits to a new store and expects that commit verified.
func RequireActive(t *testing.T) {
	t.Helper()
	if !Installed() {
		t.Fatal("the read model's commit observer is not installed (viewstest.Main in the package's TestMain)")
	}
	ctx := context.Background()
	s, err := storage.Open(ctx, filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	mu.Lock()
	probing[s] = true
	mu.Unlock()
	defer func() {
		mu.Lock()
		delete(probing, s)
		mu.Unlock()
	}()
	var account string
	if err := s.Write(ctx, func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		if err != nil {
			return err
		}
		account = a.ID
		return tx.Account(a.ID).PutRecord(storage.RecordRow{Entity: "account", EntityID: a.ID, FieldGroup: "discovery_policy", Value: `{"new_episodes_action":"inbox"}`, Provenance: `["n"]`})
	}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	ok := probes[cacheKey{s, account}]
	delete(probes, cacheKey{s, account})
	mu.Unlock()
	if !ok {
		t.Fatalf("the commit was not verified (%+v)", Report())
	}
}

// Mismatch is the observer's panic value.
type Mismatch struct{ Message string }

func (m Mismatch) Error() string { return m.Message }

func observe(s *storage.Store, accounts map[string]storage.CommittedAccount) {
	if _, ok := skipped.Load(s); ok {
		mu.Lock()
		counts.Skipped++
		mu.Unlock()
		return
	}
	mu.Lock()
	pause := pauses[s]
	delete(pauses, s)
	mu.Unlock()
	if pause != nil {
		pause()
	}
	mu.Lock()
	defer mu.Unlock()
	counts.Commits++
	ids := make([]string, 0, len(accounts))
	for id := range accounts {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		want := accounts[id]
		counts.Accounts++
		k := cacheKey{s, id}
		if !want.Exists {
			delete(cache, k)
			counts.Deleted++
			continue
		}
		// The check reads on a connection of its own: the write gate is
		// held, and a pool reader may be waiting for this write.
		if err := s.VerifyReadForTesting(context.Background(), func(r *storage.ReadTx) error { return check(r, k, want) }); err != nil {
			panic(err)
		}
	}
	if len(cache) > maxCached {
		cache = map[cacheKey]*views.Version{}
	}
}

func check(r *storage.ReadTx, k cacheKey, want storage.CommittedAccount) error {
	acct := r.Account(k.account)
	head, err := acct.Head()
	if err != nil {
		return err
	}
	clock, err := acct.ViewClock()
	if err != nil {
		return err
	}
	if head.LogID != want.Head.LogID || head.Epoch != want.Head.Epoch || head.Seq != want.Head.Seq || !bytes.Equal(head.Digest, want.Head.Digest) ||
		clock != want.Clock {
		return Mismatch{fmt.Sprintf("read model: account %s: the snapshot after the commit is %v %+v, the commit made %v %+v",
			k.account, head, clock, want.Head, want.Clock)}
	}
	full, fullErr := views.BuildVersion(acct, k.account)
	v, path, err := views.Advance(cache[k], acct, k.account)
	if fullErr != nil {
		if err == nil {
			return Mismatch{fmt.Sprintf("read model: account %s: the %s built a version where the full build fails: %v", k.account, path, fullErr)}
		}
		if err := PairedFailure(r, k.account, fullErr); err != nil {
			return err
		}
		delete(cache, k)
		counts.Unbuildable++
		return nil
	}
	if err != nil {
		return Mismatch{fmt.Sprintf("read model: account %s: the %s failed where the full build succeeds: %v", k.account, path, err)}
	}
	corrupted := false
	if c := corrupt; c != nil && c.at == k {
		corrupt, corrupted = nil, true
		v = c.f(cache[k], v)
	}
	for _, x := range []struct {
		what string
		v    *views.Version
	}{{"full build", full}, {path.String(), v}} {
		if msg := IdentityMismatch(x.v, k.account, head, clock); msg != "" {
			if corrupted {
				counts.Corrupted++
			}
			return Mismatch{fmt.Sprintf("read model: account %s: the %s %s", k.account, x.what, msg)}
		}
	}
	if err := views.EqualVersions(v, full); err != nil {
		if corrupted {
			counts.Corrupted++
		}
		return Mismatch{fmt.Sprintf("read model: account %s at %v %+v: the %s differs from a full build: %v", k.account, head, clock, path, err)}
	}
	cache[k] = v
	if probing[k.store] {
		probes[k] = true
	}
	counts.Verified++
	counts.Paths[path]++
	return nil
}

// PairedFailure decides a snapshot where both the full build and the
// advance failed: exempt (nil) only when the oracle cannot load the library
// either (a record that is not an object); else a mismatch.
func PairedFailure(r *storage.ReadTx, account string, fullErr error) error {
	if _, refErr := reference.Load(r, account); refErr == nil {
		return Mismatch{fmt.Sprintf("read model: account %s: the full build fails where the oracle loads: %v", account, fullErr)}
	}
	return nil
}

// IdentityMismatch describes how a version's identity differs from the
// snapshot it was made in: its account, its full head (digest included) and
// its view clock; "" when it is the snapshot's.
func IdentityMismatch(v *views.Version, account string, head storage.Head, clock storage.ViewClock) string {
	h := v.Head
	switch {
	case v.Account != account:
		return fmt.Sprintf("is of account %s", v.Account)
	case h.LogID != head.LogID || h.Epoch != head.Epoch || h.Seq != head.Seq || !bytes.Equal(h.Digest, head.Digest):
		return fmt.Sprintf("has head %s/%d/%d/%x, the snapshot %s/%d/%d/%x", h.LogID, h.Epoch, h.Seq, h.Digest, head.LogID, head.Epoch, head.Seq, head.Digest)
	case v.Clock != clock:
		return fmt.Sprintf("has clock %+v, the snapshot %+v", v.Clock, clock)
	}
	return ""
}

// Main runs a package's tests with the observer installed, prints what it
// verified, and fails a run that verified no commit (unless it ran only
// some tests, or only listed them).
func Main(m *testing.M) int {
	Install()
	code := m.Run()
	c := Report()
	fmt.Printf("read model: %d commits observed; %d account versions verified (%d applied, %d exact, %d first builds, %d lineage or generation builds); %d deleted, %d unbuildable, %d skipped\n",
		c.Commits, c.Verified, c.Paths[views.PathApply], c.Paths[views.PathExact], c.Paths[views.PathFirst], c.Paths[views.PathGeneration],
		c.Deleted, c.Unbuildable, c.Skipped)
	if code == 0 && c.Accounts != c.Verified+c.Deleted+c.Unbuildable+c.Corrupted {
		fmt.Println("read model: an observed account identity was not verified")
		return 1
	}
	selected := func(name string) bool { f := flag.Lookup(name); return f != nil && f.Value.String() != "" }
	if code == 0 && !selected("test.run") && !selected("test.list") && c.Verified == 0 {
		fmt.Println("read model: the observer verified no commit")
		return 1
	}
	return code
}
