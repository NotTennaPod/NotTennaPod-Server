// Package acceptancecheck is the acceptance witness (PROTOCOL §16). After an acceptance
// run, on a copy of the stopped server's data directory, it checks the
// database's integrity; authenticates the archive with the directory's own
// keys, as a restore checks a backup (every fact against its signed receipt,
// every receipt's and decision's MAC, and every sealed value); verifies every
// account's log digest chain; and runs the rebuild invariant (PROTOCOL §6.11,
// rebuildtest.Check) for every account: rebuilding the account from its
// archive reproduces its published records and its derived state. A client
// can look right while the server's derived state has drifted; the witness
// catches that before a person meets it weeks later. It says nothing about a
// phone's files or Keychain.
//
// Only `make acceptance-witness DATA=<dir>` runs it (TestWitness skips
// without NTP_WITNESS_DATA); the package's own tests run with the others.
package acceptancecheck

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/rebuildtest"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The checks, as problems name them.
const (
	CheckOpen      = "open"      // copying and opening the data directory (quick check, schema, every account's head)
	CheckIntegrity = "integrity" // SQLite's full integrity and foreign key checks
	CheckArchive   = "archive"   // facts against their signed receipts, receipts' and decisions' MACs, and the history beside them
	CheckSecrets   = "secrets"   // every sealed value opens with the data directory's secrets key
	CheckChain     = "chain"     // an account's log digest chain
	CheckRebuild   = "rebuild"   // an account's rebuild invariant
)

// Problem is one failed check, with the first divergence it met.
type Problem struct {
	Account  string // "" for the data directory as a whole
	Username string
	Check    string
	Detail   string
}

func (p Problem) String() string {
	where := "the data directory"
	if p.Account != "" {
		where = "account " + p.Account
		if p.Username != "" {
			where += " (" + p.Username + ")"
		}
	}
	return where + ": " + p.Check + ": " + p.Detail
}

// Account is one account the witness checked.
type Account struct {
	ID, Username string
	Commits      int64    // the log's head
	Log          []string // what the rebuild invariant reported when it held
}

// Result is what the witness found.
type Result struct {
	Accounts []Account
	Problems []Problem
}

// Verify runs the witness on dataDir, a stopped server's data directory,
// which it copies first while holding its existing lock (storage.OpenCopy):
// the copy is checked, rebuilt and removed. Nothing in dataDir is created,
// written or re-permissioned; a directory without its lock, or whose
// database is not a server's, is refused. It returns the problems instead of
// failing t, which it uses for its temporary folder and as the test the
// rebuild invariant reports to (its failures are collected per account).
func Verify(t testing.TB, dataDir string) Result {
	t.Helper()
	ctx := context.Background()
	var res Result
	store, err := storage.OpenCopy(ctx, dataDir, filepath.Join(t.TempDir(), "copy"))
	if err != nil {
		res.Problems = append(res.Problems, Problem{Check: CheckOpen, Detail: err.Error()})
		return res
	}
	defer store.Close()
	var ids []string
	names := map[string]string{}
	heads := map[string]int64{}
	err = store.Read(ctx, func(r *storage.ReadTx) error {
		var err error
		if ids, err = r.AccountIDs(); err != nil {
			return err
		}
		logins, err := r.ListAccounts()
		if err != nil {
			return err
		}
		for _, l := range logins {
			names[l.AccountID] = l.Username
		}
		for _, id := range ids {
			h, err := r.Account(id).Head()
			if err != nil {
				return fmt.Errorf("account %s: %w", id, err)
			}
			heads[id] = h.Seq
		}
		return nil
	})
	if err != nil {
		res.Problems = append(res.Problems, Problem{Check: CheckOpen, Detail: err.Error()})
		return res
	}
	// Server-wide checks; a failure names its account, to which it is put.
	serverWide := func(check string, err error) {
		if err == nil {
			return
		}
		p := Problem{Check: check, Detail: err.Error()}
		for _, id := range ids {
			if strings.Contains(p.Detail, id) {
				p.Account, p.Username = id, names[id]
				break
			}
		}
		res.Problems = append(res.Problems, p)
	}
	serverWide(CheckIntegrity, store.CheckIntegrity(ctx))
	serverWide(CheckArchive, store.VerifyArchive(ctx))
	serverWide(CheckSecrets, store.VerifySealed(ctx))
	box, err := secretsBox(store.Dir())
	if err != nil {
		res.Problems = append(res.Problems, Problem{Check: CheckOpen, Detail: err.Error()})
		return res
	}
	registry := protocol.NewRegistry()
	ops.Register(registry)
	svc := &protocol.Service{Store: store, Registry: registry, Now: time.Now, Box: box}
	for _, id := range ids {
		acct := Account{ID: id, Username: names[id], Commits: heads[id]}
		problem := func(check, detail string) {
			res.Problems = append(res.Problems, Problem{Account: id, Username: acct.Username, Check: check, Detail: detail})
		}
		if err := store.VerifyChain(ctx, id); err != nil {
			problem(CheckChain, err.Error())
		}
		errs, logs := rebuild(t, svc, id)
		for _, e := range errs {
			problem(CheckRebuild, strings.TrimPrefix(e, "rebuild invariant: "))
		}
		if len(errs) == 0 {
			acct.Log = logs
		}
		res.Accounts = append(res.Accounts, acct)
	}
	return res
}

// Check is Verify reported through t: every problem fails it, naming its
// account and the first divergence; every account that holds is logged.
func Check(t testing.TB, dataDir string) {
	t.Helper()
	res := Verify(t, dataDir)
	failed := map[string]bool{}
	for _, p := range res.Problems {
		failed[p.Account] = true
		t.Errorf("witness: %s", p)
	}
	for _, a := range res.Accounts {
		if !failed[a.ID] {
			t.Logf("witness: account %s (%s): %d commits, chain verified; %s", a.ID, a.Username, a.Commits, strings.Join(a.Log, "; "))
		}
	}
	if !failed[""] {
		t.Logf("witness: %s: integrity verified, %d accounts", dataDir, len(res.Accounts))
	}
}

// secretsBox is the copy's secrets key, never made: without one (nothing is
// sealed, or the secrets check failed), a key of its own for the rebuild,
// which seals nothing it keeps.
func secretsBox(dir string) (*sealed.Box, error) {
	if box, err := sealed.Load(dir); err == nil || !errors.Is(err, os.ErrNotExist) {
		return box, err
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return sealed.New(key)
}

// rebuild runs the rebuild invariant on one account and returns what it
// reported: its failures, and its log when it held.
func rebuild(t testing.TB, svc *protocol.Service, account string) (errs, logs []string) {
	rec := &recorder{TB: t}
	func() {
		defer func() {
			if r := recover(); r != nil && r != errStop {
				rec.errs = append(rec.errs, fmt.Sprintf("the rebuild panicked: %v\n%s", r, debug.Stack()))
			}
		}()
		rebuildtest.Check(rec, svc, account, nil)
	}()
	return rec.errs, rec.logs
}

// errStop ends a check that called FailNow on the recorder.
var errStop = &struct{ string }{"stop"}

// recorder is the test rebuildtest.Check reports to for one account: it
// collects the failures and the log instead of failing the witness's own
// test, and starts unfailed, so every account is checked.
type recorder struct {
	testing.TB
	errs, logs []string
}

func (r *recorder) Helper()       {}
func (r *recorder) Failed() bool  { return len(r.errs) > 0 }
func (r *recorder) Skipped() bool { return false }
func (r *recorder) Error(args ...any) {
	r.errs = append(r.errs, strings.TrimSuffix(fmt.Sprintln(args...), "\n"))
}
func (r *recorder) Errorf(format string, a ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, a...))
}
func (r *recorder) Log(args ...any) {
	r.logs = append(r.logs, strings.TrimSuffix(fmt.Sprintln(args...), "\n"))
}
func (r *recorder) Logf(format string, a ...any)   { r.logs = append(r.logs, fmt.Sprintf(format, a...)) }
func (r *recorder) Fail()                          { r.errs = append(r.errs, "failed") }
func (r *recorder) FailNow()                       { r.Fail(); panic(errStop) }
func (r *recorder) Fatal(args ...any)              { r.Error(args...); panic(errStop) }
func (r *recorder) Fatalf(format string, a ...any) { r.Errorf(format, a...); panic(errStop) }
