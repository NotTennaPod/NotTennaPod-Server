package storage

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The store records times by the server's clock (WithClock, PROTOCOL §16): with the clock three
// days ahead, an account's and a commit's times, a backup's time and name are the moved
// clock's, and a staged copy (a rebuild's or an import's) keeps the clock.
func TestTheStoreRecordsByItsClock(t *testing.T) {
	ctx := context.Background()
	at := time.Now().Add(72 * time.Hour).Truncate(time.Second)
	s, err := Open(ctx, t.TempDir(), WithClock(func() time.Time { return at }))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ids := createAccounts(t, s, 1)
	if err := s.Write(ctx, func(tx *WriteTx) error {
		_, _, err := tx.Account(ids[0]).AppendCommit(record(ids[0], 1))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var created, committed int64
	if err := s.Read(ctx, func(r *ReadTx) error {
		if err := r.tx.QueryRowContext(ctx, `SELECT created_at FROM accounts WHERE account_id = ?`, ids[0]).Scan(&created); err != nil {
			return err
		}
		return r.tx.QueryRowContext(ctx, `SELECT committed_at FROM log_commits WHERE account_id = ? AND seq = 1`, ids[0]).Scan(&committed)
	}); err != nil {
		t.Fatal(err)
	}
	if created != at.UnixMilli() || committed != at.UnixMilli() {
		t.Errorf("account created at %d, commit at %d, want the clock's %d", created, committed, at.UnixMilli())
	}
	info, err := s.Backup(ctx, filepath.Join(t.TempDir(), "backups"))
	if err != nil {
		t.Fatal(err)
	}
	if info.TakenAt != at.UnixMilli() || !strings.Contains(filepath.Base(info.Path), at.UTC().Format("20060102-150405")) {
		t.Errorf("backup %+v, want taken at %v", info, at)
	}
	staged, err := s.StageCopy(ctx, filepath.Join(t.TempDir(), "staged"))
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Close()
	if !staged.Now().Equal(at) {
		t.Errorf("the staged copy's clock reads %v, want %v", staged.Now(), at)
	}
	// Without a clock, the system's.
	plain := openTest(t, config{dataDir: t.TempDir()})
	if d := time.Since(plain.Now()); d < 0 || d > time.Minute {
		t.Errorf("the default clock is %v off the system's", d)
	}
}
