package storage

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
)

// Every account-scoped table is either copied by a publication or explicitly
// excluded (PROTOCOL §11.5): a new table must be classified before imports can miss
// or overwrite it.
func TestImportTablesCoverEveryAccountTable(t *testing.T) {
	s := openTest(t, config{dataDir: t.TempDir()})
	rows, err := s.reader.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, n)
	}
	rows.Close()
	for _, table := range tables {
		var has int
		if err := s.reader.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = 'account_id'`, table).Scan(&has); err != nil {
			t.Fatal(err)
		}
		in, out := slices.Contains(importTables, table), slices.Contains(importExcluded, table)
		switch {
		case has == 0 && (in || out):
			t.Errorf("%s has no account_id but is classified", table)
		case has == 1 && in == out:
			t.Errorf("account table %s must be in exactly one of importTables and importExcluded", table)
		}
	}
}

func reserve(t *testing.T, s *Store, account, id, fingerprint string) (Import, bool, error) {
	t.Helper()
	var im Import
	var created bool
	err := s.Write(context.Background(), func(tx *WriteTx) error {
		var err error
		im, created, err = tx.Account(account).ReserveImport(Import{ImportID: id, Kind: "android_db", Fingerprint: fingerprint, Size: 10, FileName: "a.db"})
		return err
	})
	return im, created, err
}

// Reservation: one live import per empty account; the same file returns it;
// a failure releases the account; a library refuses an import.
func TestReserveImport(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, s, 2)
	if _, created, err := reserve(t, s, ids[0], "11111111-1111-4111-8111-111111111111", "aa"); err != nil || !created {
		t.Fatalf("first reservation: %v %v", created, err)
	}
	if im, created, err := reserve(t, s, ids[0], "22222222-2222-4222-8222-222222222222", "aa"); err != nil || created || im.ImportID != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("the same file: %+v %v %v", im, created, err)
	}
	if _, _, err := reserve(t, s, ids[0], "33333333-3333-4333-8333-333333333333", "bb"); !errors.Is(err, ErrImportExists) {
		t.Fatalf("another file: %v", err)
	}
	if err := s.Write(ctx, func(tx *WriteTx) error {
		return tx.Account(ids[0]).SetImportStatus("11111111-1111-4111-8111-111111111111", "failed", "unreadable")
	}); err != nil {
		t.Fatal(err)
	}
	if _, created, err := reserve(t, s, ids[0], "33333333-3333-4333-8333-333333333333", "bb"); err != nil || !created {
		t.Fatalf("after a failure: %v %v", created, err)
	}
	// An account with a library refuses an import.
	if err := s.Write(ctx, func(tx *WriteTx) error {
		a := tx.Account(ids[1])
		if err := a.InsertNode("44444444-4444-4444-8444-444444444444", "decision", 1, nil); err != nil {
			return err
		}
		return a.InsertDecision(DecisionRow{DecisionID: "44444444-4444-4444-8444-444444444444", DecisionType: "x", Subject: "s", Epoch: 1, Seq: 1,
			NormalizedTime: 1, Decision: []byte("{}")})
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := reserve(t, s, ids[1], "55555555-5555-4555-8555-555555555555", "cc"); !errors.Is(err, ErrAccountNotEmpty) {
		t.Fatalf("an account with a library: %v", err)
	}
	// A restore transition fails unfinished imports.
	if err := s.Write(ctx, func(tx *WriteTx) error { return tx.FailUnfinishedImports() }); err != nil {
		t.Fatal(err)
	}
	if left, err := s.UnfinishedImports(ctx); err != nil || len(left) != 0 {
		t.Fatalf("unfinished after the transition: %v %v", left, err)
	}
}

// Publication copies the staged library and moves the head in one
// transaction, only from the head staging began at.
func TestPublishImport(t *testing.T) {
	ctx := context.Background()
	live := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, live, 1)
	account := ids[0]
	const importID = "66666666-6666-4666-8666-666666666666"
	stagingPath, base, stagedHead := stageImport(t, live, account, importID)
	if err := live.PublishImport(ctx, stagingPath, account, importID, base, `{"imported":{}}`); err != nil {
		t.Fatal(err)
	}
	if err := live.Read(ctx, func(r *ReadTx) error {
		h, err := r.Account(account).Head()
		if err != nil {
			return err
		}
		if h.Seq != stagedHead.Seq || !bytes.Equal(h.Digest, stagedHead.Digest) || h.LogID != base.LogID {
			t.Errorf("head after publication %+v, staged %+v", h, stagedHead)
		}
		im, err := r.Account(account).Import(importID)
		if err != nil || im.Status != "published" {
			t.Errorf("import after publication %+v %v", im, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := live.VerifyChain(ctx, account); err != nil {
		t.Fatalf("the published log does not verify: %v", err)
	}
	// Publishing again, or from a stale head, changes nothing.
	if err := live.PublishImport(ctx, stagingPath, account, importID, base, "{}"); !errors.Is(err, ErrImportChanged) {
		t.Fatalf("a second publication: %v", err)
	}
	var podcasts int
	if err := live.reader.QueryRow(`SELECT COUNT(*) FROM podcasts WHERE account_id = ?`, account).Scan(&podcasts); err != nil || podcasts != 1 {
		t.Fatalf("%d podcasts after a refused second publication (%v)", podcasts, err)
	}
}

// stageImport reserves an import into account and stages a one-podcast
// library for it, as the import runner does.
func stageImport(t *testing.T, live *Store, account, importID string) (stagingPath string, base, stagedHead Head) {
	t.Helper()
	ctx := context.Background()
	if _, _, err := reserve(t, live, account, importID, "ff"); err != nil {
		t.Fatal(err)
	}
	if err := live.Write(ctx, func(tx *WriteTx) error { return tx.Account(account).SetImportStatus(importID, "processing", "") }); err != nil {
		t.Fatal(err)
	}
	var namespace string
	if err := live.Write(ctx, func(tx *WriteTx) error {
		var err error
		if base, err = tx.Account(account).Head(); err != nil {
			return err
		}
		namespace, err = tx.Account(account).Namespace()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	stagingDir := t.TempDir()
	staging := openTest(t, config{dataDir: stagingDir})
	if err := staging.Write(ctx, func(tx *WriteTx) error {
		if err := tx.MirrorAccount(account, namespace, base); err != nil {
			return err
		}
		a := tx.Account(account)
		if err := a.InsertNode("77777777-7777-4777-8777-777777777777", "decision", 1, nil); err != nil {
			return err
		}
		if err := a.CreatePodcast("p1:aa", "77777777-7777-4777-8777-777777777777"); err != nil {
			return err
		}
		if _, _, err := a.AppendCommit(record(account, 1)); err != nil {
			return err
		}
		var err error
		stagedHead, err = a.Head()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := staging.Close(); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(stagingDir, databaseName), base, stagedHead
}
