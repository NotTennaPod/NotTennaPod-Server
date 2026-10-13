package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
)

// An existing data directory and database with loose
// modes are made private, WAL and shared memory included.
func TestExistingDataFilesAreMadePrivate(t *testing.T) {
	d := t.TempDir()
	os.Chmod(d, 0o777)
	db := filepath.Join(d, databaseName)
	os.WriteFile(db, nil, 0o666)
	os.Chmod(db, 0o666)
	s, err := Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, p := range []string{d, db, db + "-wal", db + "-shm", filepath.Join(d, receiptKeysName)} {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s is %04o", filepath.Base(p), fi.Mode().Perm())
		}
	}
}

// A database that is a symlink is refused, and its target
// is not touched.
func TestDatabaseSymlinkIsRefused(t *testing.T) {
	d := t.TempDir()
	outside := filepath.Join(t.TempDir(), "victim.db")
	os.WriteFile(outside, nil, 0o666)
	os.Chmod(outside, 0o666)
	os.Symlink(outside, filepath.Join(d, databaseName))
	s, err := Open(context.Background(), d)
	if err == nil {
		s.Close()
		t.Fatal("a symlinked database was opened")
	}
	if !errors.Is(err, private.ErrSymlink) {
		t.Fatalf("got %v", err)
	}
	if fi, _ := os.Stat(outside); fi.Size() != 0 || fi.Mode().Perm() != 0o666 {
		t.Fatalf("the symlink's target changed: %d bytes, %04o", fi.Size(), fi.Mode().Perm())
	}
}

// The receipt and secrets keys are never read through
// symlinks, here to public files.
func TestSymlinkedKeyFilesAreRefused(t *testing.T) {
	for _, name := range []string{receiptKeysName, sealed.KeyFile} {
		t.Run(name, func(t *testing.T) {
			d := t.TempDir()
			s := openTest(t, config{dataDir: d})
			if _, err := sealed.LoadOrCreate(d); err != nil {
				t.Fatal(err)
			}
			s.Close()
			p := filepath.Join(d, name)
			b, _ := os.ReadFile(p)
			os.Remove(p)
			outside := filepath.Join(t.TempDir(), name)
			os.WriteFile(outside, b, 0o644)
			os.Chmod(outside, 0o644)
			os.Symlink(outside, p)
			if s, err := Open(context.Background(), d); !errors.Is(err, private.ErrSymlink) {
				if err == nil {
					s.Close()
				}
				t.Fatalf("the server started with %s a symlink: %v", name, err)
			}
			if name == sealed.KeyFile {
				if _, err := sealed.LoadOrCreate(d); !errors.Is(err, private.ErrSymlink) {
					t.Fatalf("the secrets key was read through a symlink: %v", err)
				}
			}
		})
	}
}

// A data directory reached through a symlink is allowed,
// and its target is made private.
func TestSymlinkedDataDirectoryIsMadePrivate(t *testing.T) {
	target := filepath.Join(t.TempDir(), "data")
	os.Mkdir(target, 0o777)
	os.Chmod(target, 0o777)
	link := filepath.Join(t.TempDir(), "link")
	os.Symlink(target, link)
	s, err := Open(context.Background(), link)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if fi, _ := os.Stat(target); fi.Mode().Perm() != 0o700 {
		t.Fatalf("the data directory is %04o", fi.Mode().Perm())
	}
	if b, err := os.ReadFile(filepath.Join(target, databaseName)); err != nil || !bytes.HasPrefix(b, []byte("SQLite format 3")) {
		t.Fatalf("the database is not in the symlink's target: %v", err)
	}
}

const keepTokensTrigger = `CREATE TRIGGER keep_tokens AFTER DELETE ON auth_tokens BEGIN INSERT INTO auth_tokens VALUES(OLD.token_hash, OLD.kind,
	OLD.account_id, OLD.install_id, OLD.created_at, OLD.last_used_at, OLD.expires_at); END`

// A backup changed to bring back revoked tokens (a trigger that puts deleted
// tokens back) is refused, unsigned by its MAC and, signed again, by the schema
// check; the revoked token stays dead.
func TestModifiedBackupCannotBringBackRevokedTokens(t *testing.T) {
	fx := newRestoreFixture(t)
	for _, tc := range []struct {
		name, want string
		backup     func(c *restoreFixture) string
	}{
		{"changed", "has been changed since it was made", func(c *restoreFixture) string {
			data, _ := os.ReadFile(c.backup)
			path := filepath.Join(t.TempDir(), "changed.db")
			os.WriteFile(path, data, 0o600)
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(keepTokensTrigger); err != nil {
				t.Fatal(err)
			}
			return path
		}},
		{"signed again", "trigger keep_tokens of its own", func(c *restoreFixture) string { return c.tamper(t, keepTokensTrigger) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fx.clone(t)
			c.backup = tc.backup(c)
			if _, err := c.restore(config{}); !errors.Is(err, ErrRestoreRefused) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want a refusal mentioning %q", err, tc.want)
			}
			s := openTest(t, config{dataDir: c.dir})
			if tokenWorks(t, s, c.tokens["alice"]) {
				t.Fatal("alice's token, revoked by her password reset, works again")
			}
		})
	}
}

// A changed backup put in place by hand (a detected restore, which has no key
// bundle) is refused at start by the schema check.
func TestDatabaseWithATriggerOfItsOwnDoesNotStart(t *testing.T) {
	fx := newRestoreFixture(t)
	c := fx.clone(t)
	data, _ := os.ReadFile(c.tamper(t, keepTokensTrigger))
	writeInPlace(t, c.dir, data)
	s, err := open(context.Background(), config{dataDir: c.dir, syncDir: noSync})
	if err == nil {
		s.Close()
		t.Fatal("the server started on a database with a trigger of its own")
	}
	if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), "trigger keep_tokens of its own") {
		t.Fatalf("got %v", err)
	}
}

// A trigger renamed through writable_schema to a statistics name (sqlite_stat…)
// is still a trigger of the database's own, and the database does not start;
// ANALYZE's own tables are still accepted.
func TestATriggerWithAStatisticsNameDoesNotStart(t *testing.T) {
	fx := newRestoreFixture(t)
	for _, tc := range []struct{ name, edit string }{
		{"trigger", `UPDATE sqlite_schema SET name='sqlite_stat_keep_tokens', sql=replace(sql,'TRIGGER keep_tokens','TRIGGER sqlite_stat_keep_tokens') WHERE type='trigger' AND name='keep_tokens'`},
		{"trigger named as the statistics table", `UPDATE sqlite_schema SET name='sqlite_stat1', sql=replace(sql,'TRIGGER keep_tokens','TRIGGER sqlite_stat1') WHERE type='trigger' AND name='keep_tokens'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fx.clone(t)
			path := filepath.Join(t.TempDir(), "edited.db")
			data, _ := os.ReadFile(c.backup)
			os.WriteFile(path, data, 0o600)
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			for _, q := range []string{keepTokensTrigger, "PRAGMA writable_schema=ON", tc.edit} {
				if _, err := db.Exec(q); err != nil {
					t.Fatal(err)
				}
			}
			db.Close()
			data, _ = os.ReadFile(path)
			writeInPlace(t, c.dir, data)
			s, err := open(context.Background(), config{dataDir: c.dir, syncDir: noSync})
			if err == nil {
				defer s.Close()
				t.Fatalf("the server started on a database with a renamed trigger; alice's revoked token works: %v", tokenWorks(t, s, c.tokens["alice"]))
			}
			if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), "of its own") {
				t.Fatalf("got %v", err)
			}
		})
	}
	t.Run("ANALYZE", func(t *testing.T) {
		c := fx.clone(t)
		path := filepath.Join(t.TempDir(), "analyzed.db")
		data, _ := os.ReadFile(c.backup)
		os.WriteFile(path, data, 0o600)
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("ANALYZE"); err != nil {
			t.Fatal(err)
		}
		db.Close()
		data, _ = os.ReadFile(path)
		writeInPlace(t, c.dir, data)
		s, err := open(context.Background(), config{dataDir: c.dir, syncDir: noSync})
		if err != nil {
			t.Fatalf("a database its owner ran ANALYZE on does not start: %v", err)
		}
		s.Close()
	})
}

// A backup is signed, and its signature covers every byte.
func TestBackupsAreSigned(t *testing.T) {
	d := t.TempDir()
	s := openTest(t, config{dataDir: d})
	createAccounts(t, s, 2)
	info, err := s.Backup(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f, tr, err := openSignedBackup(info.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	keyByID := map[string][]byte{s.receiptKeys[0].ID: s.receiptKeys[0].Secret}
	if err := checkBackupMAC(f, tr, keyByID); err != nil {
		t.Fatal(err)
	}
	if id, _ := s.ServerID(context.Background()); tr.ServerID != id || tr.KeyID != s.receiptKeys[0].ID || tr.Size >= info.Size {
		t.Fatalf("trailer %+v", tr)
	}
	data, _ := os.ReadFile(info.Path)
	for _, i := range []int{0, 100, int(tr.Size) - 1} {
		changed := bytes.Clone(data)
		changed[i] ^= 0x80
		if err := checkBackupMAC(bytes.NewReader(changed), tr, keyByID); err == nil {
			t.Fatalf("a change at byte %d verifies", i)
		}
	}
	if err := VerifyDatabaseFile(context.Background(), info.Path, -1); err != nil {
		t.Fatalf("the signed backup does not open as a database: %v", err)
	}
}

// The instance ID file is private, and a new start
// replaces it.
func TestInstanceIDFileIsPrivate(t *testing.T) {
	d := t.TempDir()
	s := openTest(t, config{dataDir: d})
	id, err := s.WriteInstance()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ReadInstance(d); err != nil || got != id || s.InstanceID() != id {
		t.Fatalf("read %q %v, want %q", got, err, id)
	}
	if fi, _ := os.Stat(filepath.Join(d, instanceName)); fi.Mode().Perm() != 0o600 {
		t.Fatalf("server.instance is %04o", fi.Mode().Perm())
	}
	if next, _ := NewInstance(d); next == id {
		t.Fatal("a new start kept the instance ID")
	}
	if err := ClearInstance(d); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadInstance(d); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("after clearing: %v", err)
	}
}

// A backup folder open to others is made private, and a
// backup file replaced between verification and publication is never
// published as a verified backup.
func TestReplacedBackupFileIsNotPublished(t *testing.T) {
	s := openTest(t, config{dataDir: t.TempDir()})
	dir := t.TempDir()
	os.Chmod(dir, 0o777)
	published := filepath.Join(dir, "published.db")
	_, err := s.BackupAs(context.Background(), dir, func(time.Time) (string, error) {
		temps, _ := filepath.Glob(filepath.Join(dir, backupTempPrefix+"*"))
		if len(temps) != 1 {
			t.Fatalf("temps %v", temps)
		}
		os.Remove(temps[0])
		if err := os.WriteFile(temps[0], []byte("attacker-controlled replacement"), 0o644); err != nil {
			t.Fatal(err)
		}
		return published, nil
	})
	if err == nil || !strings.Contains(err.Error(), "replaced") {
		t.Fatalf("got %v", err)
	}
	if _, err := os.Stat(published); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a replaced file was published: %v", err)
	}
	if fi, _ := os.Stat(dir); fi.Mode().Perm() != 0o700 {
		t.Fatalf("the backup folder is %04o", fi.Mode().Perm())
	}
	if _, err := s.Backup(context.Background(), dir); err != nil {
		t.Fatalf("the next backup: %v", err)
	}
}

// Behind the backup MAC, a restore checks the stored history
// against what was signed. Each change here is signed again with the
// server's key, so only these checks can refuse it.
func TestRestoreVerifiesTheSignedHistory(t *testing.T) {
	fx := newRestoreFixture(t)
	_, ks, _, err := readReceiptKeysFile(fx.dir)
	if err != nil {
		t.Fatal(err)
	}
	alice := fx.ids["alice"]
	superseded := func(mac bool, epoch, seq, index int64) string {
		var hash string
		db, _ := openReadOnly(fx.backup)
		db.QueryRow(`SELECT content_hash FROM facts WHERE op_id = 'op-1'`).Scan(&hash)
		db.Close()
		receipt := signed(t, ks[0], alice, map[string]any{"op_id": "op-1", "content_hash": hash, "outcome": "applied",
			"normalized_time": int64(4), "epoch": int64(1), "seq": int64(1), "index": int64(0)})
		if !mac {
			receipt = []byte(`{"key_id":"k1","mac":"invalid","outcome":"applied","normalized_time":999}`)
		}
		return fmt.Sprintf(`INSERT INTO superseded_receipts (account_id, op_id, epoch, seq, receipt_index, receipt, replaced_at)
			VALUES ('%s', 'op-1', %d, %d, %d, X'%x', 0)`, alice, epoch, seq, index, receipt)
	}
	for _, tc := range []struct{ name, sql, want string }{
		{"an invalid superseded receipt", superseded(false, 9, 9, 0), "superseded receipt of op-1"},
		{"a superseded receipt at another position", superseded(true, 1, 7, 0), "does not match its fact or its stored position"},
		{"a fact moved from its signed position", `UPDATE facts SET epoch = 42, seq = 123, receipt_index = 9`, "stored at a position"},
		{"a decision re-ranked", `UPDATE decisions SET epoch = 42, seq = 123`, "rank, subject or time"},
		{"a decision with another subject", `UPDATE decisions SET subject = 'unsigned replacement'`, "rank, subject or time"},
		{"a decision with another time", `UPDATE decisions SET normalized_time = 999`, "rank, subject or time"},
		{"a rewritten log commit", `UPDATE log_commits SET commit_json = CAST('{"accepted":[],"decisions":[],"epoch":1,"records":[],"seq":1,"slots":[]}' AS BLOB)
			WHERE seq = 1`, "log of account"},
		{"a log commit at another epoch", `UPDATE log_commits SET epoch = 7 WHERE seq = 1`, "log of account"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fx.clone(t)
			c.backup = c.tamper(t, tc.sql)
			before := snapshotDir(t, c.dir)
			if _, err := c.restore(config{}); !errors.Is(err, ErrRestoreRefused) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want a refusal mentioning %q", err, tc.want)
			}
			if after := snapshotDir(t, c.dir); !reflect.DeepEqual(after, before) {
				t.Fatal("the refusal changed the data directory")
			}
		})
	}
	t.Run("a valid superseded receipt", func(t *testing.T) {
		c := fx.clone(t)
		c.backup = c.tamper(t, superseded(true, 1, 1, 0))
		if _, err := c.restore(config{}); err != nil {
			t.Fatalf("a signed superseded receipt at its own position was refused: %v", err)
		}
	})
}

// A start refuses a server file with a name outside the data directory (here a
// backup made readable through it), naming the file, and makes loose files
// private.
func TestStartRefusesFilesWithAnotherName(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	s, err := Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Backup(ctx, filepath.Join(d, "backups"))
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	os.Chmod(info.Path, 0o644)
	alias := filepath.Join(t.TempDir(), "backup-alias")
	if err := os.Link(info.Path, alias); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(ctx, d); !errors.Is(err, private.ErrLinked) || !strings.Contains(err.Error(), filepath.Base(info.Path)) {
		if err == nil {
			s.Close()
		}
		t.Fatalf("a backup with another name: %v", err)
	}
	os.Remove(alias)
	s, err = Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if fi, _ := os.Stat(info.Path); fi.Mode().Perm() != 0o600 {
		t.Fatalf("the loose backup is %04o", fi.Mode().Perm())
	}
}

// A start refuses a server folder that
// is a symlink, and writes nothing through it.
func TestStartRefusesASymlinkedServerFolder(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	s, err := Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	outside := t.TempDir()
	os.Symlink(outside, filepath.Join(d, "views"))
	if s, err := Open(ctx, d); !errors.Is(err, private.ErrSymlink) {
		if err == nil {
			s.Close()
		}
		t.Fatalf("a symlinked server folder: %v", err)
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 0 {
		t.Fatalf("the outside folder was written: %v", entries)
	}
}
