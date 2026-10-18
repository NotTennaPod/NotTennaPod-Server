package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// PROTOCOL §11.4: a restore never takes the data folder's last gigabyte, its
// copies and SQLite's own writes (migrations, the transition, checkpoints)
// included; when space runs short it refuses, or stops and keeps the
// database it would replace, and finishes exactly once space is freed.

// newBlobFixture is a stopped server whose backup was taken at schema
// version, holding n stored feed responses of size random bytes (and, before
// migration 0021, identity rows that migration copies); the server itself has
// since been upgraded to the latest schema.
func newBlobFixture(t *testing.T, version, n, size int) *restoreFixture {
	t.Helper()
	ctx := context.Background()
	fx := &restoreFixture{dir: t.TempDir(), pass: []byte(fixturePass), ids: map[string]string{}, tokens: map[string][]byte{}}
	s := openTest(t, config{dataDir: fx.dir, migrations: embeddedMigrations[:version]})
	randomBlobs(t, s, n, size)
	if version < 21 {
		acct := createAccounts(t, s, 1)[0]
		if err := s.Write(ctx, func(w *WriteTx) error {
			for i := range 4000 {
				if _, err := w.tx.ExecContext(ctx, `INSERT INTO identity_aliases (account_id, podcast_id, kind, value, episode_id, node_id)
					VALUES (?, 'p', 'guid', ?, ?, 'n')`, acct, fmt.Sprintf("guid-%06d-%s", i, strings.Repeat("g", 40)), fmt.Sprintf("e-%06d", i)); err != nil {
					return err
				}
				if _, err := w.tx.ExecContext(ctx, `INSERT INTO identity_releases (account_id, podcast_id, guid, episode_id, pub_date, first_enclosure,
					node_id, enclosures, original) VALUES (?, 'p', ?, ?, 1, ?, 'n', '[]', 1)`, acct, fmt.Sprintf("guid-%06d", i), fmt.Sprintf("e-%06d", i),
					"https://cdn.example.invalid/"+strings.Repeat("a", 60)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	info, err := s.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fx.backup = info.Path
	keys := filepath.Join(t.TempDir(), "keys.json")
	if err := s.ExportKeys(ctx, keys, fx.pass); err != nil {
		t.Fatal(err)
	}
	if fx.bundle, err = os.ReadFile(keys); err != nil {
		t.Fatal(err)
	}
	s.Close()
	openTest(t, config{dataDir: fx.dir}).Close() // the upgraded server
	return fx
}

// disk is a simulated file system for a data folder: its free space is a
// budget less what the restore has written there (its staged files and
// temporaries) and what another program takes. It records the most the
// restore had written at once.
type disk struct {
	dir    string
	budget atomic.Int64
	other  atomic.Int64
	peak   atomic.Int64
	// fill, when set, is called with what the restore has written and may
	// make another program take space (other).
	fill func(d *disk)
}

func (d *disk) used() int64 {
	var n int64
	entries, _ := os.ReadDir(d.dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), stagedName) || strings.HasPrefix(e.Name(), restoreTempPrefix) {
			if fi, err := e.Info(); err == nil {
				n += fi.Size()
			}
		}
	}
	return n
}

func (d *disk) free(string) (int64, error) {
	if d.fill != nil {
		d.fill(d)
	}
	u := d.used()
	if u > d.peak.Load() {
		d.peak.Store(u)
	}
	return d.budget.Load() - u - d.other.Load(), nil
}

// sample records the most the restore writes at once until stop is called.
func (d *disk) sample() (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			d.free("")
			time.Sleep(100 * time.Microsecond)
		}
	}()
	return func() { close(done); wg.Wait() }
}

func fileSHA(t *testing.T, path string) string {
	t.Helper()
	s, err := fileDigest(path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// A backup taken before a migration restores into the upgraded server: the
// staged copy is migrated and every stored response is kept exactly.
func TestRestoreOldSchemaBackupMigrates(t *testing.T) {
	fx := newBlobFixture(t, 23, 3, 64<<10)
	forEachFormat(t, func(t *testing.T, format int) {
		c := fx.clone(t)
		c.backup = asFormat(t, fx.backup, fx.dir, format)
		want := plainCopy(t, c.backup)
		if v, _ := schemaVersionOf(context.Background(), rawSQL(t, want)); v != 23 {
			t.Fatalf("the backup is at schema %d", v)
		}
		if _, err := c.restore(config{}); err != nil {
			t.Fatal(err)
		}
		s := openTest(t, config{dataDir: c.dir})
		if v, _ := s.SchemaVersion(context.Background()); v != latestSchema() {
			t.Fatalf("schema %d after the restore", v)
		}
		got := dump(t, filepath.Join(c.dir, databaseName))["feed_responses"]
		before := dump(t, want)["feed_responses"]
		if len(got) != 3 || len(before) != 3 {
			t.Fatalf("%d responses restored, %d in the backup", len(got), len(before))
		}
		for i := range got {
			for _, col := range []string{"sealed=", "stored_at=", "podcast_id="} {
				if field(got[i], col) != field(before[i], col) {
					t.Fatalf("response %d: %s differs", i, col)
				}
			}
		}
		assertSettled(t, s)
	})
}

// field is one column of a dump row.
func field(row, col string) string {
	for _, f := range strings.Split(row, " ") {
		if strings.HasPrefix(f, col) {
			return f
		}
	}
	return ""
}

// Free space is checked before the backup is copied into the data folder,
// against the backup's own database size after its MAC verifies, and while
// it is copied and decompressed: a refusal at verification changes nothing;
// one at staging keeps the record and the database it would replace, takes
// back what it wrote, and the restore finishes exactly once space is freed.
func TestRestoreSpace(t *testing.T) {
	fx := newBlobFixture(t, len(embeddedMigrations), 3, 512<<10)
	ref := fx.clone(t)
	if _, err := ref.restore(config{}); err != nil {
		t.Fatal(err)
	}
	refDump := dump(t, filepath.Join(ref.dir, databaseName))
	forEachFormat(t, func(t *testing.T, format int) {
		smallChunks(t, 64<<10)
		backup := asFormat(t, fx.backup, fx.dir, format)
		fi, _ := os.Stat(backup)
		size, dbSize := fi.Size(), int64(len(rawDatabaseOf(t, backup)))
		run := func(c *restoreFixture, d *disk, h hooks) error {
			_, err := c.restore(config{free: d.free, hooks: h})
			return err
		}
		refusedUnchanged := func(t *testing.T, c *restoreFixture, d *disk, want string) {
			t.Helper()
			before := snapshotDir(t, c.dir)
			decoded := BackupsDecoded()
			err := run(c, d, hooks{})
			if !errors.Is(err, ErrRestoreRefused) || !errors.Is(err, ErrNoSpace) || !strings.Contains(err.Error(), want) {
				t.Fatalf("got %v, want a refusal for space mentioning %q", err, want)
			}
			if after := snapshotDir(t, c.dir); !reflect.DeepEqual(after, before) {
				t.Fatalf("the refusal changed the data directory:\n%v\n%v", before, after)
			}
			if format == 2 && strings.Contains(want, "database") && BackupsDecoded() != decoded {
				t.Fatal("decoded although the database would not fit")
			}
		}
		t.Run("refused before the copy", func(t *testing.T) {
			c := fx.clone(t)
			c.backup = backup
			d := &disk{dir: c.dir}
			d.budget.Store(restoreReserve + size - 1)
			want := map[int]string{1: "restore this backup", 2: "check the backup"}[format]
			refusedUnchanged(t, c, d, want)
		})
		if format == 2 {
			t.Run("refused by the backup's own database size", func(t *testing.T) {
				c := fx.clone(t)
				c.backup = backup
				d := &disk{dir: c.dir}
				d.budget.Store(restoreReserve + size + dbSize - 1)
				refusedUnchanged(t, c, d, "check the backup's database")
			})
		}
		// Another program takes the space once the restore has started
		// writing a database: format 2's decoding, at verification or staging.
		fillWhen := func(name string) func(d *disk) {
			return func(d *disk) {
				matches, _ := filepath.Glob(filepath.Join(d.dir, name))
				for _, m := range matches {
					if fi, err := os.Stat(m); err == nil && fi.Size() > 0 && fi.Size() < dbSize {
						d.other.Store(d.budget.Load())
					}
				}
			}
		}
		if format == 2 {
			t.Run("the disk fills while the backup is checked", func(t *testing.T) {
				c := fx.clone(t)
				c.backup = backup
				d := &disk{dir: c.dir, fill: fillWhen(restoreTempPrefix + "verify-*.db")}
				d.budget.Store(100 << 30)
				refusedUnchanged(t, c, d, "the restore was stopped")
			})
		}
		t.Run("the disk fills while the backup is staged", func(t *testing.T) {
			c := fx.clone(t)
			c.backup = backup
			live := fileSHA(t, filepath.Join(c.dir, databaseName))
			d := &disk{dir: c.dir}
			d.budget.Store(100 << 30)
			// A restore stopped once recorded is resumed with the disk filling
			// while it stages: a copy for version 1, a decoding for version 2.
			crash := errors.New("crash")
			if err := run(c, d, hooks{restoreFault: func(p string) error {
				if p == "staged-source-copied" {
					return crash
				}
				return nil
			}}); !errors.Is(err, crash) {
				t.Fatal(err)
			}
			d.fill = fillWhen(map[int]string{1: stagedSourceName, 2: stagedName}[format])
			err := run(c, d, hooks{})
			if !errors.Is(err, ErrRestoreRefused) || !errors.Is(err, ErrNoSpace) {
				t.Fatalf("got %v", err)
			}
			if st := readState(t, c.dir); st.Restore == nil || st.Restore.Step != stepStage {
				t.Fatalf("the record is %+v", st.Restore)
			}
			if got := fileSHA(t, filepath.Join(c.dir, databaseName)); got != live {
				t.Fatal("the database the restore would replace changed")
			}
			if left := restoreLeftovers(t, c.dir); len(left) > 0 {
				t.Fatalf("left %v", left)
			}
			// Space is freed: the restore finishes, exactly.
			d.fill = nil
			d.other.Store(0)
			if _, err := c.restore(config{free: d.free}); err != nil {
				t.Fatal(err)
			}
			if got := dump(t, filepath.Join(c.dir, databaseName)); !reflect.DeepEqual(got, refDump) {
				t.Fatal("the restore finished after space was freed differs from one that had space")
			}
		})
	})
}

// SQLite's own writes on the staged copy (a migration that copies a
// table, here 0024 copying every stored response, then the checkpoint that
// writes the copied pages into the file while the log still holds them) are
// allowed for before they start, so they never take the reserve themselves.
// Another program taking the space meanwhile is detected late, during the
// migration or after the checkpoint: the restore stops with the database it
// would replace kept, and finishes exactly once space is freed. Both
// formats, on a populated database from before the migration.
func TestRestoreMigrationSpace(t *testing.T) {
	fx := newBlobFixture(t, 23, 8, 1<<20)
	ref := fx.clone(t)
	if _, err := ref.restore(config{}); err != nil {
		t.Fatal(err)
	}
	refDump := dump(t, filepath.Join(ref.dir, databaseName))
	same := func(t *testing.T, c *restoreFixture) {
		t.Helper()
		if got := dump(t, filepath.Join(c.dir, databaseName)); !reflect.DeepEqual(got, refDump) {
			t.Fatal("the restore finished after space was freed differs from one that had space")
		}
		s := openTest(t, config{dataDir: c.dir})
		if v, _ := s.SchemaVersion(context.Background()); v != latestSchema() {
			t.Fatalf("schema %d", v)
		}
		assertSettled(t, s)
	}
	stopped := func(t *testing.T, c *restoreFixture, err error, live string) {
		t.Helper()
		if !errors.Is(err, ErrRestoreRefused) || !errors.Is(err, ErrNoSpace) {
			t.Fatalf("got %v, want a stop for space", err)
		}
		if st := readState(t, c.dir); st.Restore == nil || st.Restore.Step != stepStage {
			t.Fatalf("the record is %+v", st.Restore)
		}
		if got := fileSHA(t, filepath.Join(c.dir, databaseName)); got != live {
			t.Fatal("the database the restore would replace changed")
		}
		if left := restoreLeftovers(t, c.dir); len(left) > 0 {
			t.Fatalf("left %v", left)
		}
	}
	forEachFormat(t, func(t *testing.T, format int) {
		backup := asFormat(t, fx.backup, fx.dir, format)
		fi, _ := os.Stat(backup)
		dbSize := int64(len(rawDatabaseOf(t, backup)))
		need := restoreNeed(format, fi.Size(), dbSize, 23)
		t.Run("refused when the update would not fit", func(t *testing.T) {
			c := fx.clone(t)
			c.backup = backup
			d := &disk{dir: c.dir}
			d.budget.Store(restoreReserve + need - 1)
			before := snapshotDir(t, c.dir)
			_, err := c.restore(config{free: d.free})
			if !errors.Is(err, ErrRestoreRefused) || !errors.Is(err, ErrNoSpace) || !strings.Contains(err.Error(), "restore this backup") {
				t.Fatalf("got %v", err)
			}
			if after := snapshotDir(t, c.dir); !reflect.DeepEqual(after, before) {
				t.Fatalf("the refusal changed the data directory:\n%v\n%v", before, after)
			}
			// Exactly enough: it fits, and never takes the reserve.
			d.budget.Store(restoreReserve + need)
			stop := d.sample()
			_, err = c.restore(config{free: d.free})
			stop()
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("database %d bytes, allowed %d, the most written at once %d (%.2f times the database)", dbSize, need, d.peak.Load(),
				float64(d.peak.Load())/float64(dbSize))
			if d.peak.Load() > need {
				t.Fatalf("the restore wrote %d bytes at once, %d allowed", d.peak.Load(), need)
			}
			same(t, c)
		})
		t.Run("the disk fills during the migration", func(t *testing.T) {
			c := fx.clone(t)
			c.backup = backup
			live := fileSHA(t, filepath.Join(c.dir, databaseName))
			d := &disk{dir: c.dir}
			d.budget.Store(100 << 30)
			watched := make(chan struct{})
			var once sync.Once
			migrated := false
			_, err := c.restore(config{free: d.free, spaceWatch: time.Millisecond, hooks: hooks{
				duringMigration: func(version int) error {
					if version == 24 {
						migrated = true
						d.other.Store(d.budget.Load()) // another program fills the disk
						select {
						case <-watched:
						case <-time.After(10 * time.Second):
							return errors.New("the space watch did not stop the migration")
						}
					}
					return nil
				},
				restoreFault: func(p string) error {
					if p == "space-watch-stopped" {
						once.Do(func() { close(watched) })
					}
					return nil
				},
			}})
			if !migrated {
				t.Fatal("the staged copy was not migrated")
			}
			stopped(t, c, err, live)
			d.other.Store(0)
			if _, err := c.restore(config{free: d.free}); err != nil {
				t.Fatal(err)
			}
			same(t, c)
		})
		t.Run("the disk fills during the checkpoint", func(t *testing.T) {
			c := fx.clone(t)
			c.backup = backup
			live := fileSHA(t, filepath.Join(c.dir, databaseName))
			d := &disk{dir: c.dir}
			d.budget.Store(100 << 30)
			// The staged file grows past the database only when the
			// checkpoint writes the migration's pages into it.
			d.fill = func(d *disk) {
				if fi, err := os.Stat(filepath.Join(d.dir, stagedName)); err == nil && fi.Size() > dbSize {
					d.other.Store(d.budget.Load())
				}
			}
			_, err := c.restore(config{free: d.free})
			stopped(t, c, err, live)
			d.fill = nil
			d.other.Store(0)
			if _, err := c.restore(config{free: d.free}); err != nil {
				t.Fatal(err)
			}
			same(t, c)
		})
	})
}

// The allowance for SQLite's writes covers the supported migration chain
// with room to spare: measured on the migration that copies a table holding
// almost the whole database (0024, every stored response), and on the chain
// from schema 20 (0021's copies of the identity tables, then 0022 to 0024).
// The peak is the log of the migration's one transaction plus the growth of
// the file when the checkpoint copies it in, both on disk at once.
func TestRestoreMigrationSpaceFitsTheAllowance(t *testing.T) {
	for _, c := range []struct{ version, n, size int }{{23, 8, 1 << 20}, {20, 4, 1 << 20}} {
		t.Run(fmt.Sprintf("from schema %d", c.version), func(t *testing.T) {
			fx := newBlobFixture(t, c.version, c.n, c.size)
			dir := t.TempDir()
			db := rawDatabaseOf(t, fx.backup)
			staged := filepath.Join(dir, stagedName)
			if err := os.WriteFile(staged, db, 0o600); err != nil {
				t.Fatal(err)
			}
			s, err := open(context.Background(), config{dataDir: dir, dbName: stagedName, mode: modeRaw, syncDir: noSync})
			if err != nil {
				t.Fatal(err)
			}
			var peak int64
			for _, sfx := range []string{"", "-wal", "-shm"} {
				if fi, err := os.Stat(staged + sfx); err == nil {
					peak += fi.Size()
				}
			}
			peak -= int64(len(db))
			s.Close()
			allowed := sqliteAllowance(int64(len(db)), true)
			t.Logf("database %d bytes: the migration and its checkpoint wrote %d more (%.2f times), %d allowed", len(db), peak,
				float64(peak)/float64(len(db)), allowed)
			if peak > allowed {
				t.Fatalf("the migration wrote %d bytes, %d allowed", peak, allowed)
			}
			if peak < int64(len(db)) {
				t.Fatalf("the measurement saw only %d bytes: the migration did not copy the responses", peak)
			}
		})
	}
}
