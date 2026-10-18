package importer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer/androiddb"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// queuedBackup writes a copy of the parity backup with its feeds copied
// feedCopies times and each feed's items itemCopies times (new IDs, URLs and
// GUIDs, publication dates a day apart per copy), and a queue of its first
// queued items in item order.
func queuedBackup(t *testing.T, feedCopies, itemCopies, queued int) string {
	t.Helper()
	data, err := os.ReadFile("androiddb/testdata/android-3.12.1/emulator-parity-library.db")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "AntennaPodBackup.db")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	exec := func(q string, args ...any) {
		if _, err := tx.Exec(q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	tagged := func(col, tag string) string {
		return col + " || CASE WHEN instr(" + col + ", '?') > 0 THEN '&" + tag + "' ELSE '?" + tag + "' END"
	}
	for c := range feedCopies {
		if c > 0 {
			exec("CREATE TEMP TABLE f AS SELECT * FROM main.Feeds WHERE id < 1000")
			exec("UPDATE temp.f SET id = id + ?, download_url = "+tagged("download_url", fmt.Sprintf("copy=%d", c)), c*1000)
			exec("INSERT INTO main.Feeds SELECT * FROM temp.f")
			exec("DROP TABLE temp.f")
		}
		for m := range itemCopies {
			k := c*itemCopies + m
			if k == 0 {
				continue
			}
			exec("CREATE TEMP TABLE i AS SELECT * FROM main.FeedItems WHERE id < 1000")
			exec("UPDATE temp.i SET id = id + ?, feed = feed + ?, item_identifier = item_identifier || ?, pubDate = pubDate - ?",
				k*1000, c*1000, fmt.Sprintf("-%d", k), int64(m)*86_400_000)
			exec("INSERT INTO main.FeedItems SELECT * FROM temp.i")
			exec("DROP TABLE temp.i")
			exec("CREATE TEMP TABLE m AS SELECT * FROM main.FeedMedia WHERE id < 1000")
			exec("UPDATE temp.m SET id = id + ?, feeditem = feeditem + ?, download_url = "+tagged("download_url", fmt.Sprintf("item=%d", k)), k*1000, k*1000)
			exec("INSERT INTO main.FeedMedia SELECT * FROM temp.m")
			exec("DROP TABLE temp.m")
		}
	}
	exec("DELETE FROM main.Queue")
	exec("INSERT INTO main.Queue (id, feeditem, feed) SELECT ROW_NUMBER() OVER (ORDER BY i.id), i.id, i.feed FROM main.FeedItems i JOIN main.FeedMedia m ON m.feeditem = i.id ORDER BY i.id LIMIT ?", queued)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return path
}

// An import's queued episodes are concurrent (each depends on its podcast),
// so the queue frontier holds every one until a later command observes the
// manifest. Admitting them reads a few rows each, never the frontier built
// so far: the medium synthetic library (128 podcasts, about 6,000 episodes)
// with 5,000 of them queued, imported for real. The earlier admission read
// the whole frontier for each (Q × (Q − 1) / 2 rows) and every edge ranked
// since its oldest member. Under the race detector, and with -short, a
// smaller library: 16 podcasts, 116 episodes, 100 queued (make check runs
// the full one without the race detector).
func TestImportQueueAdmissionBudget(t *testing.T) {
	feeds, items, queued := 8, 13, 5000
	if testing.Short() || raceEnabled {
		feeds, items, queued = 1, 2, 100
	}
	ctx := context.Background()
	store, err := storage.Open(ctx, filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	box, err := sealed.LoadOrCreate(store.Dir())
	if err != nil {
		t.Fatal(err)
	}
	var account string
	if err := store.Write(ctx, func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		account = a.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	reg := protocol.NewRegistry()
	ops.Register(reg)
	clock := int64(1_790_000_000_000)
	e := &env{t: t, account: account, svc: &protocol.Service{Store: store, Registry: reg, Box: box, Now: func() time.Time { return time.UnixMilli(clock) }}}
	viewstest.Skip(store) // a measurement of the import: the read model's checks of every commit would dominate it
	lib, err := androiddb.Read(ctx, queuedBackup(t, feeds, items, queued), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const importID = "3d1c6f8e-5b2a-4e7d-9c0b-1a2b3c4d5e6f"
	plan, err := FromAndroid(lib, importID, clock)
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	rows := storage.QueueAdmissionRowsRead()
	if _, err := Apply(ctx, e.svc, e.account, plan, 0, nil); err != nil {
		t.Fatal(err)
	}
	rows = storage.QueueAdmissionRowsRead() - rows
	applied := time.Since(began)
	if _, err := Complete(ctx, e.svc, e.account, importID, "", plan); err != nil {
		t.Fatal(err)
	}
	res, err := Summarize(ctx, e.svc.Store, e.account, plan)
	if err != nil {
		t.Fatal(err)
	}
	q := res.Imported.Queue
	var frontier []string
	if err := e.svc.Store.Write(ctx, func(tx *storage.WriteTx) error {
		frontier, err = tx.Account(e.account).QueueFrontier()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("imported %d podcasts, %d episodes, %d queued in %.1f s (apply); admission read %d rows", res.Imported.Podcasts, res.Imported.Episodes, q,
		applied.Seconds(), rows)
	if q < queued*9/10 || len(frontier) != q {
		t.Fatalf("queued %d, frontier %d", q, len(frontier))
	}
	if rows > int64(2*q) {
		t.Fatalf("%d queued episodes' admission read %d rows, want at most %d", q, rows, 2*q)
	}
}
