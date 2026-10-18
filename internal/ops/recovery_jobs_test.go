package ops

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// A recovery upload run as a job (PROTOCOL §6.11) is the synchronous upload:
// from one data directory, at one fixed clock and with one seeded random
// source, the job path and the synchronous path, in place and staged, leave
// byte-identical databases and answer byte-identical results. The upload is
// the provisional-supersession recovery (originals replacing provisional
// receipts, a provisional podcast merged by a decision the server makes).
func TestRecoveryJobEquivalence(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "in place", true: "staged"}[staged], func(t *testing.T) {
			e := newEnv(t)
			e.storeOpts = []storage.OpenOption{storage.WithClock(func() time.Time { return time.UnixMilli(e.clock) })}
			a, b, d := e.device("A"), e.device("B"), e.device("D")
			e.pushOK(t, d, e.fact(d, "account.set_discovery_policy", nil, []any{map[string]any{"entity": "account", "id": e.account}},
				map[string]any{"new_episodes_action": "inbox"}), "applied")
			backup := e.backup(t)
			atBackup := e.headSeq(t)
			e.clock += minute
			sub := e.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed"})
			e.pushOK(t, a, sub, "applied")
			p := contract.PodcastID(e.namespace, sub["op_id"].(string))
			g := contract.GenerationID(e.namespace, p, sub["op_id"].(string))
			ref := e.episodeRef(p, g, "one")
			e.clock += minute
			e.pushOK(t, a, e.lfact(a, "episode.mark_played", ref, map[string]any{"reset_position": false}), "applied")
			lost := e.logCommits(t, atBackup)
			e.restore(t, backup)
			b2, a2 := e.successor(t, b), e.successor(t, a)
			e.clock += minute
			subB := e.fact(b2, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://example.com/feed"})
			e.pushOK(t, b2, subB, "applied")
			var outbox []map[string]any
			for _, cm := range lost {
				for _, x := range cm.entries {
					if x.Fact != nil {
						outbox = append(outbox, x.Fact.Object)
					}
				}
			}
			e.push(a2, outbox...)
			d2 := e.successor(t, d)
			rec, _ := e.startRecovery(t, d2, nil)
			hash := bytes.Repeat([]byte{7}, 32)
			e.clock += minute
			req := protocol.RecoveryUploadRequest{AccountID: e.account, InstallID: d2.inst.InstallID, RecoveryID: rec.id, Entries: entries(lost),
				Steps: protocol.AllRecoveryEndSteps}
			cred := protocol.StoredCredential{Hash: hash, Kind: "device"}

			dir := e.svc.Store.Dir()
			if err := e.svc.Store.Close(); err != nil {
				t.Fatal(err)
			}
			syncDir, jobDir := filepath.Join(t.TempDir(), "sync"), filepath.Join(t.TempDir(), "job")
			for _, to := range []string{syncDir, jobDir} {
				if err := copyDir(dir, to); err != nil {
					t.Fatal(err)
				}
			}
			e.reopen(t) // the env's own checks at the end
			threshold := map[bool]int{false: -1, true: 1}[staged]
			open := func(dir string) *protocol.Service {
				store, err := storage.Open(context.Background(), dir, e.storeOpts...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { store.Close() })
				box, err := sealed.LoadOrCreate(dir)
				if err != nil {
					t.Fatal(err)
				}
				views, err := protocol.NewViewRegistry(filepath.Join(dir, "views"), nil)
				if err != nil {
					t.Fatal(err)
				}
				return &protocol.Service{Store: store, Registry: e.svc.Registry, Views: views, Box: box, Now: e.svc.Now,
					Random: &seededRandom{r: rand.New(rand.NewPCG(5, 16))}, StagingThreshold: threshold}
			}
			syncSvc, jobSvc := open(syncDir), open(jobDir)
			// A copied database opens as a restored one (its devices signed
			// out): the uploader's credential is added to each copy alike.
			for _, svc := range []*protocol.Service{syncSvc, jobSvc} {
				if err := svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error {
					return tx.Account(e.account).InsertCredential(hash, "device", d2.inst.InstallID, nil)
				}); err != nil {
					t.Fatal(err)
				}
			}
			attempts := 0
			jobSvc.SetRecoveryJobHooksForTest(func(point string) error {
				if point == "attempt" {
					attempts++
				}
				return nil
			}, 0, 0)
			want, err := syncSvc.RecoveryUpload(context.Background(), req, protocol.AuthorizeStoredCredential(syncSvc.Now, cred))
			if err != nil {
				t.Fatal(err)
			}
			got, err := jobSvc.SubmitRecoveryUpload(context.Background(), req, protocol.AuthorizeStoredCredential(jobSvc.Now, cred), cred)
			if err != nil {
				t.Fatal(err)
			}
			if st := statuses(want); st["replaced"] == 0 || st["joined"] == 0 {
				t.Fatalf("the upload exercises too little: %v", st)
			}
			wb, _ := json.Marshal(want)
			gb, _ := json.Marshal(got)
			if !bytes.Equal(wb, gb) {
				t.Errorf("results differ:\n  sync %s\n  job  %s", wb, gb)
			}
			// The identical repeat joins nothing: the job path answers it at
			// once, in place, without a job; the synchronous path evaluates
			// it as before (staged in the staged mode).
			e.clock += minute
			want, err = syncSvc.RecoveryUpload(context.Background(), req, protocol.AuthorizeStoredCredential(syncSvc.Now, cred))
			if err != nil {
				t.Fatal(err)
			}
			got, err = jobSvc.SubmitRecoveryUpload(context.Background(), req, protocol.AuthorizeStoredCredential(jobSvc.Now, cred), cred)
			if err != nil {
				t.Fatal(err)
			}
			if st := statuses(want); st["known"] != len(req.Entries) {
				t.Fatalf("the repeat: %v", st)
			}
			if attempts != 1 {
				t.Errorf("%d job attempts; the repeat must not run a job", attempts)
			}
			wb, _ = json.Marshal(want)
			gb, _ = json.Marshal(got)
			if !bytes.Equal(wb, gb) {
				t.Errorf("the repeat's results differ:\n  sync %s\n  job  %s", wb, gb)
			}
			syncSvc.Store.Close()
			jobSvc.Store.Close()
			ws, gs := dumpDatabase(t, syncDir), dumpDatabase(t, jobDir)
			// Each copy took its own incarnation when it opened, before
			// either upload (a copied database opens as a restored one).
			for _, m := range []map[string]string{ws, gs} {
				var kept []string
				for _, line := range strings.Split(m["server_meta"], "\n") {
					if !strings.HasPrefix(line, "incarnation|") {
						kept = append(kept, line)
					}
				}
				m["server_meta"] = strings.Join(kept, "\n")
			}
			if staged {
				// The synchronous repeat publishes a staged copy of the
				// account, which raises its read model's generation (a
				// bulk rewrite) though nothing changed; the job path's
				// in-place answer changes nothing it reads. Only that
				// clock may differ, and only by the job path's being
				// behind.
				var sa, ga string
				var stick, sgen, gtick, ggen int64
				fmt.Sscanf(strings.ReplaceAll(ws["view_clock"], "|", " "), "%s %d %d", &sa, &stick, &sgen)
				fmt.Sscanf(strings.ReplaceAll(gs["view_clock"], "|", " "), "%s %d %d", &ga, &gtick, &ggen)
				if sa != ga || sa == "" || gtick > stick || ggen > sgen {
					t.Errorf("view_clock: sync %s, job %s", ws["view_clock"], gs["view_clock"])
				}
				delete(ws, "view_clock")
				delete(gs, "view_clock")
			}
			for _, table := range sortedKeys(ws, gs) {
				if ws[table] != gs[table] {
					t.Errorf("table %s differs:\n  sync %.600s\n  job  %.600s", table, ws[table], gs[table])
				}
			}
			if gs["recovery_jobs"] != "" {
				t.Errorf("a job row is left: %s", gs["recovery_jobs"])
			}
		})
	}
}

// seededRandom is a reproducible source of the server's choices.
type seededRandom struct{ r *rand.Rand }

func (s *seededRandom) DecisionID(string) string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], s.r.Uint64())
	binary.BigEndian.PutUint64(b[8:], s.r.Uint64())
	b[6], b[8] = b[6]&0x0f|0x40, b[8]&0x3f|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (s *seededRandom) PositionID() [16]byte {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], s.r.Uint64())
	binary.BigEndian.PutUint64(b[8:], s.r.Uint64())
	return b
}

func (s *seededRandom) Index(n int) int { return s.r.IntN(n) }

// dumpDatabase returns every table's rows, in a canonical order, as text.
func dumpDatabase(t *testing.T, dir string) map[string]string {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "nottennapod.db")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
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
	out := map[string]string{}
	for _, table := range tables {
		rows, err := db.Query(`SELECT * FROM "` + table + `"`)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		var lines []string
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			parts := make([]string, len(vals))
			for i, v := range vals {
				if b, ok := v.([]byte); ok {
					parts[i] = fmt.Sprintf("%x", b)
				} else {
					parts[i] = fmt.Sprint(v)
				}
			}
			lines = append(lines, strings.Join(parts, "|"))
		}
		rows.Close()
		sort.Strings(lines)
		out[table] = strings.Join(lines, "\n")
	}
	return out
}

func sortedKeys(maps ...map[string]string) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range maps {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	sort.Strings(out)
	return out
}
