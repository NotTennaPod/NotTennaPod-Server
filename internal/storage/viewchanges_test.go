package storage

// The read model's change capture (viewchanges.go): its triggers, its
// exactness through the real writers, its work, and its delta readers.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// ---- the oracle: an account's view inputs and stamps, read independently

type entityKey struct{ entity, id string }

func (k entityKey) String() string { return k.entity + ":" + k.id }

type keySet map[entityKey]bool

func (s keySet) String() string {
	var out []string
	for k := range s {
		out = append(out, k.String())
	}
	slices.Sort(out)
	return "{" + strings.Join(out, " ") + "}"
}

func entities(ks ...entityKey) keySet {
	out := keySet{}
	for _, k := range ks {
		out[k] = true
	}
	return out
}

func ep(id string) entityKey  { return entityKey{"episode", id} }
func pod(id string) entityKey { return entityKey{"podcast", id} }

// viewInputs is P(S, a): per entity, everything views.Load reads of it
// (every records column, an episode's registration, its catalog item's
// publication date and insertion rank, a podcast's creating node and that
// node's rank), with its own SQL.
func viewInputs(t testing.TB, q queryer, account string) map[entityKey]string {
	t.Helper()
	ctx := context.Background()
	parts := map[entityKey][]string{}
	read := func(query string, row func(*sql.Rows) (entityKey, string, error)) {
		rows, err := q.QueryContext(ctx, query, account)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			k, v, err := row(rows)
			if err != nil {
				t.Fatal(err)
			}
			parts[k] = append(parts[k], v)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	read(`SELECT entity, entity_id, field_group, value, provenance FROM records WHERE account_id = ?`, func(r *sql.Rows) (entityKey, string, error) {
		var e, id, g, v, p string
		err := r.Scan(&e, &id, &g, &v, &p)
		return entityKey{e, id}, "record " + g + "=" + v + " " + p, err
	})
	read(`SELECT episode_id FROM episodes WHERE account_id = ?`, func(r *sql.Rows) (entityKey, string, error) {
		var id string
		err := r.Scan(&id)
		return ep(id), "registered", err
	})
	read(`SELECT episode_id, pub_date, rowid FROM catalog_items WHERE account_id = ?`, func(r *sql.Rows) (entityKey, string, error) {
		var id string
		var date, rowid int64
		err := r.Scan(&id, &date, &rowid)
		return ep(id), fmt.Sprintf("catalog %d #%d", date, rowid), err
	})
	read(`SELECT p.podcast_id, p.creating_op, (SELECT o.rank FROM node_order o WHERE o.account_id = p.account_id AND o.node_id = p.creating_op)
		FROM podcasts p WHERE p.account_id = ?`, func(r *sql.Rows) (entityKey, string, error) {
		var id, op string
		var rank sql.NullInt64
		err := r.Scan(&id, &op, &rank)
		return pod(id), fmt.Sprintf("podcast by %s at %v", op, rank), err
	})
	out := map[entityKey]string{}
	for k, v := range parts {
		slices.Sort(v)
		out[k] = strings.Join(v, "\n")
	}
	return out
}

// stampsOf reads an account's stamps and clock.
func stampsOf(t testing.TB, q queryer, account string) (map[entityKey]int64, ViewClock) {
	t.Helper()
	ctx := context.Background()
	out := map[entityKey]int64{}
	rows, err := q.QueryContext(ctx, `SELECT entity, entity_id, tick FROM view_changes WHERE account_id = ?`, account)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var k entityKey
		var tick int64
		if err := rows.Scan(&k.entity, &k.id, &tick); err != nil {
			t.Fatal(err)
		}
		out[k] = tick
	}
	rows.Close()
	var c ViewClock
	rows, err = q.QueryContext(ctx, `SELECT tick, generation FROM view_clock WHERE account_id = ?`, account)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		if err := rows.Scan(&c.Tick, &c.Generation); err != nil {
			t.Fatal(err)
		}
	}
	rows.Close()
	return out, c
}

// changedInputs lists the entities whose inputs differ.
func changedInputs(before, after map[entityKey]string) keySet {
	out := keySet{}
	for k, v := range before {
		if after[k] != v {
			out[k] = true
		}
	}
	for k, v := range after {
		if before[k] != v {
			out[k] = true
		}
	}
	return out
}

func stampedAbove(stamps map[entityKey]int64, tick int64) keySet {
	out := keySet{}
	for k, t := range stamps {
		if t > tick {
			out[k] = true
		}
	}
	return out
}

func subset(a, b keySet) bool {
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func equalSets(a, b keySet) bool { return subset(a, b) && subset(b, a) }

var errRollback = errors.New("roll back")

// seedViews gives an account a small library through the real writers:
// nodes, podcasts (one created by a node placed later, two sharing a
// node), registered and catalog-only episodes, and records of every
// entity kind.
func seedViews(t testing.TB, s *Store, account string) {
	t.Helper()
	err := s.Write(context.Background(), func(tx *WriteTx) error {
		a := tx.Account(account)
		if err := a.InsertNode("n:anchor", "fact", 0, nil); err != nil {
			return err
		}
		for i := range 4 {
			if err := a.InsertNode(fmt.Sprintf("n:%d", i), "fact", int64(10+i), nil); err != nil {
				return err
			}
		}
		creators := []string{"n:0", "n:1", "n:2", "n:2", "c:later"}
		for i, op := range creators {
			if err := a.CreatePodcast(fmt.Sprintf("p:%d", i), op); err != nil {
				return err
			}
			if err := a.PutRecord(RecordRow{"podcast", fmt.Sprintf("p:%d", i), "subscription", `{"state":"subscribed"}`, `["n:0"]`}); err != nil {
				return err
			}
		}
		for i := range 24 {
			id, p := fmt.Sprintf("e:%03d", i), fmt.Sprintf("p:%d", i%len(creators))
			if i%6 != 5 {
				if err := a.AddEpisode(id, p, `{"o":1}`, "n:0"); err != nil {
					return err
				}
			}
			if i%4 != 3 {
				if err := a.PutCatalogItem(CatalogItem{EpisodeID: id, PodcastID: p, FirstNode: "n:0", PubDate: int64(1000 + i)}); err != nil {
					return err
				}
			}
			for _, g := range []string{"identity", "metadata", "state"} {
				if err := a.PutRecord(RecordRow{"episode", id, g, fmt.Sprintf(`{"%s":%d}`, g, i), `["n:1"]`}); err != nil {
					return err
				}
			}
		}
		return a.PutRecord(RecordRow{"account", account, "queue_policy", `{"keep_sorted":false}`, `["n:2"]`})
	})
	if err != nil {
		t.Fatal(err)
	}
}

// ---- every trigger, each event, old and new keys

// The canonical capture triggers are the 15 the migration creates, with the
// text a suspended transaction creates them again from.
func TestCaptureTriggersMatchTheMigration(t *testing.T) {
	ref, err := referenceSchema(context.Background(), embeddedMigrations, len(embeddedMigrations))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, o := range ref {
		if o.typ == "trigger" {
			names = append(names, o.name)
		}
	}
	slices.Sort(names)
	var want []string
	for _, table := range []string{"records", "catalog", "episodes", "podcasts", "node_order"} {
		for _, event := range []string{"insert", "update", "delete"} {
			want = append(want, "view_capture_"+table+"_"+event)
		}
	}
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Fatalf("the schema's triggers are %v, want the 15 capture triggers %v", names, want)
	}
	if len(captureTriggers) != 15 {
		t.Fatalf("%d capture triggers", len(captureTriggers))
	}
	for _, c := range captureTriggers {
		o, ok := ref["trigger\x00"+c.name]
		if !ok || o.sql != c.sql {
			t.Errorf("%s: the migration's text differs from captureTriggers:\n%s\n--- captureTriggers:\n%s", c.name, o.sql, c.sql)
		}
	}
}

// Each capture trigger stamps exactly the entities its row change changed,
// the old and the new on a key-changing update, and nothing for a change of
// no view input; and the stamps of each case are that trigger's (dropping
// it alone leaves the change unstamped).
func TestEachCaptureTriggerStamps(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, s, 2)
	account, other := ids[0], ids[1]
	seedViews(t, s, account)
	seedViews(t, s, other)
	exec := func(a *AccountTx, q string, args ...any) error {
		_, err := a.w.tx.ExecContext(ctx, q, append([]any{a.id}, args...)...)
		return err
	}
	type tcase struct {
		name, trigger string
		setup         func(a *AccountTx) error
		change        func(a *AccountTx) error
		want          keySet
	}
	rec := func(e, id, g, v, p string) func(a *AccountTx) error {
		return func(a *AccountTx) error { return a.PutRecord(RecordRow{e, id, g, v, p}) }
	}
	stmt := func(q string, args ...any) func(a *AccountTx) error {
		return func(a *AccountTx) error { return exec(a, q, args...) }
	}
	rank := func(a *AccountTx, node string) int64 {
		var r int64
		if err := a.w.tx.QueryRowContext(ctx, `SELECT rank FROM node_order WHERE account_id = ? AND node_id = ?`, a.id, node).Scan(&r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	cases := []tcase{
		{name: "record insert", trigger: "records_insert", change: rec("episode", "e:000", "extra", `{"x":1}`, `[]`), want: entities(ep("e:000"))},
		{name: "account record insert", trigger: "records_insert", change: rec("account", account, "discovery_policy", `{"a":1}`, `[]`),
			want: entities(entityKey{"account", account})},
		{name: "record value", trigger: "records_update", change: rec("episode", "e:000", "metadata", `{"metadata":"new"}`, `["n:1"]`), want: entities(ep("e:000"))},
		{name: "record provenance only", trigger: "records_update", change: rec("episode", "e:000", "metadata", `{"metadata":0}`, `["n:1","n:2"]`),
			want: entities(ep("e:000"))},
		{name: "record moved to another entity", trigger: "records_update",
			change: stmt(`UPDATE records SET entity_id = 'e:900' WHERE account_id = ? AND entity = 'episode' AND entity_id = 'e:000' AND field_group = 'state'`),
			want:   entities(ep("e:000"), ep("e:900"))},
		{name: "record moved to another kind", trigger: "records_update",
			change: stmt(`UPDATE records SET entity = 'podcast', entity_id = 'p:0' WHERE account_id = ? AND entity = 'episode' AND entity_id = 'e:000' AND field_group = 'extra2'`),
			setup:  rec("episode", "e:000", "extra2", `{}`, `[]`),
			want:   entities(ep("e:000"), pod("p:0"))},
		{name: "record field group renamed", trigger: "records_update",
			change: stmt(`UPDATE records SET field_group = 'meta2' WHERE account_id = ? AND entity = 'episode' AND entity_id = 'e:000' AND field_group = 'metadata'`),
			want:   entities(ep("e:000"))},
		{name: "record delete", trigger: "records_delete", change: func(a *AccountTx) error {
			_, err := a.DeleteRecord("episode", "e:000", "metadata")
			return err
		}, want: entities(ep("e:000"))},
		{name: "catalog insert", trigger: "catalog_insert", change: func(a *AccountTx) error {
			return a.PutCatalogItem(CatalogItem{EpisodeID: "e:003", PodcastID: "p:3", FirstNode: "n:0", PubDate: 7})
		}, want: entities(ep("e:003"))},
		{name: "catalog date", trigger: "catalog_update", change: func(a *AccountTx) error {
			return a.PutCatalogItem(CatalogItem{EpisodeID: "e:000", PodcastID: "p:0", FirstNode: "n:0", PubDate: 5})
		}, want: entities(ep("e:000"))},
		{name: "catalog episode renamed", trigger: "catalog_update",
			change: stmt(`UPDATE catalog_items SET episode_id = 'e:800' WHERE account_id = ? AND episode_id = 'e:000'`), want: entities(ep("e:000"), ep("e:800"))},
		{name: "catalog insertion rank", trigger: "catalog_update",
			change: stmt(`UPDATE catalog_items SET rowid = rowid + 100000 WHERE account_id = ? AND episode_id = 'e:000'`), want: entities(ep("e:000"))},
		{name: "catalog delete", trigger: "catalog_delete", change: func(a *AccountTx) error { return a.DeleteCatalogItem("e:000") }, want: entities(ep("e:000"))},
		{name: "episode registered", trigger: "episodes_insert", change: func(a *AccountTx) error { return a.AddEpisode("e:005", "p:0", `{}`, "n:0") },
			want: entities(ep("e:005"))},
		{name: "episode renamed", trigger: "episodes_update",
			change: stmt(`UPDATE episodes SET episode_id = 'e:700' WHERE account_id = ? AND episode_id = 'e:001'`), want: entities(ep("e:001"), ep("e:700"))},
		{name: "episode delete", trigger: "episodes_delete", change: stmt(`DELETE FROM episodes WHERE account_id = ? AND episode_id = 'e:001'`),
			want: entities(ep("e:001"))},
		{name: "podcast insert", trigger: "podcasts_insert", change: func(a *AccountTx) error { return a.CreatePodcast("p:new", "n:0") },
			want: entities(pod("p:new"))},
		{name: "podcast creating node", trigger: "podcasts_update",
			setup:  func(a *AccountTx) error { return a.CreatePodcast("p:free", "c:free") },
			change: stmt(`UPDATE podcasts SET creating_op = 'n:3' WHERE account_id = ? AND podcast_id = 'p:free'`), want: entities(pod("p:free"))},
		{name: "podcast renamed", trigger: "podcasts_update",
			setup:  func(a *AccountTx) error { return a.CreatePodcast("p:free", "c:free") },
			change: stmt(`UPDATE podcasts SET podcast_id = 'p:moved' WHERE account_id = ? AND podcast_id = 'p:free'`), want: entities(pod("p:free"), pod("p:moved"))},
		{name: "podcast delete", trigger: "podcasts_delete",
			setup:  func(a *AccountTx) error { return a.CreatePodcast("p:free", "c:free") },
			change: stmt(`DELETE FROM podcasts WHERE account_id = ? AND podcast_id = 'p:free'`), want: entities(pod("p:free"))},
		// An unsuspended cascade: deleting a populated podcast removes its
		// episodes' registrations (e:003 has no catalog item, so only its
		// registration's trigger can stamp it) and catalog items.
		{name: "populated podcast deleted", change: stmt(`DELETE FROM podcasts WHERE account_id = ? AND podcast_id = 'p:3'`),
			want: entities(pod("p:3"), ep("e:003"), ep("e:008"), ep("e:013"), ep("e:018"))},
		{name: "creating node placed", trigger: "node_order_insert", change: func(a *AccountTx) error { return a.InsertNode("c:later", "fact", 50, nil) },
			want: entities(pod("p:4"))},
		{name: "node shared by two podcasts moved", trigger: "node_order_update", change: func(a *AccountTx) error {
			return exec(a, `UPDATE node_order SET rank = rank + 1 WHERE account_id = ? AND node_id = 'n:2'`)
		}, want: entities(pod("p:2"), pod("p:3"))},
		{name: "creating nodes renumbered", trigger: "node_order_update", change: func(a *AccountTx) error {
			// Nodes that each land right after the anchor halve one gap until
			// it is exhausted: the account's ranks are renumbered.
			before := rank(a, "n:0")
			for i := 0; rank(a, "n:0") == before; i++ {
				if i == 64 {
					return errors.New("no renumbering")
				}
				if err := a.InsertNode(fmt.Sprintf("k:%04d", 9999-i), "fact", 1, []string{"n:anchor"}); err != nil {
					return err
				}
			}
			return nil
		}, want: entities(pod("p:0"), pod("p:1"), pod("p:2"), pod("p:3"))},
		{name: "node renamed", trigger: "node_order_update",
			setup: func(a *AccountTx) error {
				if err := exec(a, `INSERT INTO causal_nodes (account_id, node_id, kind, normalized_time) VALUES (?, 'c:spare', 'fact', 5)`); err != nil {
					return err
				}
				return a.CreatePodcast("p:spare", "c:spare")
			},
			change: stmt(`UPDATE node_order SET node_id = 'c:spare' WHERE account_id = ? AND node_id = 'n:0'`), want: entities(pod("p:0"), pod("p:spare"))},
		{name: "creating node delete", trigger: "node_order_delete", change: stmt(`DELETE FROM node_order WHERE account_id = ? AND node_id = 'n:1'`),
			want: entities(pod("p:1"))},
		// Changes of no view input stamp nothing.
		{name: "same record again", change: rec("episode", "e:000", "metadata", `{"metadata":0}`, `["n:1"]`), want: keySet{}},
		{name: "catalog item moved to another podcast", change: func(a *AccountTx) error { return a.MoveCatalogItems("p:0", "p:1") }, want: keySet{}},
		{name: "catalog item's other columns", change: func(a *AccountTx) error {
			return a.PutCatalogItem(CatalogItem{EpisodeID: "e:000", PodcastID: "p:2", FirstNode: "n:3", PlayableNode: "n:3", ServerSeenNode: "n:3",
				ServerCatalogOnly: true, PubDate: 1000})
		}, want: keySet{}},
		{name: "episode rebound", change: func(a *AccountTx) error { return a.RebindEpisode("e:000", "p:1") }, want: keySet{}},
		{name: "episode occurrence", change: stmt(`UPDATE episodes SET occurrence = '{"o":2}', registered_by = 'x' WHERE account_id = ? AND episode_id = 'e:000'`),
			want: keySet{}},
		{name: "other node moved", change: stmt(`UPDATE node_order SET rank = rank + 1 WHERE account_id = ? AND node_id = 'n:3'`), want: keySet{}},
		{name: "other node placed", change: func(a *AccountTx) error { return a.InsertNode("x:other", "fact", 60, nil) }, want: keySet{}},
		{name: "missing record deleted", change: func(a *AccountTx) error {
			_, err := a.DeleteRecord("episode", "e:000", "nothing")
			return err
		}, want: keySet{}},
	}
	covered := map[string]bool{}
	for _, c := range cases {
		run := func(dropped string) (keySet, keySet, ViewClock, ViewClock) {
			var stamped, changed keySet
			var before, after ViewClock
			err := s.Write(ctx, func(tx *WriteTx) error {
				a := tx.Account(account)
				if c.setup != nil {
					if err := c.setup(a); err != nil {
						return err
					}
				}
				if dropped != "" {
					if _, err := tx.tx.ExecContext(ctx, `DROP TRIGGER `+dropped); err != nil {
						return err
					}
				}
				inputs := viewInputs(t, tx.tx, account)
				_, before = stampsOf(t, tx.tx, account)
				_, otherBefore := stampsOf(t, tx.tx, other)
				if err := c.change(a); err != nil {
					return err
				}
				changed = changedInputs(inputs, viewInputs(t, tx.tx, account))
				var stamps map[entityKey]int64
				stamps, after = stampsOf(t, tx.tx, account)
				stamped = stampedAbove(stamps, before.Tick)
				if _, o := stampsOf(t, tx.tx, other); o != otherBefore {
					t.Errorf("%s: the other account's clock moved: %+v → %+v", c.name, otherBefore, o)
				}
				return errRollback
			})
			if !errors.Is(err, errRollback) {
				t.Fatalf("%s: %v", c.name, err)
			}
			return stamped, changed, before, after
		}
		stamped, changed, before, after := run("")
		if !equalSets(stamped, c.want) {
			t.Errorf("%s: stamped %v, want %v", c.name, stamped, c.want)
		}
		if !subset(changed, stamped) {
			t.Errorf("%s: inputs of %v changed, stamped %v", c.name, changed, stamped)
		}
		if len(c.want) == 0 && after.Tick != before.Tick {
			t.Errorf("%s: a change of no view input moved the tick from %d to %d", c.name, before.Tick, after.Tick)
		}
		if after.Generation != before.Generation {
			t.Errorf("%s: the generation moved", c.name)
		}
		if c.trigger == "" {
			continue
		}
		covered[c.trigger] = true
		// The stamps are this trigger's: without it, the change is unstamped.
		if again, changedAgain, _, _ := run("view_capture_" + c.trigger); len(again) != 0 || len(changedAgain) == 0 {
			t.Errorf("%s: without %s the change of %v stamped %v", c.name, c.trigger, changedAgain, again)
		}
	}
	for _, tr := range captureTriggers {
		if !covered[strings.TrimPrefix(tr.name, "view_capture_")] {
			t.Errorf("no case exercises %s", tr.name)
		}
	}
}

// ---- exactness through the real writers

// capState is what the random writer knows of an account's library.
type capState struct {
	podcasts map[string]string // podcast → creating node
	placed   map[string]bool   // nodes with a rank
	episodes map[string]bool   // registered
	catalog  map[string]CatalogItem
	records  map[[3]string][2]string // (entity, id, group) → value, provenance
}

func loadCapState(t testing.TB, tx *WriteTx, account string) *capState {
	t.Helper()
	ctx := context.Background()
	st := &capState{podcasts: map[string]string{}, placed: map[string]bool{}, episodes: map[string]bool{}, catalog: map[string]CatalogItem{},
		records: map[[3]string][2]string{}}
	each := func(q string, row func(*sql.Rows) error) {
		rows, err := tx.tx.QueryContext(ctx, q, account)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			if err := row(rows); err != nil {
				t.Fatal(err)
			}
		}
	}
	each(`SELECT podcast_id, creating_op FROM podcasts WHERE account_id = ?`, func(r *sql.Rows) error {
		var p, op string
		err := r.Scan(&p, &op)
		st.podcasts[p] = op
		return err
	})
	each(`SELECT node_id FROM node_order WHERE account_id = ?`, func(r *sql.Rows) error {
		var n string
		err := r.Scan(&n)
		st.placed[n] = true
		return err
	})
	each(`SELECT episode_id FROM episodes WHERE account_id = ?`, func(r *sql.Rows) error {
		var e string
		err := r.Scan(&e)
		st.episodes[e] = true
		return err
	})
	each(`SELECT episode_id FROM catalog_items WHERE account_id = ?`, func(r *sql.Rows) error {
		var e string
		if err := r.Scan(&e); err != nil {
			return err
		}
		it, _, err := tx.Account(account).CatalogItem(e)
		st.catalog[e] = it
		return err
	})
	each(`SELECT entity, entity_id, field_group, value, provenance FROM records WHERE account_id = ?`, func(r *sql.Rows) error {
		var k [3]string
		var v, p string
		err := r.Scan(&k[0], &k[1], &k[2], &v, &p)
		st.records[k] = [2]string{v, p}
		return err
	})
	return st
}

func pick[K comparable, V any](r *rand.Rand, m map[K]V, less func(a, b K) int) (K, bool) {
	var zero K
	if len(m) == 0 {
		return zero, false
	}
	ks := slices.SortedFunc(maps.Keys(m), less)
	return ks[r.IntN(len(ks))], true
}

func pickString[V any](r *rand.Rand, m map[string]V) (string, bool) {
	return pick(r, m, strings.Compare)
}

func pickRecord(r *rand.Rand, m map[[3]string][2]string) ([3]string, bool) {
	return pick(r, m, func(a, b [3]string) int {
		return strings.Compare(strings.Join(a[:], "\x00"), strings.Join(b[:], "\x00"))
	})
}

// Kinds of writer call.
const (
	callExact = iota // one statement that changes view inputs: it stamps exactly what it changed
	callNoop         // changes no view input: no stamp and no tick
	callNodes        // places a node, perhaps renumbering: stamps at least what changed, podcasts only
)

type capCall struct {
	name string
	kind int
	run  func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) // false: not applicable now
}

var capCalls = []capCall{
	{"record.insert", callExact, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		var e, id string
		switch r.IntN(3) {
		case 0:
			e, id = "account", a.id
		case 1:
			e, id = "podcast", fmt.Sprintf("p:%d", r.IntN(8))
		default:
			e, id = "episode", fmt.Sprintf("e:%03d", r.IntN(40))
		}
		g := fmt.Sprintf("g%d", n)
		return true, a.PutRecord(RecordRow{e, id, g, fmt.Sprintf(`{"v":%d}`, n), `[]`})
	}},
	{"record.value", callExact, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		k, ok := pickRecord(r, st.records)
		if !ok {
			return false, nil
		}
		return true, a.PutRecord(RecordRow{k[0], k[1], k[2], fmt.Sprintf(`{"v":%d}`, n), st.records[k][1]})
	}},
	{"record.provenance", callExact, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		k, ok := pickRecord(r, st.records)
		if !ok {
			return false, nil
		}
		return true, a.PutRecord(RecordRow{k[0], k[1], k[2], st.records[k][0], fmt.Sprintf(`["n%d"]`, n)})
	}},
	{"record.same", callNoop, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		k, ok := pickRecord(r, st.records)
		if !ok {
			return false, nil
		}
		return true, a.PutRecord(RecordRow{k[0], k[1], k[2], st.records[k][0], st.records[k][1]})
	}},
	{"record.delete", callExact, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		k, ok := pickRecord(r, st.records)
		if !ok {
			return false, nil
		}
		_, err := a.DeleteRecord(k[0], k[1], k[2])
		return true, err
	}},
	{"record.delete-missing", callNoop, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		_, err := a.DeleteRecord("episode", "e:missing", "metadata")
		return true, err
	}},
	{"podcast.create", callExact, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		var op string
		switch r.IntN(4) {
		case 0: // a node with a rank
			op, _ = pickString(r, st.placed)
		case 1: // the node of another podcast
			if p, ok := pickString(r, st.podcasts); ok {
				op = st.podcasts[p]
			}
		}
		if op == "" { // a node placed later
			op = fmt.Sprintf("c:%04d", n)
		}
		return true, a.CreatePodcast(fmt.Sprintf("p:n%04d", n), op)
	}},
	{"episode.add", callExact, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		p, ok := pickString(r, st.podcasts)
		if !ok {
			return false, nil
		}
		id := fmt.Sprintf("e:n%04d", n)
		for _, e := range slices.Sorted(maps.Keys(st.catalog)) { // sometimes a catalog item that was not registered
			if !st.episodes[e] && r.IntN(2) == 0 {
				id = e
				break
			}
		}
		return true, a.AddEpisode(id, p, `{"o":1}`, "n:0")
	}},
	{"catalog.insert", callExact, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		p, ok := pickString(r, st.podcasts)
		if !ok {
			return false, nil
		}
		id := fmt.Sprintf("e:c%04d", n)
		for _, e := range slices.Sorted(maps.Keys(st.episodes)) { // sometimes a registered episode without one
			if _, has := st.catalog[e]; !has && r.IntN(2) == 0 {
				id = e
				break
			}
		}
		return true, a.PutCatalogItem(CatalogItem{EpisodeID: id, PodcastID: p, FirstNode: "n:0", PubDate: int64(r.IntN(5000))})
	}},
	{"catalog.date", callExact, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		e, ok := pickString(r, st.catalog)
		if !ok {
			return false, nil
		}
		it := st.catalog[e]
		it.PubDate += 1 + int64(r.IntN(100))
		return true, a.PutCatalogItem(it)
	}},
	{"catalog.other-columns", callNoop, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		e, ok := pickString(r, st.catalog)
		p, _ := pickString(r, st.podcasts)
		if !ok {
			return false, nil
		}
		it := st.catalog[e]
		it.PodcastID, it.FirstNode, it.PlayableNode, it.ServerSeenNode = p, fmt.Sprintf("f%d", n), fmt.Sprintf("y%d", n), fmt.Sprintf("s%d", n)
		it.ServerCatalogOnly = !it.ServerCatalogOnly
		return true, a.PutCatalogItem(it)
	}},
	{"catalog.same", callNoop, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		e, ok := pickString(r, st.catalog)
		if !ok {
			return false, nil
		}
		return true, a.PutCatalogItem(st.catalog[e])
	}},
	{"catalog.delete", callExact, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		e, ok := pickString(r, st.catalog)
		if !ok {
			return false, nil
		}
		return true, a.DeleteCatalogItem(e)
	}},
	{"catalog.move", callNoop, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		e, ok := pickString(r, st.catalog)
		into, _ := pickString(r, st.podcasts)
		if !ok {
			return false, nil
		}
		return true, a.MoveCatalogItems(st.catalog[e].PodcastID, into)
	}},
	{"episode.rebind", callNoop, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		e, ok := pickString(r, st.episodes)
		p, _ := pickString(r, st.podcasts)
		if !ok {
			return false, nil
		}
		return true, a.RebindEpisode(e, p)
	}},
	{"podcast.delete-populated", callExact, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		// An unsuspended cascade: the podcast's episodes and catalog items go
		// with it (no production writer does this outside a bulk rewrite).
		var populated []string
		for _, p := range slices.Sorted(maps.Keys(st.podcasts)) {
			if has, err := a.EpisodesOfPodcast(p); err != nil {
				return false, err
			} else if len(has) > 0 && r.IntN(4) == 0 {
				populated = append(populated, p)
			}
		}
		if len(populated) == 0 {
			return false, nil
		}
		_, err := a.w.tx.ExecContext(a.w.ctx, `DELETE FROM podcasts WHERE account_id = ? AND podcast_id = ?`, a.id, populated[r.IntN(len(populated))])
		return true, err
	}},
	{"node.creating", callNodes, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		for _, p := range slices.Sorted(maps.Keys(st.podcasts)) {
			if op := st.podcasts[p]; !st.placed[op] {
				deps := []string{}
				if d, ok := pickString(r, st.placed); ok && r.IntN(2) == 0 {
					deps = []string{d}
				}
				return true, a.InsertNode(op, "fact", int64(10+r.IntN(90)), deps)
			}
		}
		return false, nil
	}},
	{"node.other", callNodes, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		return true, a.InsertNode(fmt.Sprintf("x:%04d", n), "fact", int64(10+r.IntN(90)), nil)
	}},
	{"node.crowd", callNodes, func(a *AccountTx, st *capState, r *rand.Rand, n int) (bool, error) {
		// Right after the anchor, each halving one gap: renumbering, now and then.
		for i := range 4 {
			if err := a.InsertNode(fmt.Sprintf("k:%06d", 999999-4*n-i), "fact", 1, []string{"n:anchor"}); err != nil {
				return true, err
			}
		}
		return true, nil
	}},
}

// Every change of a view input made through the real writers is stamped
// above the tick its transaction began at; a writer call of one statement
// stamps exactly the entities it changed, and one that changes no view
// input (an identical upsert, a podcast-only move) stamps nothing and leaves
// the tick; a transaction that rolls back leaves stamps and clock; the
// other account is untouched; and the delta readers return, in a later
// snapshot, every stamped entity.
func TestCaptureIsExactThroughTheWriters(t *testing.T) {
	txs := 200
	if os.Getenv("NTP_READMODEL_LONG") != "" {
		txs *= 10
	}
	seeds := []uint64{1, 2, 3}
	if v, err := strconv.ParseUint(os.Getenv("NTP_CAPTURE_SEED"), 10, 64); err == nil {
		seeds = []uint64{v}
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprint("seed ", seed), func(t *testing.T) {
			t.Logf("seed %d, %d transactions (NTP_CAPTURE_SEED replays one seed)", seed, txs)
			run := runCaptureSequence(t, seed, txs, true)
			for _, c := range capCalls {
				if run.bins[c.name] < txs/20 {
					t.Errorf("only %d %s calls", run.bins[c.name], c.name)
				}
			}
			if run.renumbered == 0 || run.rolledBack == 0 {
				t.Errorf("%d renumberings, %d rollbacks", run.renumbered, run.rolledBack)
			}
			t.Logf("calls %v; %d renumberings; %d rollbacks", run.bins, run.renumbered, run.rolledBack)
		})
	}
}

// A seed replays: the random writer draws the same calls on the same
// entities, so a failing seed reproduces its failure.
func TestCaptureRandomWriterReplays(t *testing.T) {
	first := runCaptureSequence(t, 7, 60, false)
	for range 2 {
		if again := runCaptureSequence(t, 7, 60, false); !slices.Equal(again.trace, first.trace) {
			t.Fatalf("seed 7 drew another sequence:\n%s\nagainst\n%s", strings.Join(again.trace, "\n"), strings.Join(first.trace, "\n"))
		}
	}
	if len(first.trace) < 60 {
		t.Fatalf("a trace of %d calls", len(first.trace))
	}
}

type captureRun struct {
	trace                  []string // each applied call and the entities it changed
	bins                   map[string]int
	renumbered, rolledBack int
}

// runCaptureSequence runs the random writer for a seed, checking capture
// after every call when check is set, and returns its trace; a failure
// logs the trace's end.
func runCaptureSequence(t *testing.T, seed uint64, txs int, check bool) (run captureRun) {
	t.Helper()
	ctx := context.Background()
	defer func() {
		if t.Failed() {
			from := max(0, len(run.trace)-40)
			t.Logf("seed %d trace, from call %d:\n%s", seed, from, strings.Join(run.trace[from:], "\n"))
		}
	}()
	r := rand.New(rand.NewPCG(seed, 25))
	s := openTest(t, config{dataDir: t.TempDir()})
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for _, id := range []string{capA, capB} {
			if _, err := tx.CreateAccountWithIDs(id, "ns-"+id); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	account, other := capA, capB
	seedViews(t, s, account)
	seedViews(t, s, other)
	run.bins = map[string]int{}
	n := 0
	for i := range txs {
		var start ViewClock
		var startInputs map[entityKey]string
		var endClock ViewClock
		var netChanged keySet
		var rollBack = r.IntN(10) == 0
		err := s.Write(ctx, func(tx *WriteTx) error {
			a := tx.Account(account)
			startInputs = viewInputs(t, tx.tx, account)
			_, start = stampsOf(t, tx.tx, account)
			_, otherStart := stampsOf(t, tx.tx, other)
			for range 1 + r.IntN(4) {
				st := loadCapState(t, tx, account)
				call := capCalls[r.IntN(len(capCalls))]
				n++
				inputs := viewInputs(t, tx.tx, account)
				ranks := nodeRanks(t, tx, account)
				_, before := stampsOf(t, tx.tx, account)
				applicable, err := call.run(a, st, r, n)
				if err != nil {
					return fmt.Errorf("%s: %w", call.name, err)
				}
				if !applicable {
					continue
				}
				run.bins[call.name]++
				changed := changedInputs(inputs, viewInputs(t, tx.tx, account))
				run.trace = append(run.trace, fmt.Sprintf("tx %d %s %v", i, call.name, changed))
				if !check {
					continue
				}
				stamps, after := stampsOf(t, tx.tx, account)
				stamped := stampedAbove(stamps, before.Tick)
				if after.Generation != before.Generation {
					t.Fatalf("tx %d %s: the generation moved", i, call.name)
				}
				switch call.kind {
				case callExact:
					if len(changed) == 0 || !equalSets(changed, stamped) {
						t.Fatalf("tx %d %s: inputs of %v changed, stamped %v", i, call.name, changed, stamped)
					}
				case callNoop:
					if len(changed) != 0 || len(stamped) != 0 || after.Tick != before.Tick {
						t.Fatalf("tx %d %s: changed %v, stamped %v, tick %d → %d", i, call.name, changed, stamped, before.Tick, after.Tick)
					}
				case callNodes:
					moved := !maps.Equal(ranks, nodeRanksWithout(nodeRanks(t, tx, account), ranks))
					if moved {
						run.renumbered++
					}
					if !subset(changed, stamped) {
						t.Fatalf("tx %d %s: inputs of %v changed, stamped %v", i, call.name, changed, stamped)
					}
					for k := range stamped {
						if k.entity != "podcast" {
							t.Fatalf("tx %d %s: a node stamped %v", i, call.name, k)
						}
					}
					if len(changed) == 0 && !moved && (len(stamped) != 0 || after.Tick != before.Tick) {
						t.Fatalf("tx %d %s: nothing changed, yet stamped %v (tick %d → %d)", i, call.name, stamped, before.Tick, after.Tick)
					}
				}
			}
			netChanged = changedInputs(startInputs, viewInputs(t, tx.tx, account))
			stamps, end := stampsOf(t, tx.tx, account)
			if stamped := stampedAbove(stamps, start.Tick); check && !subset(netChanged, stamped) {
				t.Fatalf("tx %d: inputs of %v changed, stamped %v", i, netChanged, stamped)
			}
			if _, o := stampsOf(t, tx.tx, other); o != otherStart {
				t.Fatalf("tx %d: the other account's clock moved", i)
			}
			endClock = end
			if rollBack {
				return errRollback
			}
			return nil
		})
		if rollBack {
			if !errors.Is(err, errRollback) {
				t.Fatal(err)
			}
			run.rolledBack++
			run.trace = append(run.trace, fmt.Sprintf("tx %d rolled back", i))
			if _, c := stampsOf(t, s.reader, account); c != start {
				t.Fatalf("tx %d: a rolled-back transaction moved the clock %+v → %+v", i, start, c)
			}
			if changed := changedInputs(startInputs, viewInputs(t, s.reader, account)); len(changed) != 0 {
				t.Fatalf("tx %d: a rolled-back transaction changed %v", i, changed)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if !check {
			continue
		}
		// In a later snapshot, the delta walk returns every stamped
		// entity, and the entity readers read what the full readers do.
		walked := walkChanges(t, s, account, start.Tick)
		stamps, now := stampsOf(t, s.reader, account)
		if now != endClock || !equalSets(walked, stampedAbove(stamps, start.Tick)) || !subset(netChanged, walked) {
			t.Fatalf("tx %d: walked %v; clock %+v (committed %+v); changed %v", i, walked, now, endClock, netChanged)
		}
		checkEntityReaders(t, s, account, walked)
	}
	return run
}

func nodeRanks(t testing.TB, tx *WriteTx, account string) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	rows, err := tx.tx.QueryContext(context.Background(), `SELECT node_id, rank FROM node_order WHERE account_id = ?`, account)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var n string
		var r int64
		if err := rows.Scan(&n, &r); err != nil {
			t.Fatal(err)
		}
		out[n] = r
	}
	return out
}

// nodeRanksWithout keeps the ranks of the nodes that were already placed.
func nodeRanksWithout(now, before map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for n := range before {
		out[n] = now[n]
	}
	return out
}

// walkChanges reads every change above base, page by page, in one snapshot,
// checking the pages' order and sizes.
func walkChanges(t testing.TB, s *Store, account string, base int64) keySet {
	t.Helper()
	out := keySet{}
	err := s.Read(context.Background(), func(r *ReadTx) error {
		cur := ViewCursor{Tick: base}
		var last *ViewChange
		for page := 0; ; page++ {
			changes, next, more, err := r.Account(account).ViewChangesSince(base, cur)
			if err != nil {
				return err
			}
			if more && len(changes) != ViewChangesPage || len(changes) > ViewChangesPage {
				return fmt.Errorf("page %d: %d changes, more %v", page, len(changes), more)
			}
			for i := range changes {
				c := changes[i]
				if c.Tick <= base || last != nil && cmpChange(*last, c) >= 0 {
					return fmt.Errorf("page %d: %+v after %+v (base %d)", page, c, last, base)
				}
				k := entityKey{c.Entity, c.ID}
				if out[k] {
					return fmt.Errorf("%v twice", k)
				}
				out[k] = true
				last = &c
			}
			if !more {
				return nil
			}
			cur = next
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func cmpChange(a, b ViewChange) int {
	if a.Tick != b.Tick {
		if a.Tick < b.Tick {
			return -1
		}
		return 1
	}
	return cmpJoin(strings.Compare(a.Entity, b.Entity), strings.Compare(a.ID, b.ID))
}

// checkEntityReaders reads the entities with the entity readers and with the
// full readers, in one snapshot: they agree.
func checkEntityReaders(t testing.TB, s *Store, account string, entities keySet) {
	t.Helper()
	byKind := map[string][]string{}
	for k := range entities {
		byKind[k.entity] = append(byKind[k.entity], k.id)
	}
	for _, ids := range byKind {
		slices.Sort(ids)
	}
	err := s.Read(context.Background(), func(r *ReadTx) error {
		a := r.Account(account)
		all, err := a.ViewRecords()
		if err != nil {
			return err
		}
		for kind, ids := range byKind {
			got, err := a.ViewEntityRecords(kind, ids)
			if err != nil {
				return err
			}
			var want []RecordRow
			for _, row := range all {
				if row.Entity == kind && slices.Contains(ids, row.EntityID) {
					want = append(want, row)
				}
			}
			if !slices.Equal(got, want) {
				return fmt.Errorf("%s records of %v: %v, the full reader %v", kind, ids, got, want)
			}
		}
		episodes := byKind["episode"]
		registered, err := a.RegisteredEpisodes(episodes)
		if err != nil {
			return err
		}
		rows, err := a.EpisodeRows()
		if err != nil {
			return err
		}
		var wantRegistered []string
		for _, e := range rows {
			if slices.Contains(episodes, e.EpisodeID) {
				wantRegistered = append(wantRegistered, e.EpisodeID)
			}
		}
		if !slices.Equal(registered, wantRegistered) {
			return fmt.Errorf("registered %v, the full reader %v", registered, wantRegistered)
		}
		catalog, err := a.CatalogRowsFor(episodes)
		if err != nil {
			return err
		}
		allCatalog, err := a.CatalogRows()
		if err != nil {
			return err
		}
		var wantCatalog []CatalogRow
		for _, c := range allCatalog {
			if slices.Contains(episodes, c.EpisodeID) {
				wantCatalog = append(wantCatalog, c)
			}
		}
		slices.SortFunc(wantCatalog, func(x, y CatalogRow) int { return strings.Compare(x.EpisodeID, y.EpisodeID) })
		if !slices.Equal(catalog, wantCatalog) {
			return fmt.Errorf("catalog %v, the full reader %v", catalog, wantCatalog)
		}
		podcasts := byKind["podcast"]
		ranks, err := a.PodcastRanksFor(podcasts)
		if err != nil {
			return err
		}
		allRanks, err := a.PodcastRanks()
		if err != nil {
			return err
		}
		for _, p := range podcasts {
			if ranks[p] != allRanks[p] { // a missing podcast: 0 in both
				return fmt.Errorf("podcast %s: rank %d, the full reader %d", p, ranks[p], allRanks[p])
			}
		}
		if len(ranks) != len(podcasts) {
			return fmt.Errorf("%d ranks for %d podcasts", len(ranks), len(podcasts))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// ---- a provenance-only change

// Publishing the same value with another provenance (a second favourite
// from another operation) is stamped, and the delta readers return the new
// provenance.
func TestProvenanceOnlyChangeIsCaptured(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	account := createAccounts(t, s, 1)[0]
	seedViews(t, s, account)
	put := func(prov string) {
		if err := s.Write(ctx, func(tx *WriteTx) error {
			return tx.Account(account).PutRecord(RecordRow{"episode", "e:007", "favorite", `{"on":true}`, prov})
		}); err != nil {
			t.Fatal(err)
		}
	}
	put(`["aaaaaaaa-0000-4000-8000-000000000001"]`)
	var base ViewClock
	s.Read(ctx, func(r *ReadTx) error { base, _ = r.Account(account).ViewClock(); return nil })
	put(`["aaaaaaaa-0000-4000-8000-000000000001","aaaaaaaa-0000-4000-8000-000000000002"]`)
	if walked := walkChanges(t, s, account, base.Tick); !equalSets(walked, entities(ep("e:007"))) {
		t.Fatalf("a provenance-only change stamped %v", walked)
	}
	if err := s.Read(ctx, func(r *ReadTx) error {
		rows, err := r.Account(account).ViewEntityRecords("episode", []string{"e:007"})
		if err != nil {
			return err
		}
		for _, row := range rows {
			if row.FieldGroup == "favorite" {
				if row.Provenance != `["aaaaaaaa-0000-4000-8000-000000000001","aaaaaaaa-0000-4000-8000-000000000002"]` {
					t.Errorf("the delta read %s", row.Provenance)
				}
				return nil
			}
		}
		return errors.New("no favorite record")
	}); err != nil {
		t.Fatal(err)
	}
}

// ---- deterministic work

// countCaptureWrites counts, on the writer connection, the rows written to
// the capture clock and stamps (each insert and each update).
func countCaptureWrites(t testing.TB, s *Store) (reset func(), read func() (clock, stamps int)) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []string{
		`CREATE TEMP TABLE IF NOT EXISTS capture_writes (what TEXT PRIMARY KEY, n INTEGER NOT NULL)`,
		`CREATE TEMP TRIGGER IF NOT EXISTS count_clock_insert AFTER INSERT ON main.view_clock BEGIN
			INSERT INTO capture_writes VALUES ('clock', 1) ON CONFLICT (what) DO UPDATE SET n = n + 1; END`,
		`CREATE TEMP TRIGGER IF NOT EXISTS count_clock_update AFTER UPDATE ON main.view_clock BEGIN
			INSERT INTO capture_writes VALUES ('clock', 1) ON CONFLICT (what) DO UPDATE SET n = n + 1; END`,
		`CREATE TEMP TRIGGER IF NOT EXISTS count_stamp_insert AFTER INSERT ON main.view_changes BEGIN
			INSERT INTO capture_writes VALUES ('stamps', 1) ON CONFLICT (what) DO UPDATE SET n = n + 1; END`,
		`CREATE TEMP TRIGGER IF NOT EXISTS count_stamp_update AFTER UPDATE ON main.view_changes BEGIN
			INSERT INTO capture_writes VALUES ('stamps', 1) ON CONFLICT (what) DO UPDATE SET n = n + 1; END`,
	} {
		if _, err := s.writer.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	reset = func() {
		if _, err := s.writer.ExecContext(ctx, `DELETE FROM temp.capture_writes`); err != nil {
			t.Fatal(err)
		}
	}
	read = func() (clock, stamps int) {
		if err := s.writer.QueryRowContext(ctx, `SELECT COALESCE((SELECT n FROM temp.capture_writes WHERE what = 'clock'), 0),
			COALESCE((SELECT n FROM temp.capture_writes WHERE what = 'stamps'), 0)`).Scan(&clock, &stamps); err != nil {
			t.Fatal(err)
		}
		return clock, stamps
	}
	return reset, read
}

func allClocks(t testing.TB, s *Store) map[string]ViewClock {
	t.Helper()
	out := map[string]ViewClock{}
	rows, err := s.reader.Query(`SELECT account_id, tick, generation FROM view_clock`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var c ViewClock
		if err := rows.Scan(&id, &c.Tick, &c.Generation); err != nil {
			t.Fatal(err)
		}
		out[id] = c
	}
	return out
}

// A routine commit writes 2 capture rows per changed row of the view inputs
// (the tick, then the stamp), one more stamp for an update's old key, and
// keeps one stamp per changed entity; it touches only its account's clock;
// a change of no input writes nothing; a suspended transaction writes no
// stamp and one clock row per declared account.
func TestCaptureWorkIsPerChangedRow(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, s, 2)
	account, other := ids[0], ids[1]
	seedViews(t, s, account)
	seedViews(t, s, other)
	reset, read := countCaptureWrites(t, s)
	for _, c := range []struct {
		name                   string
		write                  func(tx *WriteTx) error
		clock, stamps, entries int // capture rows written; stamps kept above the start
	}{
		{"three groups of one episode and one of another", func(tx *WriteTx) error {
			a := tx.Account(account)
			for _, r := range []RecordRow{{"episode", "e:001", "metadata", `{"m":2}`, `[]`}, {"episode", "e:001", "state", `{"s":2}`, `[]`},
				{"episode", "e:001", "new", `{}`, `[]`}, {"episode", "e:002", "metadata", `{"m":2}`, `[]`}} {
				if err := a.PutRecord(r); err != nil {
					return err
				}
			}
			return nil
		}, 4, 4, 2},
		{"a record moved to another episode", func(tx *WriteTx) error {
			_, err := tx.tx.ExecContext(ctx, `UPDATE records SET entity_id = 'e:950' WHERE account_id = ? AND entity_id = 'e:003' AND field_group = 'state'`, account)
			return err
		}, 1, 2, 2},
		{"a creating node moved", func(tx *WriteTx) error {
			_, err := tx.tx.ExecContext(ctx, `UPDATE node_order SET rank = rank + 1 WHERE account_id = ? AND node_id = 'n:1'`, account)
			return err
		}, 1, 1, 1},
		{"no input changed", func(tx *WriteTx) error {
			a := tx.Account(account)
			if err := a.PutRecord(RecordRow{"episode", "e:004", "metadata", `{"metadata":4}`, `["n:1"]`}); err != nil {
				return err
			}
			if err := a.RebindEpisode("e:004", "p:2"); err != nil {
				return err
			}
			if err := a.MoveCatalogItems("p:4", "p:0"); err != nil {
				return err
			}
			return a.InsertNode("x:work", "fact", 70, nil)
		}, 0, 0, 0},
		{"a suspended bulk rewrite", func(tx *WriteTx) error {
			if err := tx.SuspendViewCapture(account); err != nil {
				return err
			}
			for _, q := range []string{
				`CREATE TEMP TABLE bulk AS SELECT * FROM main.records WHERE account_id = ?1`,
				`DELETE FROM main.records WHERE account_id = ?1`,
				`INSERT INTO main.records SELECT * FROM temp.bulk`,
				`UPDATE catalog_items SET pub_date = pub_date + 1 WHERE account_id = ?1`,
				`UPDATE node_order SET rank = rank + 1 WHERE account_id = ?1`,
				`DROP TABLE temp.bulk`,
			} {
				var args []any
				if strings.Contains(q, "?1") {
					args = []any{account}
				}
				if _, err := tx.tx.ExecContext(ctx, q, args...); err != nil {
					return err
				}
			}
			return nil
		}, 1, 0, 0},
	} {
		before := allClocks(t, s)
		reset()
		if err := s.Write(ctx, func(tx *WriteTx) error { return c.write(tx) }); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		clock, stamps := read()
		after := allClocks(t, s)
		kept, _ := stampsOf(t, s.reader, account)
		entries := len(stampedAbove(kept, before[account].Tick))
		if clock != c.clock || stamps != c.stamps || entries != c.entries {
			t.Errorf("%s: %d clock and %d stamp writes, %d stamps kept; want %d, %d, %d", c.name, clock, stamps, entries, c.clock, c.stamps, c.entries)
		}
		if after[other] != before[other] {
			t.Errorf("%s: the other account's clock moved", c.name)
		}
		if c.clock > 0 && after[account] == before[account] {
			t.Errorf("%s: the clock did not move", c.name)
		}
	}
}

// A power user's library: with 50,400 episodes' changes stamped, a routine
// change writes as many capture rows as in a small library, the delta read
// after it returns that change alone, and the walk over everything still
// returns each change once.
func TestCaptureWorkDoesNotGrowWithTheLibrary(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	ids := createAccounts(t, s, 2)
	small, large := ids[0], ids[1]
	const n = 50_400
	if err := s.Write(ctx, func(tx *WriteTx) error {
		for account, count := range map[string]int{small: 50, large: n} {
			if _, err := tx.tx.ExecContext(ctx, `WITH RECURSIVE i(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM i WHERE n + 1 < ?2)
				INSERT INTO records (account_id, entity, entity_id, field_group, value, provenance)
				SELECT ?1, 'episode', printf('e:%06d', n), 'state', '{}', '[]' FROM i`, account, count); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reset, read := countCaptureWrites(t, s)
	for _, account := range []string{small, large} {
		_, before := stampsOf(t, s.reader, account)
		reset()
		if err := s.Write(ctx, func(tx *WriteTx) error {
			return tx.Account(account).PutRecord(RecordRow{"episode", "e:000007", "state", `{"played":true}`, `[]`})
		}); err != nil {
			t.Fatal(err)
		}
		if clock, stamps := read(); clock != 1 || stamps != 1 {
			t.Errorf("a routine change wrote %d clock and %d stamp rows", clock, stamps)
		}
		if got := walkChanges(t, s, account, before.Tick); !equalSets(got, entities(ep("e:000007"))) {
			t.Errorf("the delta after a routine change: %v", got)
		}
	}
	if got := walkChanges(t, s, large, 0); len(got) != n {
		t.Fatalf("the whole walk returned %d of %d changes", len(got), n)
	}
}

// The delta query reads a range of the covering tick index, and the entity
// readers read by primary key: neither scans.
func TestCaptureReadersUseTheirIndexes(t *testing.T) {
	s := openTest(t, config{dataDir: t.TempDir()})
	account := createAccounts(t, s, 1)[0]
	seedViews(t, s, account)
	plan := func(q string, args ...any) string {
		t.Helper()
		rows, err := s.reader.Query(`EXPLAIN QUERY PLAN `+q, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			out = append(out, detail)
		}
		return strings.Join(out, "\n")
	}
	in3 := []any{"a", "b", "c"}
	for _, c := range []struct {
		name, sql string
		args      []any
		want      []string
	}{
		{"changes", viewChangesSQL, []any{account, 5, 5, "episode", "e:1", ViewChangesPage + 1},
			[]string{"SEARCH view_changes USING COVERING INDEX view_changes_by_tick (account_id=? AND (tick,entity,entity_id)>(?,?,?))"}},
		{"records", fmt.Sprintf(viewEntityRecordsSQL, "?,?,?"), append([]any{account, "episode"}, in3...),
			[]string{"SEARCH records USING INDEX sqlite_autoindex_records_1 (account_id=? AND entity=? AND entity_id=?)"}},
		{"registered", fmt.Sprintf(registeredEpisodesSQL, "?,?,?"), append([]any{account}, in3...),
			[]string{"SEARCH episodes USING COVERING INDEX sqlite_autoindex_episodes_1 (account_id=? AND episode_id=?)"}},
		{"catalog", fmt.Sprintf(catalogRowsSQL, "?,?,?"), append([]any{account}, in3...),
			[]string{"SEARCH catalog_items USING INDEX sqlite_autoindex_catalog_items_1 (account_id=? AND episode_id=?)"}},
		{"ranks", fmt.Sprintf(podcastRanksSQL, "?,?,?"), append([]any{account}, in3...),
			[]string{"SEARCH p USING INDEX sqlite_autoindex_podcasts_1 (account_id=? AND podcast_id=?)",
				"SEARCH o USING INDEX sqlite_autoindex_node_order_1 (account_id=? AND node_id=?)"}},
	} {
		p := plan(c.sql, c.args...)
		t.Logf("%s: %s", c.name, p)
		for _, w := range c.want {
			if !strings.Contains(p, w) {
				t.Errorf("%s: plan\n%s\nlacks %q", c.name, p, w)
			}
		}
		if strings.Contains(p, "SCAN") || strings.Contains(p, "TEMP B-TREE") {
			t.Errorf("%s: plan\n%s\nscans or sorts", c.name, p)
		}
	}
	// The capture's lookup of a node's podcasts uses its index.
	if p := plan(`SELECT 1 FROM podcasts WHERE account_id = ? AND creating_op = ?`, account, "n:1"); !strings.Contains(p, "podcasts_by_creating_op") {
		t.Errorf("a node's podcasts: plan %s", p)
	}
}

// ---- pages and batches

// The delta walk returns every change exactly once at 999, 1,000, 1,001 and
// 2,500 changes, and when a page ends inside a run of changes with one tick.
func TestViewChangesPagesAtTheirBoundaries(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	for _, n := range []int{999, 1000, 1001, 2500} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			account := createAccounts(t, s, 1)[0]
			want := keySet{}
			if err := s.Write(ctx, func(tx *WriteTx) error {
				for i := range n {
					want[ep(fmt.Sprintf("e:%05d", i))] = true
					if err := tx.Account(account).PutRecord(RecordRow{"episode", fmt.Sprintf("e:%05d", i), "state", `{}`, `[]`}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			pages := 0
			if err := s.Read(ctx, func(r *ReadTx) error {
				cur := ViewCursor{}
				for {
					changes, next, more, err := r.Account(account).ViewChangesSince(0, cur)
					if err != nil {
						return err
					}
					pages++
					rest := n - (pages-1)*ViewChangesPage
					if wantLen := min(rest, ViewChangesPage); len(changes) != wantLen || more != (rest > ViewChangesPage) {
						return fmt.Errorf("page %d: %d changes, more %v; want %d", pages, len(changes), more, wantLen)
					}
					if !more {
						return nil
					}
					cur = next
				}
			}); err != nil {
				t.Fatal(err)
			}
			if got := walkChanges(t, s, account, 0); !equalSets(got, want) {
				t.Fatalf("walked %d of %d changes", len(got), len(want))
			}
			// From a later base, only the later changes.
			if got := walkChanges(t, s, account, int64(n-3)); len(got) != 3 {
				t.Fatalf("above tick %d: %v", n-3, got)
			}
		})
	}
	t.Run("a page ending inside one tick", func(t *testing.T) {
		account := createAccounts(t, s, 1)[0]
		want := keySet{}
		if err := s.Write(ctx, func(tx *WriteTx) error {
			a := tx.Account(account)
			for _, p := range []string{"p:a", "p:b", "p:c"} { // three podcasts of one node
				want[pod(p)] = true
				if err := a.CreatePodcast(p, "n:shared"); err != nil {
					return err
				}
			}
			for i := range 998 {
				want[ep(fmt.Sprintf("e:%05d", i))] = true
				if err := a.PutRecord(RecordRow{"episode", fmt.Sprintf("e:%05d", i), "state", `{}`, `[]`}); err != nil {
					return err
				}
			}
			// Placing the node stamps its three podcasts at one tick, last.
			return a.InsertNode("n:shared", "fact", 1, nil)
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.Read(ctx, func(r *ReadTx) error {
			first, next, more, err := r.Account(account).ViewChangesSince(0, ViewCursor{})
			if err != nil {
				return err
			}
			if !more || len(first) != ViewChangesPage || first[len(first)-1].Tick != first[len(first)-2].Tick || first[len(first)-1].Entity != "podcast" {
				return fmt.Errorf("the first page ends %+v (more %v)", first[len(first)-2:], more)
			}
			second, _, more, err := r.Account(account).ViewChangesSince(0, next)
			if err != nil {
				return err
			}
			if more || len(second) != 1 || second[0].Tick != first[len(first)-1].Tick {
				return fmt.Errorf("the second page %+v (more %v)", second, more)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if got := walkChanges(t, s, account, 0); !equalSets(got, want) {
			t.Fatalf("walked %d of %d changes", len(got), len(want))
		}
	})
}

// The entity readers read in batches of 500 and return, for 499, 500, 501
// and 1,201 IDs (with unknown and repeated IDs), what the full readers
// return for them.
func TestViewEntityReadersReadEveryBatch(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	account := createAccounts(t, s, 1)[0]
	if err := s.Write(ctx, func(tx *WriteTx) error {
		a := tx.Account(account)
		for i := range 1201 {
			p, e := fmt.Sprintf("p:%04d", i), fmt.Sprintf("e:%04d", i)
			if err := a.CreatePodcast(p, "n:"+p); err != nil {
				return err
			}
			if i%3 != 0 { // the others' creating nodes have no rank
				if err := a.InsertNode("n:"+p, "fact", int64(i), nil); err != nil {
					return err
				}
			}
			if err := a.AddEpisode(e, p, `{}`, "n:x"); err != nil {
				return err
			}
			if err := a.PutCatalogItem(CatalogItem{EpisodeID: e, PodcastID: p, FirstNode: "n:x", PubDate: int64(i)}); err != nil {
				return err
			}
			for _, entity := range []string{"episode", "podcast"} {
				id := e
				if entity == "podcast" {
					id = p
				}
				if err := a.PutRecord(RecordRow{entity, id, "metadata", `{"description":"d","chapters":[],"title":"t"}`, `[]`}); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{499, 500, 501, 1201} {
		set := keySet{}
		for i := range n {
			set[ep(fmt.Sprintf("e:%04d", i))] = true
			set[pod(fmt.Sprintf("p:%04d", i))] = true
		}
		set[ep("e:missing")], set[pod("p:missing")] = true, true
		checkEntityReaders(t, s, account, set)
		// The last of the batch is there.
		if err := s.Read(ctx, func(r *ReadTx) error {
			last := fmt.Sprintf("e:%04d", n-1)
			ids := []string{}
			for i := range n {
				ids = append(ids, fmt.Sprintf("e:%04d", i))
			}
			ids = append(ids, ids[0], "e:missing")
			reg, err := r.Account(account).RegisteredEpisodes(ids)
			if err != nil {
				return err
			}
			cat, err := r.Account(account).CatalogRowsFor(ids)
			if err != nil {
				return err
			}
			recs, err := r.Account(account).ViewEntityRecords("episode", ids)
			if err != nil {
				return err
			}
			if len(reg) != n || reg[n-1] != last || len(cat) != n || cat[n-1].EpisodeID != last || len(recs) != n || recs[n-1].EntityID != last {
				return fmt.Errorf("%d IDs: %d registered, %d catalog rows, %d records", n, len(reg), len(cat), len(recs))
			}
			if strings.Contains(recs[0].Value, "description") || !strings.Contains(recs[0].Value, "title") {
				return fmt.Errorf("an episode's metadata as list views read it: %s", recs[0].Value)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// ---- source rules

// The server never vacuums in place (VACUUM INTO only: an in-place VACUUM
// cannot run in a transaction, and would have to raise every account's
// generation offline; docs/ARCHITECTURE.md), and never writes a view input
// table with REPLACE (whose implicit deletes fire no trigger).
func TestNoInPlaceVacuumOrReplaceOfViewInputs(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	vacuum := regexp.MustCompile(`(?i)\bvacuum\b`)
	into := regexp.MustCompile(`(?i)^\s+into\b`)
	replace := regexp.MustCompile(`(?i)(\bor\s+replace\s+into|\breplace\s+into|\bupdate\s+or\s+replace)\s+(main\.)?(records|catalog_items|episodes|podcasts|node_order)\b`)
	check := func(where, text string) {
		for _, m := range vacuum.FindAllStringIndex(text, -1) {
			if !into.MatchString(text[m[1]:]) {
				t.Errorf("%s: an in-place VACUUM: %q", where, text)
			}
		}
		if replace.MatchString(text) {
			t.Errorf("%s: REPLACE on a view input table: %q", where, text)
		}
	}
	files := 0
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			switch {
			case d.IsDir() && (d.Name() == "testdata" || d.Name() == "dist"):
				return filepath.SkipDir
			case strings.HasSuffix(path, ".sql"):
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				files++
				check(path, string(data))
			case strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go"):
				f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
				if err != nil {
					return err
				}
				files++
				ast.Inspect(f, func(n ast.Node) bool {
					if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
						if s, err := strconv.Unquote(lit.Value); err == nil {
							check(path, s)
						}
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if files < 100 {
		t.Fatalf("only %d files checked", files)
	}
}
