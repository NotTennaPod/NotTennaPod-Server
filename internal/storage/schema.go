package storage

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// The schema check. A database's schema must be exactly the one its migrations
// create: the same tables with the same columns, the same indexes, and no
// trigger, view or other object of its own. SQLite runs a database's own
// triggers whatever trusted_schema says, so a backup that brought one could
// undo the restore transition (an AFTER DELETE trigger on auth_tokens put
// revoked tokens back).

// schemaMigrationsDDL creates the migrations table; the reference schema
// creates it with the same text.
const schemaMigrationsDDL = `CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY, checksum TEXT NOT NULL, applied_at INTEGER NOT NULL) STRICT`

// schemaObject is one row of sqlite_schema, without its root page.
type schemaObject struct{ typ, name, table, sql string }

func (o schemaObject) String() string { return o.typ + " " + o.name }

// statisticsTables are SQLite's statistics tables, which ANALYZE creates and
// only the query planner reads, with the definitions SQLite gives them.
var statisticsTables = map[string]string{
	"sqlite_stat1": "CREATE TABLE sqlite_stat1(tbl,idx,stat)",
	"sqlite_stat4": "CREATE TABLE sqlite_stat4(tbl,idx,neq,nlt,ndlt,sample)",
}

// readSchema lists a database's objects. Only SQLite's statistics tables,
// exactly as ANALYZE defines them, are left out: any other object with a
// reserved name, a trigger or a view above all, is listed and so refused
// (a database edited through writable_schema can name anything sqlite_stat…).
func readSchema(ctx context.Context, q rowQuerier) (map[string]schemaObject, error) {
	rows, err := q.QueryContext(ctx, `SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]schemaObject{}
	for rows.Next() {
		var o schemaObject
		if err := rows.Scan(&o.typ, &o.name, &o.table, &o.sql); err != nil {
			return nil, err
		}
		if def, ok := statisticsTables[o.name]; ok && o.typ == "table" && o.table == o.name && o.sql == def {
			continue
		}
		out[o.typ+"\x00"+o.name] = o
	}
	return out, rows.Err()
}

// referenceSchemas holds the schema each migration chain creates, by the
// chain's checksums.
var referenceSchemas sync.Map

// referenceSchema is the schema the first version migrations create, built
// once in an empty in-memory database.
func referenceSchema(ctx context.Context, migrations []migration, version int) (map[string]schemaObject, error) {
	var key strings.Builder
	var chain []migration
	for _, m := range migrations {
		if m.version <= version {
			chain = append(chain, m)
			key.WriteString(m.checksum)
		}
	}
	if len(chain) != version {
		return nil, fmt.Errorf("%w: schema %d is not one this server knows", ErrIntegrity, version)
	}
	if v, ok := referenceSchemas.Load(key.String()); ok {
		return v.(map[string]schemaObject), nil
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if version > 0 {
		if _, err := db.ExecContext(ctx, schemaMigrationsDDL); err != nil {
			return nil, err
		}
	}
	for _, m := range chain {
		if _, err := db.ExecContext(ctx, m.sql); err != nil {
			return nil, fmt.Errorf("storage: building the reference schema: migration %s: %w", m.name, err)
		}
	}
	out, err := readSchema(ctx, db)
	if err != nil {
		return nil, err
	}
	referenceSchemas.Store(key.String(), out)
	return out, nil
}

// checkSchema compares a database's schema and recorded migrations with what
// its migrations create; it returns ErrIntegrity naming the differences. A
// database without the migrations table must be empty (a new database).
func checkSchema(ctx context.Context, q rowQuerier, migrations []migration) error {
	version, diffs, err := compareSchema(ctx, q, migrations)
	if err != nil {
		return err
	}
	var problems []string
	for _, d := range diffs {
		switch d.kind {
		case schemaOwn:
			problems = append(problems, "has "+d.object.String()+" of its own")
		case schemaDifferent:
			problems = append(problems, "has a different "+d.object.String())
		case schemaLacks:
			problems = append(problems, "lacks "+d.object.String())
		}
	}
	if len(problems) > 0 {
		slices.Sort(problems)
		if len(problems) > 5 {
			problems = append(problems[:5], fmt.Sprintf("and %d more differences", len(problems)-5))
		}
		return fmt.Errorf("%w: the schema is not the one migration %d creates: it %s", ErrIntegrity, version, strings.Join(problems, "; it "))
	}
	return nil
}

// A schemaDifference is an object a database has of its own, has with other
// SQL, or lacks, against the schema its recorded migrations create.
type schemaDifference struct {
	kind   int // schemaOwn, schemaDifferent or schemaLacks
	object schemaObject
}

const (
	schemaOwn       = iota // the database's object, which the migrations do not create
	schemaDifferent        // the database's object, whose SQL is not the migrations'
	schemaLacks            // the migrations' object, which the database lacks
)

// compareSchema lists the differences between a database's schema and the
// one its recorded migrations create (checkSchema), and that version. An
// error is a database checkSchema refuses whatever the differences are.
func compareSchema(ctx context.Context, q rowQuerier, migrations []migration) (int, []schemaDifference, error) {
	have, err := readSchema(ctx, q)
	if err != nil {
		return 0, nil, fmt.Errorf("%w: reading the schema: %v", ErrIntegrity, err)
	}
	version := 0
	if _, ok := have["table\x00schema_migrations"]; ok {
		rows, err := q.QueryContext(ctx, `SELECT version, checksum FROM schema_migrations ORDER BY version`)
		if err != nil {
			return 0, nil, fmt.Errorf("%w: reading the migrations: %v", ErrIntegrity, err)
		}
		known := map[int]migration{}
		for _, m := range migrations {
			known[m.version] = m
		}
		for rows.Next() {
			var v int
			var sum string
			if err := rows.Scan(&v, &sum); err != nil {
				rows.Close()
				return 0, nil, err
			}
			m, ok := known[v]
			switch {
			case !ok:
				rows.Close()
				return 0, nil, fmt.Errorf("%w: database has migration %d", ErrSchemaTooNew, v)
			case sum != m.checksum:
				rows.Close()
				return 0, nil, fmt.Errorf("%w: %s", ErrMigrationEdited, m.name)
			case v != version+1:
				rows.Close()
				return 0, nil, fmt.Errorf("%w: the database's recorded migrations skip from %d to %d", ErrIntegrity, version, v)
			}
			version = v
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return 0, nil, err
		}
	}
	want, err := referenceSchema(ctx, migrations, version)
	if err != nil {
		return 0, nil, err
	}
	var diffs []schemaDifference
	for k, o := range have {
		w, ok := want[k]
		switch {
		case !ok:
			diffs = append(diffs, schemaDifference{schemaOwn, o})
		case w != o:
			diffs = append(diffs, schemaDifference{schemaDifferent, o})
		}
	}
	for k, o := range want {
		if _, ok := have[k]; !ok {
			diffs = append(diffs, schemaDifference{schemaLacks, o})
		}
	}
	return version, diffs, nil
}
