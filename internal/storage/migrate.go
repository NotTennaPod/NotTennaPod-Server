package storage

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type migration struct {
	version  int
	name     string
	sql      string
	checksum string
}

var embeddedMigrations = loadMigrations()

// dataSteps are migrations' Go parts, run right after their SQL in the same
// transaction, for work SQL cannot do.
var dataSteps = map[string]func(*WriteTx) error{
	"0010_place_archived_facts.sql": placeArchivedFacts,
}

func loadMigrations() []migration {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		panic(err)
	}
	var out []migration
	for _, e := range entries {
		data, err := migrationFiles.ReadFile("migrations/" + e.Name())
		if err != nil {
			panic(err)
		}
		out = append(out, newMigration(e.Name(), string(data)))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	for i, m := range out {
		if m.version != i+1 {
			panic(fmt.Sprintf("storage: migrations must be numbered 1, 2, …: %s", m.name))
		}
	}
	return out
}

// newMigration parses "NNNN_description.sql".
func newMigration(name, sql string) migration {
	prefix, _, ok := strings.Cut(name, "_")
	version, err := strconv.Atoi(prefix)
	if !ok || err != nil || version < 1 {
		panic("storage: bad migration name " + name)
	}
	sum := sha256.Sum256([]byte(sql))
	return migration{version: version, name: name, sql: sql, checksum: hex.EncodeToString(sum[:])}
}

// migrate verifies applied migrations and applies pending ones, all in one write
// transaction: a failure leaves the previous schema intact (PROTOCOL §14).
func (s *Store) migrate(ctx context.Context, migrations []migration) error {
	return s.Write(ctx, func(tx *WriteTx) error {
		if _, err := tx.tx.ExecContext(ctx, schemaMigrationsDDL); err != nil {
			return fmt.Errorf("storage: creating schema_migrations: %w", err)
		}
		rows, err := tx.tx.QueryContext(ctx, "SELECT version, checksum FROM schema_migrations ORDER BY version")
		if err != nil {
			return err
		}
		applied := map[int]string{}
		for rows.Next() {
			var v int
			var sum string
			if err := rows.Scan(&v, &sum); err != nil {
				rows.Close()
				return err
			}
			applied[v] = sum
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		known := map[int]migration{}
		for _, m := range migrations {
			known[m.version] = m
		}
		for v, sum := range applied {
			m, ok := known[v]
			if !ok {
				return fmt.Errorf("%w: database has migration %d", ErrSchemaTooNew, v)
			}
			if m.checksum != sum {
				return fmt.Errorf("%w: %s", ErrMigrationEdited, m.name)
			}
		}
		for _, m := range migrations {
			if _, done := applied[m.version]; done {
				continue
			}
			if _, err := tx.tx.ExecContext(ctx, m.sql); err != nil {
				return fmt.Errorf("storage: migration %s failed: %w", m.name, err)
			}
			if step := dataSteps[m.name]; step != nil {
				if err := step(tx); err != nil {
					return fmt.Errorf("storage: migration %s failed: %w", m.name, err)
				}
			}
			if s.hooks.duringMigration != nil {
				if err := s.hooks.duringMigration(m.version); err != nil {
					return err
				}
			}
			if _, err := tx.tx.ExecContext(ctx, "INSERT INTO schema_migrations (version, checksum, applied_at) VALUES (?, ?, ?)",
				m.version, m.checksum, s.nowMillis()); err != nil {
				return err
			}
		}
		return nil
	})
}

// SchemaVersion returns the highest applied migration.
func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	var v int
	err := s.reader.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&v)
	return v, err
}
