package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // the database driver, for reading the server ID
)

// databaseName is storage's database file.
const databaseName = "nottennapod.db"

// CheckDataDir accepts a data directory for a service (PROTOCOL §14): one that holds
// this server's database, recognized by the server ID in its server_meta, or,
// with newServer, one that is absent or empty. It never creates anything.
func CheckDataDir(ctx context.Context, dir string, newServer bool) error {
	entries, err := os.ReadDir(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if newServer {
			return nil
		}
		return fmt.Errorf("service: %s does not exist; give --new-server to start a new server there", dir)
	case err != nil:
		return err
	}
	db := filepath.Join(dir, databaseName)
	if _, err := os.Stat(db); errors.Is(err, os.ErrNotExist) {
		if newServer && len(entries) == 0 {
			return nil
		}
		if newServer {
			return fmt.Errorf("service: %s is not empty and holds no server database; a new server needs an empty or absent directory", dir)
		}
		return fmt.Errorf("service: %s holds no server database (%s); check --data-dir, or give --new-server with an empty directory", dir, databaseName)
	}
	if newServer {
		return fmt.Errorf("service: %s already holds a server database; leave out --new-server to keep running it", dir)
	}
	id, err := serverID(ctx, db)
	if err != nil {
		return fmt.Errorf("service: %s is not a NotTennaPod server database: %w", db, err)
	}
	if id == "" {
		return fmt.Errorf("service: %s has no server ID", db)
	}
	return nil
}

// serverID reads server_meta's server ID without writing (the server may be running).
func serverID(ctx context.Context, path string) (string, error) {
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return "", err
	}
	defer db.Close()
	var id string
	err = db.QueryRowContext(ctx, "SELECT value FROM server_meta WHERE key = 'server_id'").Scan(&id)
	return id, err
}
