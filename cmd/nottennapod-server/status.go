package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/backupsched"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/listen"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/refresh"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/version"
)

// serviceInfo assembles the service page (PROTOCOL §14): version and
// uptime, the data directory and database, backups (daily, on-demand and the
// databases restores kept) and the keys, the refresh schedule, the listen
// address, and warnings. It holds no
// credentials; backup errors name only files and folders.
type serviceInfo struct {
	store     *storage.Store
	backups   *backupsched.Scheduler // nil when daily backups are off
	backupDir string                 // --backup-dir; default <data-dir>/backups
	refresher *refresh.Refresher
	imports   *importer.Runner
	started   time.Time
	listen    string
	https     bool
	cert      *listen.Certificate

	mu       sync.Mutex
	warnings []string
}

func (si *serviceInfo) addWarning(w string) {
	si.mu.Lock()
	si.warnings = append(si.warnings, w)
	si.mu.Unlock()
}

func (si *serviceInfo) report(ctx context.Context) (any, error) {
	now := time.Now()
	schema, err := si.store.SchemaVersion(ctx)
	if err != nil {
		return nil, err
	}
	var dbBytes int64
	for _, f := range []string{"nottennapod.db", "nottennapod.db-wal"} {
		if fi, err := os.Stat(filepath.Join(si.store.Dir(), f)); err == nil {
			dbBytes += fi.Size()
		}
	}
	si.mu.Lock()
	warnings := append([]string{}, si.warnings...)
	si.mu.Unlock()
	out := map[string]any{
		"version":          version.Version,
		"started_at":       si.started.UnixMilli(),
		"uptime_seconds":   int64(now.Sub(si.started).Seconds()),
		"data_dir":         si.store.Dir(),
		"database_bytes":   dbBytes,
		"schema_version":   schema,
		"keys_exported_at": si.store.KeysExportedAt(),
		"listen":           map[string]any{"address": si.listen, "https": si.https},
	}
	if si.store.KeysExportedAt() == 0 {
		warnings = append(warnings, "keys_not_exported")
	}
	if si.cert != nil {
		out["listen"].(map[string]any)["certificate_expires_at"] = si.cert.NotAfter().UnixMilli()
		if w := si.cert.ExpiryWarning(now); w != "" {
			warnings = append(warnings, w)
		}
	}
	if si.backups != nil {
		st := si.backups.Status()
		out["backups"] = map[string]any{
			"dir": st.Dir, "next_due": st.NextDue, "last_success": st.LastSuccess, "last_file": st.LastFile, "last_bytes": st.LastSize,
			"kept": st.Kept, "used_bytes": st.Used, "running": st.Running, "failures": st.Failures, "last_error": st.LastError,
			"retry_at": st.RetryAt, "write_check": st.WriteCheck,
		}
	} else {
		out["backups"] = nil
	}
	// The copies never deleted automatically, also with daily backups off:
	// on-demand backups and the databases restores kept (PROTOCOL §14).
	backupDir := si.backupDir
	if si.backups != nil {
		backupDir = si.backups.Status().Dir
	} else if backupDir == "" {
		backupDir = filepath.Join(si.store.Dir(), "backups")
	}
	usage := backupsched.GetUsage(backupDir, si.store.Dir())
	out["manual_backups"] = usage.Manual
	out["pre_restore"] = usage.PreRestore
	if si.refresher != nil {
		if rs, err := si.refresher.ScheduleStatus(ctx); err == nil {
			out["refresh"] = rs
		}
	}
	// Files of deleted or finished imports that could not be removed (they
	// can hold feed credentials): the runner retries them.
	if si.imports != nil {
		if paths := si.imports.PendingCleanup(); len(paths) > 0 {
			out["import_cleanup"] = map[string]any{"pending": len(paths), "paths": paths}
			warnings = append(warnings, fmt.Sprintf("files of deleted imports that could not be removed: %d (%s)", len(paths),
				strings.Join(paths, ", ")))
		}
	}
	out["warnings"] = warnings
	return out, nil
}
