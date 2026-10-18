package api

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/backupsched"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// POST /v1/admin/backup (PROTOCOL §11): an administrator's web session asks the
// running server for a verified, compressed backup in the backup directory
// (--backup-dir, mode 0700), kept free-space safe like the daily ones
// (backupsched.Run). The answer names the file in that directory, its size and
// when the copy was taken; too little free space is a 507 that says so. One
// backup runs at a time: the store refuses a second one from any entry point,
// and this handler refuses at once while its own is running.

var errBackupRunning = fail(http.StatusConflict, "backup_running", "A backup is already running; try again when it has finished.")

func (req *request) adminBackup() (int, any, error) {
	if err := req.decodeEmpty(); err != nil {
		return 0, nil, err
	}
	if !req.s.backupBusy.CompareAndSwap(false, true) {
		return 0, nil, errBackupRunning
	}
	defer req.s.backupBusy.Store(false)
	if req.s.cfg.duringBackup != nil {
		req.s.cfg.duringBackup()
	}
	// The administrator is checked again just before the backup starts, so a
	// demotion that landed after authentication wins.
	if err := req.checkCaller(); err != nil {
		return 0, nil, err
	}
	info, err := backupsched.Manual(req.ctx, req.s.cfg.Store, req.s.cfg.BackupDir, backupsched.Guard{Free: req.s.cfg.freeBytes})
	switch {
	case errors.Is(err, storage.ErrBackupRunning):
		return 0, nil, errBackupRunning
	case errors.Is(err, storage.ErrNoSpace):
		return 0, nil, fail(http.StatusInsufficientStorage, "insufficient_space", "The backup was not made: "+err.Error()+". Older backups are kept.")
	case errors.Is(err, context.DeadlineExceeded) && req.ctx.Err() == nil:
		return 0, nil, fail(http.StatusServiceUnavailable, "backup_failed", "The backup was not made: "+err.Error()+". Older backups are kept.")
	case err != nil:
		return 0, nil, err
	}
	return http.StatusOK, map[string]any{"file": filepath.Base(info.Path), "size": info.Size, "taken_at": info.TakenAt}, nil
}
