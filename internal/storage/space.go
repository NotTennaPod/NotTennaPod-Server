package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Free space (PROTOCOL §11.2): every backup and restore path checks it before
// it writes and keeps a reserve free.

const (
	mib = 1 << 20
	// restoreReserve is the free space a restore always leaves in the data
	// folder.
	restoreReserve = 1 << 30
	// sqliteSlack is the allowance for SQLite's own small writes (the schema,
	// the transition's metadata, journal and shared-memory files).
	sqliteSlack = 64 * mib
)

// FreeBytes is the space available to this user on dir's file system.
func FreeBytes(dir string) (int64, error) { return freeBytes(dir) }

// freeNear is the free space where dir is or will be made: on the file system
// of its nearest existing folder.
func freeNear(free func(string) (int64, error), dir string) (int64, error) {
	for d := dir; ; {
		if _, err := os.Stat(d); err == nil {
			return free(d)
		} else if !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		parent := filepath.Dir(d)
		if parent == d {
			return free(d)
		}
		d = parent
	}
}

// sqliteAllowance is the space SQLite may write while it migrates,
// transitions and checkpoints a staged database of size bytes (PROTOCOL
// §11.4): 2.5 times the database plus 64 MiB when a migration is pending,
// 64 MiB otherwise. This comment is the formula's one justification.
//
// With no migration pending, only the transition writes, a few pages.
// Migrations run in one transaction. The costliest kind rebuilds a table
// (0024 copies every stored feed response into a new table): each new page
// is written once into the log and again into the file when the log is
// checkpointed, while the old pages stay allocated until the log is gone, so
// a database made almost entirely of that table needs twice its size, the
// model's bound. Measured (TestRestoreMigrationSpaceFitsTheAllowance): 1.89
// times for 0024 on a database almost all stored responses, 1.63 times for
// the chain from schema 20 (0021's table copies, then 0022 to 0024). Half the
// database again covers the new indexes, columns and page splits around
// such a rebuild, a quarter over the model. Signed backups exist only from
// schema 24 on, so a restore runs only the migrations added since; a new
// migration that rebuilds more than its own table must be measured against
// this allowance in that test.
func sqliteAllowance(size int64, migrating bool) int64 {
	if !migrating {
		return sqliteSlack
	}
	return 2*size + size/2 + sqliteSlack
}

// spaceError is a refusal for lack of free space; its text is for people.
type spaceError struct{ msg string }

func (e *spaceError) Error() string { return e.msg }

// ErrNoSpace matches every refusal for lack of free space (errors.Is).
var ErrNoSpace = errors.New("not enough free space")

func (e *spaceError) Is(target error) bool { return target == ErrNoSpace }

// NoSpace is a refusal for lack of free space (errors.Is(err, ErrNoSpace)).
func NoSpace(format string, args ...any) error { return &spaceError{msg: fmt.Sprintf(format, args...)} }

// mb is n in whole megabytes, rounded up, for messages.
func mb(n int64) int64 { return (n + mib - 1) / mib }
