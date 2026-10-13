package media

import (
	"os"
	"path/filepath"
)

// databaseSize is the database with its WAL, for the free-space headroom.
func databaseSize(dir string) int64 {
	if dir == "" {
		return 0
	}
	var n int64
	for _, name := range []string{"nottennapod.db", "nottennapod.db-wal"} {
		if st, err := os.Stat(filepath.Join(dir, name)); err == nil {
			n += st.Size()
		}
	}
	return n
}
