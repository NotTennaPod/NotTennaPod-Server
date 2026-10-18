package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Version 1 backups (.db, the database followed by its trailer) are no longer
// written, and are still restored: these helpers write them for the tests,
// and every restore test runs with both formats.

// signBackup appends a version 1 trailer to the database in f, reading and
// writing through f (the version 1 writer).
func signBackup(f *os.File, serverID string, key ReceiptKey) error {
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	mac, err := backupMAC(context.Background(), key.Secret, serverID, key.ID, size, io.NewSectionReader(f, 0, size))
	if err != nil {
		return fmt.Errorf("storage: signing the backup: %w", err)
	}
	j, err := json.Marshal(backupTrailer{Format: backupMACFormat, Version: 1, ServerID: serverID, KeyID: key.ID, Size: size,
		MAC: base64.StdEncoding.EncodeToString(mac)})
	if err != nil {
		return err
	}
	j = binary.BigEndian.AppendUint32(j, uint32(len(j)))
	if _, err := f.WriteAt(append(j, backupMagic...), size); err != nil {
		return fmt.Errorf("storage: signing the backup: %w", err)
	}
	return nil
}

// forEachFormat runs f for version 1 and version 2 backups.
func forEachFormat(t *testing.T, f func(t *testing.T, format int)) {
	for _, format := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", format), func(t *testing.T) { f(t, format) })
	}
}

// rawDatabaseOf is the database a backup of either format holds.
func rawDatabaseOf(t *testing.T, backup string) []byte {
	t.Helper()
	data, err := os.ReadFile(backup)
	if err != nil {
		t.Fatal(err)
	}
	switch backupFormat(bytes.NewReader(data), int64(len(data))) {
	case 1:
		tr, err := readBackupTrailer(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		return data[:tr.Size]
	case 2:
		zr, err := gzip.NewReader(bytes.NewReader(data)) // multistream: the trailer member adds nothing
		if err != nil {
			t.Fatal(err)
		}
		db, err := io.ReadAll(zr)
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	t.Fatalf("%s is not a signed backup", backup)
	return nil
}

// writeSigned writes db to path as a backup of the given format signed with
// key, as the server's writer does.
func writeSigned(t *testing.T, path string, db []byte, serverID string, key ReceiptKey, format int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	switch format {
	case 1:
		if _, err := f.Write(db); err != nil {
			t.Fatal(err)
		}
		if err := signBackup(f, serverID, key); err != nil {
			t.Fatal(err)
		}
	case 2:
		tr, err := compressBackup(context.Background(), bytes.NewReader(db), f, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := signBackupV2(context.Background(), f, tr, serverID, key); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("format %d", format)
	}
}

// asFormat is a copy of backup in the given format, signed again with the
// key of the server whose data directory is dir; a version 1 copy is named
// .db, as that writer named them.
func asFormat(t *testing.T, backup, dir string, format int) string {
	t.Helper()
	db := rawDatabaseOf(t, backup)
	serverID, ks, _, err := readReceiptKeysFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	name := strings.TrimSuffix(filepath.Base(backup), ".gz")
	if format == 2 {
		name += ".gz"
	}
	path := filepath.Join(t.TempDir(), name)
	writeSigned(t, path, db, serverID, ks[0], format)
	return path
}

// A version 1 backup written by the helper has the old layout exactly, and a
// version 2 one is the server's own: both verify with the server's keys.
func TestBackupHelpersWriteBothFormats(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	s := openTest(t, config{dataDir: d})
	createAccounts(t, s, 2)
	info, err := s.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []int{1, 2} {
		p := asFormat(t, info.Path, d, format)
		if err := s.VerifyBackup(ctx, p, nil); err != nil {
			t.Fatalf("v%d: %v", format, err)
		}
		if !bytes.Equal(rawDatabaseOf(t, p), rawDatabaseOf(t, info.Path)) {
			t.Fatalf("v%d holds another database", format)
		}
	}
}

// withOldTrailer is the database at path in backup's format with backup's
// trailer, unchanged: a backup changed by someone without the key.
func withOldTrailer(t *testing.T, backup, path string) string {
	t.Helper()
	data, err := os.ReadFile(backup)
	if err != nil {
		t.Fatal(err)
	}
	db, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []byte
	switch backupFormat(bytes.NewReader(data), int64(len(data))) {
	case 1:
		tr, _ := readBackupTrailer(bytes.NewReader(data), int64(len(data)))
		out = append(db, data[tr.Size:]...)
	case 2:
		tr, _ := readTrailerV2(bytes.NewReader(data), int64(len(data)))
		var b bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&b, gzipLevel)
		zw.Write(db)
		zw.Close()
		out = append(b.Bytes(), data[tr.Size:]...)
	}
	changed := filepath.Join(t.TempDir(), filepath.Base(backup))
	if err := os.WriteFile(changed, out, 0o600); err != nil {
		t.Fatal(err)
	}
	return changed
}
