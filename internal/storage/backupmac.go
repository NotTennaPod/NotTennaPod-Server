package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// The backup MAC. A backup ends with a trailer that authenticates the
// whole file under a key derived from the server's receipt key, which the key
// bundle holds and no backup does. Version 1 (.db, written by earlier
// versions, still restored):
//
//	<the SQLite database> <trailer JSON> <its length, 4 bytes big-endian> "NTPBMAC1"
//
// SQLite reads only the pages its header counts, so the file still opens as a
// database. A restore checks the MAC with the bundle's keys before it uses the
// file, and removes the trailer from its staged copy.
//
// Version 2 (.db.gz, PROTOCOL §11.2) is the database compressed as one gzip
// member, then an empty gzip member whose comment is the trailer (backupgz.go).

const (
	backupMagic     = "NTPBMAC1"
	backupMACFormat = "nottennapod-backup-mac"
	maxTrailerJSON  = 4096
)

// errUnsigned is a file without a backup trailer.
var errUnsigned = errors.New("it carries no MAC")

type backupTrailer struct {
	Format   string `json:"format"`
	Version  int    `json:"version"`
	ServerID string `json:"server_id"`
	KeyID    string `json:"key_id"`
	Size     int64  `json:"size"` // the database's bytes, before the trailer
	MAC      string `json:"mac"`  // base64 HMAC-SHA256
}

// backupMAC is HMAC-SHA256, under a key derived from the receipt key, of the
// trailer's fields and the size bytes of the database read from r.
func backupMAC(ctx context.Context, receiptKey []byte, serverID, keyID string, size int64, r io.Reader) ([]byte, error) {
	m := hmac.New(sha256.New, backupMACKey(receiptKey))
	m.Write([]byte(backupMACFormat + "\x001\x00" + serverID + "\x00" + keyID + "\x00" + strconv.FormatInt(size, 10) + "\x00"))
	if err := readAll(ctx, m, r, size); err != nil {
		return nil, err
	}
	return m.Sum(nil), nil
}

// backupMACKey is the key both versions' MACs use, derived from a receipt key.
func backupMACKey(receiptKey []byte) []byte {
	k := hmac.New(sha256.New, receiptKey)
	k.Write([]byte("nottennapod backup MAC key"))
	return k.Sum(nil)
}

// readAll writes exactly n bytes of r to w, a mebibyte at a time, and stops
// when ctx ends.
func readAll(ctx context.Context, w io.Writer, r io.Reader, n int64) error {
	buf := make([]byte, mib)
	for n > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		k, err := io.ReadFull(r, buf[:min(int64(len(buf)), n)])
		if _, werr := w.Write(buf[:k]); werr != nil {
			return werr
		}
		n -= int64(k)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
	}
	return nil
}

// readBackupTrailer reads the trailer of a file of fileSize bytes; it returns
// errUnsigned when there is none.
func readBackupTrailer(r io.ReaderAt, fileSize int64) (backupTrailer, error) {
	var t backupTrailer
	tail := make([]byte, 4+len(backupMagic))
	if fileSize < int64(len(tail)) {
		return t, errUnsigned
	}
	if _, err := r.ReadAt(tail, fileSize-int64(len(tail))); err != nil {
		return t, err
	}
	if string(tail[4:]) != backupMagic {
		return t, errUnsigned
	}
	n := int64(binary.BigEndian.Uint32(tail))
	if n > maxTrailerJSON || n+int64(len(tail)) > fileSize {
		return t, fmt.Errorf("%w: its trailer is damaged", errUnsigned)
	}
	j := make([]byte, n)
	if _, err := r.ReadAt(j, fileSize-int64(len(tail))-n); err != nil {
		return t, err
	}
	dec := json.NewDecoder(bytes.NewReader(j))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&t); err != nil || t.Format != backupMACFormat || t.Version != 1 || t.ServerID == "" || t.KeyID == "" ||
		t.Size != fileSize-int64(len(tail))-n {
		return backupTrailer{}, fmt.Errorf("%w: its trailer is damaged", errUnsigned)
	}
	return t, nil
}

// checkBackupMAC verifies the trailer's MAC over the file in r with the key
// it names.
func checkBackupMAC(ctx context.Context, r io.ReaderAt, t backupTrailer, keyByID map[string][]byte) error {
	key, ok := keyByID[t.KeyID]
	if !ok {
		return fmt.Errorf("the backup is signed with receipt key %s, which the key bundle does not hold", t.KeyID)
	}
	want, err := base64.StdEncoding.DecodeString(t.MAC)
	if err != nil {
		return errors.New("the backup's MAC is damaged")
	}
	got, err := backupMAC(ctx, key, t.ServerID, t.KeyID, t.Size, io.NewSectionReader(r, 0, t.Size))
	if err != nil {
		return err
	}
	if !hmac.Equal(got, want) {
		return errors.New("the backup has been changed since it was made: its MAC does not verify with the key bundle")
	}
	return nil
}

// openSignedBackup opens a backup file and reads its trailer.
func openSignedBackup(path string) (*os.File, backupTrailer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, backupTrailer{}, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, backupTrailer{}, err
	}
	if !fi.Mode().IsRegular() {
		f.Close()
		return nil, backupTrailer{}, fmt.Errorf("%s is not a regular file", path)
	}
	t, err := readBackupTrailer(f, fi.Size())
	if err != nil {
		f.Close()
		return nil, backupTrailer{}, err
	}
	return f, t, nil
}

// unsignStagedCopy checks the staged copy's MAC with the given keys and cuts
// the trailer off, leaving the bare database.
func unsignStagedCopy(ctx context.Context, path, serverID string, keyByID map[string][]byte) error {
	f, err := private.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	t, err := readBackupTrailer(f, fi.Size())
	if err != nil {
		return err
	}
	if t.ServerID != serverID {
		return fmt.Errorf("the staged copy belongs to server %s, not %s", t.ServerID, serverID)
	}
	if err := checkBackupMAC(ctx, f, t, keyByID); err != nil {
		return err
	}
	if err := f.Truncate(t.Size); err != nil {
		return err
	}
	return f.Sync()
}
