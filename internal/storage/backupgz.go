package storage

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// Version 2 backups (PROTOCOL §11.2): the marked, verified database as one
// gzip member (level 6), then the trailer, an empty gzip member whose comment
// holds the trailer JSON, its length and the magic:
//
//	1f 8b 08 10 00000000 00 ff  <JSON> <8 lowercase hex: its length> "NTPBMAC2" 00  03 00  00000000 00000000
//
// gunzip, Python's gzip and Go's multistream reader return exactly the
// database. A reader takes nothing from the file but this fixed framing
// before the MAC over [0, size) verifies, and decodes nothing before then:
// the decoder (decompress) accepts only an authentic, which only authenticate
// makes, after checking the MAC.

const (
	backupMagicV2  = "NTPBMAC2"
	backupGzipName = "gzip"
	maxDatabase    = 1 << 42
	gzipLevel      = 6
)

// spaceChunk is how much may be written between free-space checks (tests
// make it smaller).
var spaceChunk int64 = 32 * mib

var (
	// trailerHead is the trailer member's header: gzip, deflate, a comment, no
	// time, no extra flags, unknown system.
	trailerHead = []byte{0x1f, 0x8b, 0x08, 0x10, 0, 0, 0, 0, 0, 0xff}
	// trailerTail ends the trailer: the magic, the comment's NUL, an empty
	// final deflate block, and the CRC and size of no bytes.
	trailerTail = append([]byte(backupMagicV2), 0, 0x03, 0x00, 0, 0, 0, 0, 0, 0, 0, 0)
)

// decompressStarted counts every decoding of a backup (tests: nothing is ever
// decoded before its MAC verifies).
var decompressStarted atomic.Int64

// BackupsDecoded reports how many times this process has started decoding a
// compressed backup. Tests use it to show that a backup whose MAC does not
// verify is never decoded.
func BackupsDecoded() int64 { return decompressStarted.Load() }

type trailerV2 struct {
	Format         string `json:"format"`
	Version        int    `json:"version"`
	ServerID       string `json:"server_id"`
	KeyID          string `json:"key_id"`
	Size           int64  `json:"size"` // the compressed database's bytes, before the trailer
	Compression    string `json:"compression"`
	DatabaseSize   int64  `json:"database_size"`
	DatabaseSHA256 string `json:"database_sha256"`
	MAC            string `json:"mac"` // base64 HMAC-SHA256
}

// macInputV2 is what the MAC covers before the compressed bytes: every
// trailer field a reader uses.
func macInputV2(t trailerV2) []byte {
	return []byte(backupMACFormat + "\x002\x00" + t.ServerID + "\x00" + t.KeyID + "\x00" + strconv.FormatInt(t.Size, 10) + "\x00" +
		backupGzipName + "\x00" + strconv.FormatInt(t.DatabaseSize, 10) + "\x00" + t.DatabaseSHA256 + "\x00")
}

// macV2 is the MAC of t's fields and the t.Size bytes of r.
func macV2(ctx context.Context, receiptKey []byte, t trailerV2, r io.Reader) ([]byte, error) {
	m := hmac.New(sha256.New, backupMACKey(receiptKey))
	m.Write(macInputV2(t))
	if err := readAll(ctx, m, r, t.Size); err != nil {
		return nil, err
	}
	return m.Sum(nil), nil
}

// backupFormat tells a file's format by its last bytes alone: 1, 2, or 0
// (unsigned).
func backupFormat(r io.ReaderAt, fileSize int64) int {
	tail := make([]byte, len(trailerTail))
	if fileSize >= int64(len(tail)) {
		if _, err := r.ReadAt(tail, fileSize-int64(len(tail))); err == nil && bytes.HasPrefix(tail, []byte(backupMagicV2)) {
			return 2
		}
	}
	magic := make([]byte, len(backupMagic))
	if fileSize >= int64(len(magic)) {
		if _, err := r.ReadAt(magic, fileSize-int64(len(magic))); err == nil && string(magic) == backupMagic {
			return 1
		}
	}
	return 0
}

// readTrailerV2 reads and strictly checks a version 2 trailer: the fixed
// framing, the length arithmetic and the JSON's form. Nothing it returns may
// be used before authenticate checks the MAC.
func readTrailerV2(r io.ReaderAt, fileSize int64) (trailerV2, error) {
	var t trailerV2
	damaged := func(why string) (trailerV2, error) {
		return trailerV2{}, fmt.Errorf("%w: its trailer is damaged (%s)", errUnsigned, why)
	}
	fixed := int64(len(trailerTail) + 8 + len(trailerHead))
	if fileSize < fixed {
		return t, errUnsigned
	}
	tail := make([]byte, len(trailerTail)+8)
	if _, err := r.ReadAt(tail, fileSize-int64(len(tail))); err != nil {
		return t, err
	}
	if !bytes.Equal(tail[8:], trailerTail) {
		return t, errUnsigned
	}
	var length int64
	for _, c := range tail[:8] {
		switch {
		case c >= '0' && c <= '9':
			length = length<<4 | int64(c-'0')
		case c >= 'a' && c <= 'f':
			length = length<<4 | int64(c-'a'+10)
		default:
			return damaged("its length is not lowercase hex")
		}
	}
	if length < 2 || length > maxTrailerJSON {
		return damaged("its length is out of range")
	}
	size := fileSize - fixed - length
	if size < 18 { // the smallest gzip member
		return damaged("the file is too short")
	}
	head := make([]byte, len(trailerHead)+int(length))
	if _, err := r.ReadAt(head, size); err != nil {
		return t, err
	}
	if !bytes.Equal(head[:len(trailerHead)], trailerHead) {
		return damaged("its header is not the trailer's")
	}
	j := head[len(trailerHead):]
	if j[0] != '{' {
		return damaged("it is not a JSON object")
	}
	for _, c := range j {
		if c < 0x20 || c > 0x7e {
			return damaged("it holds a byte that is not printable ASCII")
		}
	}
	dec := json.NewDecoder(bytes.NewReader(j))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&t); err != nil {
		return damaged(err.Error())
	}
	if dec.InputOffset() != int64(len(j)) {
		return damaged("something follows its JSON")
	}
	sha, err := hex.DecodeString(t.DatabaseSHA256)
	switch {
	case t.Format != backupMACFormat || t.Version != 2 || t.Compression != backupGzipName:
		return damaged("not a version 2 gzip trailer")
	case t.ServerID == "" || t.KeyID == "":
		return damaged("no server or key ID")
	case t.Size != size:
		return damaged("its size is not the file's")
	case t.DatabaseSize < 1 || t.DatabaseSize > maxDatabase:
		return damaged("the database size is out of range")
	case err != nil || len(sha) != sha256.Size || hex.EncodeToString(sha) != t.DatabaseSHA256:
		return damaged("the database digest is not 64 lowercase hex characters")
	}
	if mac, err := base64.StdEncoding.Strict().DecodeString(t.MAC); err != nil || len(mac) != sha256.Size {
		return damaged("the MAC is not 32 bytes of base64")
	}
	return t, nil
}

// authentic is a version 2 backup whose MAC verified: the only value
// decompress accepts. Only authenticate makes one.
type authentic struct {
	f io.ReaderAt
	t trailerV2
}

// authenticate checks t's server and key and its MAC over [0, t.Size) of f,
// reading only those bytes; it decodes nothing.
func authenticate(ctx context.Context, f io.ReaderAt, t trailerV2, keyByID map[string][]byte, serverID string) (authentic, error) {
	if t.ServerID != serverID {
		return authentic{}, fmt.Errorf("the backup belongs to server %s, not %s", t.ServerID, serverID)
	}
	key, ok := keyByID[t.KeyID]
	if !ok {
		return authentic{}, fmt.Errorf("the backup is signed with receipt key %s, which the key bundle does not hold", t.KeyID)
	}
	want, err := base64.StdEncoding.Strict().DecodeString(t.MAC)
	if err != nil {
		return authentic{}, errors.New("the backup's MAC is damaged")
	}
	got, err := macV2(ctx, key, t, io.NewSectionReader(f, 0, t.Size))
	if err != nil {
		return authentic{}, err
	}
	if !hmac.Equal(got, want) {
		return authentic{}, errors.New("the backup has been changed since it was made: its MAC does not verify with the key bundle")
	}
	return authentic{f: f, t: t}, nil
}

// decompress writes the database of an authentic backup to dst: exactly the
// signed number of bytes with the signed SHA-256, from a single gzip member
// with an empty header and a valid CRC that ends exactly where the signed
// region does. It reads only that region, stops when ctx ends, and asks space
// before each 32 MiB it writes (nil: no check).
func decompress(ctx context.Context, a authentic, dst io.Writer, space func(int64) error) error {
	decompressStarted.Add(1)
	br := bufio.NewReader(io.NewSectionReader(a.f, 0, a.t.Size))
	zr, err := gzip.NewReader(br)
	if err != nil {
		return fmt.Errorf("the backup does not decompress: %w", err)
	}
	zr.Multistream(false)
	if zr.Name != "" || zr.Comment != "" || len(zr.Extra) > 0 {
		return errors.New("the backup's compressed database has a name, comment or extra field")
	}
	h := sha256.New()
	// One byte more than signed is read, so a longer database is seen; it is
	// never written or asked space for.
	n, err := copyGuarded(ctx, io.MultiWriter(dst, h), io.LimitReader(zr, a.t.DatabaseSize+1), space, a.t.DatabaseSize)
	switch {
	case errors.Is(err, errTooLong):
		return fmt.Errorf("the backup's database is more than the %d bytes signed", a.t.DatabaseSize)
	case err != nil:
		return fmt.Errorf("the backup does not decompress: %w", err)
	case n != a.t.DatabaseSize:
		return fmt.Errorf("the backup's database is %d bytes, not the %d signed", n, a.t.DatabaseSize)
	case hex.EncodeToString(h.Sum(nil)) != a.t.DatabaseSHA256:
		return errors.New("the backup's database is not the one signed")
	}
	if _, err := br.ReadByte(); !errors.Is(err, io.EOF) {
		return errors.New("the backup's compressed database does not end where its signature does")
	}
	return nil
}

// errTooLong is input longer than its known size.
var errTooLong = errors.New("more bytes than expected")

// copyGuarded copies r to w a mebibyte at a time until r ends, stopping when
// ctx ends and asking space (when not nil) before each spaceChunk written.
// With a known size (limit ≥ 0), no request asks for more than the bytes
// still to come, so a copy admitted for exactly its size is never refused
// for its last partial chunk, and input going past limit is errTooLong,
// with nothing past limit written.
func copyGuarded(ctx context.Context, w io.Writer, r io.Reader, space func(int64) error, limit int64) (int64, error) {
	buf := make([]byte, mib)
	var n, allowed int64
	for {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		k, err := r.Read(buf)
		if k > 0 {
			if limit >= 0 && n+int64(k) > limit {
				return n, errTooLong
			}
			if space != nil && n+int64(k) > allowed {
				ask := max(spaceChunk, int64(k))
				if limit >= 0 {
					ask = min(ask, limit-n)
				}
				if err := space(ask); err != nil {
					return n, err
				}
				allowed = n + ask
			}
			if _, werr := w.Write(buf[:k]); werr != nil {
				return n, werr
			}
			n += int64(k)
		}
		if errors.Is(err, io.EOF) {
			return n, nil
		}
		if err != nil {
			return n, err
		}
	}
}

// guardWriter asks space before each spaceChunk it writes and stops when ctx
// ends.
type guardWriter struct {
	ctx     context.Context
	w       io.Writer
	space   func(int64) error
	n       int64
	allowed int64
}

func (g *guardWriter) Write(p []byte) (int, error) {
	if err := g.ctx.Err(); err != nil {
		return 0, err
	}
	if g.space != nil && g.n+int64(len(p)) > g.allowed {
		if err := g.space(max(spaceChunk, int64(len(p)))); err != nil {
			return 0, err
		}
		g.allowed = g.n + max(spaceChunk, int64(len(p)))
	}
	k, err := g.w.Write(p)
	g.n += int64(k)
	return k, err
}

// copyExclusive copies src's bytes, inert, to a new private file in dir
// (created exclusively, 0600) named prefix*, and returns it open, with the
// bytes' SHA-256. It stops when ctx ends and asks space before each 32 MiB
// (no more than the bytes still to come); a source that grows meanwhile is
// errTooLong. On an error nothing is left.
func copyExclusive(ctx context.Context, src *os.File, dir, prefix string, space func(int64) error) (_ *os.File, sum string, err error) {
	out, err := os.CreateTemp(dir, prefix)
	if err != nil {
		return nil, "", err
	}
	defer func() {
		if err != nil {
			out.Close()
			os.Remove(out.Name())
		}
	}()
	if err := private.Protect(out); err != nil {
		return nil, "", err
	}
	fi, err := src.Stat()
	if err != nil {
		return nil, "", err
	}
	h := sha256.New()
	if _, err := copyGuarded(ctx, io.MultiWriter(out, h), io.NewSectionReader(src, 0, 1<<62), space, fi.Size()); err != nil {
		return nil, "", err
	}
	if err := out.Sync(); err != nil {
		return nil, "", err
	}
	return out, hex.EncodeToString(h.Sum(nil)), nil
}

// compressBackup writes raw (the marked, verified database) to out as one
// gzip member, from the start of out, stopping when ctx ends and asking space
// before each 32 MiB it writes. The trailer it returns has the sizes and the
// database's SHA-256; signBackupV2 completes it.
func compressBackup(ctx context.Context, raw io.Reader, out *os.File, space func(int64) error) (trailerV2, error) {
	gw := &guardWriter{ctx: ctx, w: io.NewOffsetWriter(out, 0), space: space}
	zw, err := gzip.NewWriterLevel(gw, gzipLevel)
	if err != nil {
		return trailerV2{}, err
	}
	h := sha256.New()
	n, err := copyGuarded(ctx, io.MultiWriter(zw, h), raw, nil, -1)
	if err != nil {
		return trailerV2{}, err
	}
	if err := zw.Close(); err != nil {
		return trailerV2{}, err
	}
	if n < 1 || n > maxDatabase {
		return trailerV2{}, fmt.Errorf("a database of %d bytes cannot be backed up", n)
	}
	return trailerV2{Format: backupMACFormat, Version: 2, Size: gw.n, Compression: backupGzipName, DatabaseSize: n,
		DatabaseSHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

// signBackupV2 computes the MAC of t and the compressed database in out with
// key, stopping when ctx ends, and appends the trailer.
func signBackupV2(ctx context.Context, out *os.File, t trailerV2, serverID string, key ReceiptKey) (trailerV2, error) {
	t.ServerID, t.KeyID = serverID, key.ID
	mac, err := macV2(ctx, key.Secret, t, io.NewSectionReader(out, 0, t.Size))
	if err != nil {
		return trailerV2{}, fmt.Errorf("storage: signing the backup: %w", err)
	}
	t.MAC = base64.StdEncoding.EncodeToString(mac)
	if err := appendTrailer(out, t); err != nil {
		return trailerV2{}, fmt.Errorf("storage: signing the backup: %w", err)
	}
	return t, nil
}

// appendTrailer writes t's trailer member at the end of the compressed
// database in out.
func appendTrailer(out *os.File, t trailerV2) error {
	j, err := json.Marshal(t)
	if err != nil {
		return err
	}
	var b bytes.Buffer
	b.Write(trailerHead)
	b.Write(j)
	fmt.Fprintf(&b, "%08x", len(j))
	b.Write(trailerTail)
	_, err = out.WriteAt(b.Bytes(), t.Size)
	return err
}

// removeTemps removes the files in dir whose names start with prefix.
func removeTemps(dir, prefix string) {
	matches, _ := filepath.Glob(filepath.Join(dir, globEscape(prefix)+"*"))
	for _, m := range matches {
		os.Remove(m)
	}
}

// globEscape escapes filepath.Match's special characters.
func globEscape(s string) string {
	var b bytes.Buffer
	for _, c := range s {
		if c == '*' || c == '?' || c == '[' || c == '\\' {
			b.WriteByte('\\')
		}
		b.WriteRune(c)
	}
	return b.String()
}
