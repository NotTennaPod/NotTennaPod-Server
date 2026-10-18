package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
)

// PROTOCOL §11.2: compressed, signed backups (version 2), their strict reader,
// the decoder that runs only on an authenticated copy, and the writer's
// free-space guard and crash safety.

// smallChunks makes the free-space checks come every n bytes for one test.
func smallChunks(t *testing.T, n int64) {
	old := spaceChunk
	spaceChunk = n
	t.Cleanup(func() { spaceChunk = old })
}

func sealedBox(t *testing.T, dir string) *sealed.Box {
	t.Helper()
	box, err := sealed.LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	return box
}

// randomBlobs adds n incompressible stored feed responses of size bytes to a
// new account, sealed as the server seals them.
func randomBlobs(t *testing.T, s *Store, n, size int) {
	t.Helper()
	ctx := context.Background()
	box := sealedBox(t, s.dir)
	acct := createAccounts(t, s, 1)[0]
	if err := s.Write(ctx, func(w *WriteTx) error {
		for i := range n {
			p := fmt.Sprintf("p1:blob-%d", i)
			if _, err := w.tx.ExecContext(ctx, `INSERT INTO podcasts (account_id, podcast_id, creating_op) VALUES (?, ?, 'op')`, acct, p); err != nil {
				return err
			}
			plain := make([]byte, size)
			rand.Read(plain)
			if _, err := w.tx.ExecContext(ctx, `INSERT INTO feed_responses (account_id, podcast_id, sealed, stored_at) VALUES (?, ?, ?, ?)`,
				acct, p, box.Seal(plain, []byte("feed-response\x00"+acct+"\x00"+p)), 1000+i); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// A backup is the marked, verified database compressed and signed: named
// .db.gz, smaller than the database, with an empty gzip header, read back
// by the restore's reader before it is published. A byte changed after
// signing is caught by that read-back: nothing is published.
func TestBackupIsCompressedSignedAndSelfChecked(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	createAccounts(t, s, 3)
	dir := t.TempDir()
	info, err := s.Backup(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^nottennapod-\d{8}-\d{6}-1\.db\.gz$`).MatchString(filepath.Base(info.Path)) {
		t.Fatalf("the backup is named %s", filepath.Base(info.Path))
	}
	if fi, err := os.Stat(info.Path); err != nil || fi.Mode().Perm() != 0o600 || fi.Size() != info.Size {
		t.Fatalf("the backup file %v %v", fi, err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("the folder holds %v", entries)
	}
	data, _ := os.ReadFile(info.Path)
	tr, err := readTrailerV2(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	db := rawDatabaseOf(t, info.Path)
	sum := sha256.Sum256(db)
	if tr.DatabaseSize != int64(len(db)) || tr.DatabaseSHA256 != hex.EncodeToString(sum[:]) || info.Size >= int64(len(db)) {
		t.Fatalf("trailer %+v for a database of %d bytes in %d", tr, len(db), info.Size)
	}
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil || zr.Name != "" || zr.Comment != "" || len(zr.Extra) != 0 || !zr.ModTime.IsZero() {
		t.Fatalf("the database's gzip header: %+v %v", zr.Header, err)
	}
	plain := plainCopy(t, info.Path)
	want, _ := s.SchemaVersion(ctx)
	if err := VerifyDatabaseFile(ctx, plain, want); err != nil {
		t.Fatal(err)
	}
	f, err := readDBFacts(ctx, rawSQL(t, plain))
	if err != nil || !f.marker {
		t.Fatalf("the database is not marked as a backup: %v %v", f.marker, err)
	}
	// A byte of the compressed database changed after signing.
	broken := t.TempDir()
	s.hooks.backupFault = func(point string) error {
		if point == "signed" {
			temps, _ := filepath.Glob(filepath.Join(broken, backupTempPrefix+"*"))
			for _, p := range temps {
				b, _ := os.ReadFile(p)
				if bytes.HasPrefix(b, []byte{0x1f, 0x8b}) {
					b[len(b)/3] ^= 0x40
					os.WriteFile(p, b, 0o600)
				}
			}
		}
		return nil
	}
	if _, err := s.Backup(ctx, broken); err == nil || !strings.Contains(err.Error(), "does not read back") {
		t.Fatalf("a backup changed after signing: %v", err)
	}
	if entries, _ := os.ReadDir(broken); len(entries) != 0 {
		t.Fatalf("a changed backup left %v", entries)
	}
}

func rawSQL(t *testing.T, path string) rowQuerier {
	t.Helper()
	db, err := openReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// gunzip, Python's gzip and Go's multistream reader all return exactly the
// database: the trailer is an empty gzip member whose comment holds the JSON.
func TestBackupTrailerIsPlainGzip(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	createAccounts(t, s, 2)
	info, err := s.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(info.Path)
	tr, _ := readTrailerV2(bytes.NewReader(data), int64(len(data)))
	var db bytes.Buffer
	a, err := authenticate(ctx, bytes.NewReader(data), tr, map[string][]byte{s.receiptKeys[0].ID: s.receiptKeys[0].Secret}, tr.ServerID)
	if err != nil {
		t.Fatal(err)
	}
	if err := decompress(ctx, a, &db, nil); err != nil {
		t.Fatal(err)
	}
	zr, _ := gzip.NewReader(bytes.NewReader(data))
	zr.Multistream(false)
	if first, err := io.ReadAll(zr); err != nil || !bytes.Equal(first, db.Bytes()) {
		t.Fatalf("the first member: %d bytes %v", len(first), err)
	}
	if err := zr.Reset(bytes.NewReader(data[tr.Size:])); err != nil {
		t.Fatal(err)
	}
	zr.Multistream(false)
	if rest, err := io.ReadAll(zr); err != nil || len(rest) != 0 || !strings.HasSuffix(zr.Comment, backupMagicV2) {
		t.Fatalf("the trailer member: %d bytes, comment %q, %v", len(rest), zr.Comment, err)
	}
	multi, _ := gzip.NewReader(bytes.NewReader(data))
	if all, err := io.ReadAll(multi); err != nil || !bytes.Equal(all, db.Bytes()) {
		t.Fatalf("Go's multistream reader: %d bytes %v", len(all), err)
	}
	for _, tool := range [][]string{{"gzip", "-dc", info.Path}, {"python3", "-I", "-c", "import gzip,sys; sys.stdout.buffer.write(gzip.open(sys.argv[1]).read())", info.Path}} {
		if _, err := exec.LookPath(tool[0]); err != nil {
			t.Logf("%s is not installed", tool[0])
			continue
		}
		out, err := exec.Command(tool[0], tool[1:]...).Output()
		if err != nil || !bytes.Equal(out, db.Bytes()) {
			t.Errorf("%s: %d bytes, %v", tool[0], len(out), err)
		}
	}
}

// v2File is a version 2 backup's parts, for building damaged ones.
type v2File struct {
	region, head, json, length, tail []byte
}

func (f v2File) bytes() []byte {
	return bytes.Join([][]byte{f.region, f.head, f.json, f.length, f.tail}, nil)
}

func splitV2(t *testing.T, data []byte) (v2File, trailerV2) {
	t.Helper()
	tr, err := readTrailerV2(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rest := data[tr.Size:]
	j := rest[len(trailerHead) : len(rest)-len(trailerTail)-8]
	return v2File{region: data[:tr.Size], head: bytes.Clone(trailerHead), json: bytes.Clone(j), length: []byte(fmt.Sprintf("%08x", len(j))),
		tail: bytes.Clone(trailerTail)}, tr
}

// The trailer reader refuses every framing, length and JSON flaw before any
// MAC or decoding: nothing is decoded.
func TestStrictTrailerReader(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	createAccounts(t, s, 1)
	info, err := s.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(info.Path)
	good, tr := splitV2(t, data)
	withJSON := func(change func(m map[string]any)) func(*v2File) {
		return func(f *v2File) {
			var m map[string]any
			json.Unmarshal(f.json, &m)
			change(m)
			f.json, _ = json.Marshal(m)
			f.length = []byte(fmt.Sprintf("%08x", len(f.json)))
		}
	}
	rawJSON := func(j string) func(*v2File) {
		return func(f *v2File) {
			f.json = []byte(j)
			f.length = []byte(fmt.Sprintf("%08x", len(f.json)))
		}
	}
	base, _ := json.Marshal(tr)
	cases := map[string]func(*v2File){
		"bad magic":             func(f *v2File) { copy(f.tail, "NTPBMAC3") },
		"a changed final block": func(f *v2File) { f.tail[9] = 0x01 },
		"bad hex":               func(f *v2File) { f.length[7] = 'g' },
		"uppercase hex": func(f *v2File) {
			f.length = []byte(strings.ToUpper(fmt.Sprintf("%08x", len(f.json))))
			f.json = append(f.json, ' ')
		},
		"a length of 0":             func(f *v2File) { f.json, f.length = nil, []byte("00000000") },
		"a length of 1":             rawJSON("{"),
		"a length of 4097":          rawJSON(string(base[:len(base)-1]) + strings.Repeat(" ", 4097-len(base)) + "}"),
		"a length past the file":    func(f *v2File) { f.length = []byte("00001000") },
		"a length of ffffffff":      func(f *v2File) { f.length = []byte("ffffffff") },
		"a header with a name":      func(f *v2File) { f.head[3] = 0x18 },
		"a header with a time":      func(f *v2File) { f.head[4] = 1 },
		"a header of another OS":    func(f *v2File) { f.head[9] = 3 },
		"an unknown field":          withJSON(func(m map[string]any) { m["extra"] = 1 }),
		"trailing JSON":             rawJSON(string(base) + "{}"),
		"a trailing byte":           rawJSON(string(base) + " "),
		"a control byte":            rawJSON(strings.Replace(string(base), `"gzip"`, "\"gz\tp\"", 1)),
		"non-ASCII":                 rawJSON(strings.Replace(string(base), `"gzip"`, "\"gzíp\"", 1)),
		"not an object":             rawJSON(`["nottennapod-backup-mac"]`),
		"a size mismatch":           withJSON(func(m map[string]any) { m["size"] = tr.Size + 1 }),
		"a database size of 0":      withJSON(func(m map[string]any) { m["database_size"] = 0 }),
		"a database size over 2^42": withJSON(func(m map[string]any) { m["database_size"] = int64(1)<<42 + 1 }),
		"a digest not hex":          withJSON(func(m map[string]any) { m["database_sha256"] = strings.Repeat("z", 64) }),
		"an uppercase digest":       withJSON(func(m map[string]any) { m["database_sha256"] = strings.ToUpper(tr.DatabaseSHA256) }),
		"a short digest":            withJSON(func(m map[string]any) { m["database_sha256"] = tr.DatabaseSHA256[:62] }),
		"a MAC of 31 bytes":         withJSON(func(m map[string]any) { m["mac"] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==" }),
		"a MAC not base64":          withJSON(func(m map[string]any) { m["mac"] = strings.Repeat("!", 44) }),
		"version 1":                 withJSON(func(m map[string]any) { m["version"] = 1 }),
		"another compression":       withJSON(func(m map[string]any) { m["compression"] = "zstd" }),
		"another format":            withJSON(func(m map[string]any) { m["format"] = "other" }),
		"no server":                 withJSON(func(m map[string]any) { m["server_id"] = "" }),
	}
	if _, err := readTrailerV2(bytes.NewReader(good.bytes()), int64(len(good.bytes()))); err != nil {
		t.Fatalf("the unchanged parts: %v", err)
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			f := good
			f.head, f.tail, f.length, f.json = bytes.Clone(good.head), bytes.Clone(good.tail), bytes.Clone(good.length), bytes.Clone(good.json)
			change(&f)
			b := f.bytes()
			decoded := BackupsDecoded()
			if got, err := readTrailerV2(bytes.NewReader(b), int64(len(b))); !errors.Is(err, errUnsigned) {
				t.Fatalf("read %+v, %v", got, err)
			}
			path := filepath.Join(t.TempDir(), "damaged.db.gz")
			os.WriteFile(path, b, 0o600)
			if err := s.VerifyBackup(ctx, path, nil); err == nil {
				t.Fatal("verified")
			}
			if BackupsDecoded() != decoded {
				t.Fatal("decoded")
			}
		})
	}
}

// signRegion writes region to path as a version 2 backup whose trailer
// claims a database of dbSize bytes with digest sum, signed with key.
func signRegion(t *testing.T, path string, region []byte, dbSize int64, sum, serverID string, key ReceiptKey) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f.Write(region)
	if _, err := signBackupV2(context.Background(), f, trailerV2{Format: backupMACFormat, Version: 2, Size: int64(len(region)), Compression: backupGzipName,
		DatabaseSize: dbSize, DatabaseSHA256: sum}, serverID, key); err != nil {
		t.Fatal(err)
	}
}

func gz(t *testing.T, parts ...[]byte) []byte {
	t.Helper()
	var b bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&b, gzipLevel)
	for _, p := range parts {
		zw.Write(p)
	}
	zw.Close()
	return b.Bytes()
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// A backup signed with the server's key whose compressed database is
// malformed is refused by the decoder, before SQLite opens anything: at a
// restore's verification and staging, and by VerifyBackup.
func TestAuthenticatedButMalformedRegionRefused(t *testing.T) {
	ctx := context.Background()
	fx := newRestoreFixture(t)
	serverID, ks, _, err := readReceiptKeysFile(fx.dir)
	if err != nil {
		t.Fatal(err)
	}
	db := rawDatabaseOf(t, fx.backup)
	member := gz(t, db)
	crc := bytes.Clone(member)
	crc[len(crc)-8] ^= 1
	var named bytes.Buffer
	nw := gzip.NewWriter(&named)
	nw.Name = "nottennapod.db"
	nw.Write(db)
	nw.Close()
	half := len(db) / 2
	cases := []struct {
		name   string
		region []byte
		size   int64
		sum    string
		want   string
	}{
		{"short output", gz(t, db[:len(db)-1]), int64(len(db)), digest(db), "not the"},
		{"one extra byte", gz(t, db, []byte{0}), int64(len(db)), digest(db), "more than the"},
		{"a CRC mismatch", crc, int64(len(db)), digest(db), "does not decompress"},
		{"two members", append(gz(t, db[:half]), gz(t, db[half:])...), int64(len(db)), digest(db), "not the"},
		{"bytes after the member", append(bytes.Clone(member), 0, 0, 0), int64(len(db)), digest(db), "does not end where"},
		{"a smaller database size", member, int64(len(db) - 1), digest(db), "more than the"},
		{"a larger database size", member, int64(len(db) + 1), digest(db), "not the"},
		{"another digest", member, int64(len(db)), digest(append(bytes.Clone(db), 1)), "not the one signed"},
		{"a named member", named.Bytes(), int64(len(db)), digest(db), "has a name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "malformed.db.gz")
			signRegion(t, path, tc.region, tc.size, tc.sum, serverID, ks[0])
			s := openTest(t, config{dataDir: fx.clone(t).dir})
			if err := s.VerifyBackup(ctx, path, nil); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("VerifyBackup: %v, want %q", err, tc.want)
			}
			s.Close()
			c := fx.clone(t)
			c.backup = path
			before := snapshotDir(t, c.dir)
			if _, err := c.restore(config{}); !errors.Is(err, ErrRestoreRefused) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("restore: %v, want %q", err, tc.want)
			}
			if after := snapshotDir(t, c.dir); fmt.Sprint(after) != fmt.Sprint(before) {
				t.Fatalf("the refusal changed the data directory:\n%v\n%v", before, after)
			}
			// Staging (a resumed restore) refuses it the same way: the good
			// backup is verified, then its staged copy is replaced.
			c = fx.clone(t)
			bad, _ := os.ReadFile(path)
			_, err := c.restore(config{hooks: hooks{restoreFault: func(p string) error {
				if p == "staged-source-copied" {
					return os.WriteFile(filepath.Join(c.dir, stagedSourceName), bad, 0o600)
				}
				return nil
			}}})
			if !errors.Is(err, ErrRestoreRefused) || !strings.Contains(err.Error(), "staged copy does not verify") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("staging: %v, want %q", err, tc.want)
			}
			if _, err := os.Stat(filepath.Join(c.dir, databaseName)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Nothing is ever decoded before its MAC verifies, on any path: a byte of
// the compressed database changed, a byte of the trailer's JSON changed, or a
// backup signed with a key that is not the server's, at a restore's
// verification, its staging, a staging resumed at start, the staging again
// after an interrupted transition, and VerifyBackup. Nothing is written
// beyond the inert copy.
func TestDecoderNeverRunsBeforeMAC(t *testing.T) {
	ctx := context.Background()
	fx := newRestoreFixture(t)
	serverID, ks, _, _ := readReceiptKeysFile(fx.dir)
	good, _ := os.ReadFile(fx.backup)
	parts, tr := splitV2(t, good)
	tamperings := map[string]func() []byte{
		"a changed compressed byte": func() []byte {
			b := bytes.Clone(good)
			b[tr.Size/2] ^= 1
			return b
		},
		"a changed JSON byte": func() []byte {
			f := parts
			f.json = bytes.Replace(bytes.Clone(parts.json), []byte(tr.DatabaseSHA256), []byte(flipHex(tr.DatabaseSHA256)), 1)
			return f.bytes()
		},
		"another key": func() []byte {
			path := filepath.Join(t.TempDir(), "other.db.gz")
			writeSigned(t, path, rawDatabaseOf(t, fx.backup), serverID, ReceiptKey{ID: ks[0].ID, Secret: bytes.Repeat([]byte{5}, 32)}, 2)
			b, _ := os.ReadFile(path)
			return b
		},
	}
	// replaceSource is a fault hook that replaces the staged source with b
	// once the run has copied it, and notes how many decodings there were.
	replaceSource := func(dir string, b []byte, decoded *int64) func(string) error {
		return func(p string) error {
			if p == "staged-source-copied" {
				*decoded = BackupsDecoded()
				return os.WriteFile(filepath.Join(dir, stagedSourceName), b, 0o600)
			}
			return nil
		}
	}
	crashAt := func(c *restoreFixture, point string) {
		t.Helper()
		if _, err := c.restore(config{hooks: hooks{restoreFault: func(p string) error {
			if p == point {
				return errors.New("crash")
			}
			return nil
		}}}); err == nil {
			t.Fatal("the crash did not stop the restore")
		}
	}
	for name, tamper := range tamperings {
		bad := tamper()
		t.Run(name+"/verify", func(t *testing.T) {
			c := fx.clone(t)
			c.backup = filepath.Join(t.TempDir(), "bad.db.gz")
			os.WriteFile(c.backup, bad, 0o600)
			before, decoded := snapshotDir(t, c.dir), BackupsDecoded()
			if _, err := c.restore(config{}); !errors.Is(err, ErrRestoreRefused) {
				t.Fatalf("got %v", err)
			}
			if BackupsDecoded() != decoded {
				t.Fatal("decoded before the MAC")
			}
			if after := snapshotDir(t, c.dir); fmt.Sprint(after) != fmt.Sprint(before) {
				t.Fatalf("changed the data directory:\n%v\n%v", before, after)
			}
		})
		t.Run(name+"/stage", func(t *testing.T) {
			c := fx.clone(t)
			var decoded int64
			if _, err := c.restore(config{hooks: hooks{restoreFault: replaceSource(c.dir, bad, &decoded)}}); !errors.Is(err, ErrRestoreRefused) {
				t.Fatalf("got %v", err)
			}
			if BackupsDecoded() != decoded {
				t.Fatal("decoded before the MAC")
			}
			if _, err := os.Stat(filepath.Join(c.dir, stagedName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("a staged database was written: %v", err)
			}
		})
		t.Run(name+"/resumed at start", func(t *testing.T) {
			c := fx.clone(t)
			crashAt(c, "staged-copied")
			var decoded int64
			_, err := open(ctx, config{dataDir: c.dir, syncDir: noSync, hooks: hooks{restoreFault: replaceSource(c.dir, bad, &decoded)}})
			if !errors.Is(err, ErrRestoreRefused) {
				t.Fatalf("got %v", err)
			}
			if BackupsDecoded() != decoded {
				t.Fatal("decoded before the MAC")
			}
		})
		t.Run(name+"/after an interrupted transition", func(t *testing.T) {
			c := fx.clone(t)
			crashAt(c, "staged-transitioned")
			if st := readState(t, c.dir); st.Restore == nil || st.Restore.Step != stepTransition {
				t.Fatalf("state %+v", st.Restore)
			}
			var decoded int64
			if _, err := c.restore(config{hooks: hooks{restoreFault: replaceSource(c.dir, bad, &decoded)}}); !errors.Is(err, ErrRestoreRefused) {
				t.Fatalf("got %v", err)
			}
			if BackupsDecoded() != decoded {
				t.Fatal("decoded before the MAC")
			}
		})
		t.Run(name+"/VerifyBackup", func(t *testing.T) {
			s := openTest(t, config{dataDir: fx.clone(t).dir})
			path := filepath.Join(t.TempDir(), "bad.db.gz")
			os.WriteFile(path, bad, 0o600)
			decoded := BackupsDecoded()
			if err := s.VerifyBackup(ctx, path, nil); err == nil {
				t.Fatal("verified")
			}
			if BackupsDecoded() != decoded {
				t.Fatal("decoded before the MAC")
			}
			if left, _ := filepath.Glob(filepath.Join(filepath.Dir(path), backupTempPrefix+"*")); len(left) > 0 {
				t.Fatalf("left %v", left)
			}
		})
	}
}

func flipHex(s string) string {
	b := []byte(s)
	if b[0] == '0' {
		b[0] = '1'
	} else {
		b[0] = '0'
	}
	return string(b)
}

// VerifyBackup checks both formats with the server's keys and refuses a
// changed, cut or foreign one; a compressed backup is checked in a copy
// beside it, which goes.
func TestVerifyBackupBothFormats(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	s := openTest(t, config{dataDir: d})
	createAccounts(t, s, 2)
	info, err := s.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	other := openTest(t, config{dataDir: t.TempDir()})
	foreign, err := other.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	forEachFormat(t, func(t *testing.T, format int) {
		path := asFormat(t, info.Path, d, format)
		if err := s.VerifyBackup(ctx, path, nil); err != nil {
			t.Fatal(err)
		}
		data, _ := os.ReadFile(path)
		for name, b := range map[string][]byte{
			"changed": func() []byte { c := bytes.Clone(data); c[len(c)/3] ^= 1; return c }(),
			"cut":     data[:len(data)-1],
			"bare":    rawDatabaseOf(t, path),
		} {
			p := filepath.Join(t.TempDir(), "x"+filepath.Ext(path))
			os.WriteFile(p, b, 0o600)
			if err := s.VerifyBackup(ctx, p, nil); err == nil {
				t.Errorf("%s: verified", name)
			}
			if left, _ := filepath.Glob(filepath.Join(filepath.Dir(p), backupTempPrefix+"*")); len(left) > 0 {
				t.Errorf("%s: left %v", name, left)
			}
		}
		if err := s.VerifyBackup(ctx, asFormat(t, foreign.Path, other.dir, format), nil); err == nil || !strings.Contains(err.Error(), "belongs to server") {
			t.Errorf("another server's backup: %v", err)
		}
	})
	// The copy's free-space guard, asked before each chunk of a backup
	// larger than one read.
	randomBlobs(t, s, 3, 512<<10)
	info, err = s.Backup(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	smallChunks(t, 4<<10)
	low := errors.New("low")
	calls := 0
	if err := s.VerifyBackup(ctx, info.Path, func(int64) error {
		if calls++; calls > 1 {
			return low
		}
		return nil
	}); !errors.Is(err, low) {
		t.Fatalf("a guard that refuses: %v", err)
	}
	if left, _ := filepath.Glob(filepath.Join(filepath.Dir(info.Path), backupTempPrefix+"*")); len(left) > 0 {
		t.Fatalf("left %v", left)
	}
}

// The backup's own writing is guarded: a refusal of the space callback
// partway through compression stops the backup with both temporaries gone,
// nothing new published and the older backups as they were; incompressible
// content is guarded at every chunk; a cancel stops it at once, and an
// incomplete stream is never published.
func TestBackupSpaceGuardAndCancel(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	randomBlobs(t, s, 4, 256<<10) // incompressible: sealed
	dir := t.TempDir()
	older, err := s.Backup(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	olderBytes, _ := os.ReadFile(older.Path)
	smallChunks(t, 64<<10)
	onlyOlder := func(t *testing.T) {
		t.Helper()
		entries, _ := os.ReadDir(dir)
		if len(entries) != 1 || entries[0].Name() != filepath.Base(older.Path) {
			t.Fatalf("the folder holds %v", entries)
		}
		if b, _ := os.ReadFile(older.Path); !bytes.Equal(b, olderBytes) {
			t.Fatal("the older backup changed")
		}
	}
	t.Run("refused partway", func(t *testing.T) {
		var asked int64
		calls := 0
		_, err := s.BackupAs(ctx, dir, func(taken time.Time) (string, error) { return BackupName(dir, taken) }, BackupOptions{Space: func(n int64) error {
			calls++
			asked += n
			if calls == 5 {
				return NoSpace("not enough free space to compress the backup")
			}
			return nil
		}})
		if !errors.Is(err, ErrNoSpace) {
			t.Fatalf("got %v", err)
		}
		onlyOlder(t)
	})
	t.Run("incompressible content is guarded throughout", func(t *testing.T) {
		var asked int64
		info, err := s.BackupAs(ctx, dir, func(taken time.Time) (string, error) { return BackupName(dir, taken) }, BackupOptions{Space: func(n int64) error {
			asked += n
			return nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		if asked < info.Size {
			t.Fatalf("asked for %d bytes, wrote %d", asked, info.Size)
		}
		os.Remove(info.Path)
	})
	t.Run("cancelled", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		var at time.Time
		calls := 0
		_, err := s.BackupAs(cctx, dir, func(taken time.Time) (string, error) { return BackupName(dir, taken) }, BackupOptions{Space: func(int64) error {
			if calls++; calls == 3 {
				at = time.Now()
				cancel()
			}
			return nil
		}})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
		if d := time.Since(at); d > time.Second {
			t.Fatalf("stopped %v after the cancel", d)
		}
		onlyOlder(t)
	})
	// The compression and MAC passes check the context on their own: an
	// endless input stops within a second of a cancel.
	t.Run("endless passes", func(t *testing.T) {
		for name, pass := range map[string]func(context.Context) error{
			"compression": func(ctx context.Context) error {
				f, _ := os.CreateTemp(t.TempDir(), "x")
				defer f.Close()
				_, err := compressBackup(ctx, endless{}, f, nil)
				return err
			},
			"MAC": func(ctx context.Context) error {
				_, err := macV2(ctx, make([]byte, 32), trailerV2{Size: 1 << 40}, endless{})
				return err
			},
			"version 1 MAC": func(ctx context.Context) error {
				_, err := backupMAC(ctx, make([]byte, 32), "s", "k", 1<<40, endless{})
				return err
			},
			"copy": func(ctx context.Context) error {
				_, err := copyGuarded(ctx, io.Discard, endless{}, nil, -1)
				return err
			},
		} {
			cctx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- pass(cctx) }()
			time.Sleep(50 * time.Millisecond) // let it run
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("%s: %v", name, err)
				}
			case <-time.After(time.Second):
				t.Errorf("%s did not stop within a second", name) // and runs on until the test binary ends
			}
		}
	})
}

type endless struct{}

func (endless) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(i)
	}
	return len(p), nil
}

// A crash at each step of a backup leaves either only temporaries, which the
// next backup into the folder removes, or one complete, verified backup.
func TestBackupCrashAtEveryPoint(t *testing.T) {
	ctx := context.Background()
	for _, point := range []string{"raw", "compressed", "signed", "self-checked", "renamed"} {
		t.Run(point, func(t *testing.T) {
			s := openTest(t, config{dataDir: t.TempDir()})
			createAccounts(t, s, 2)
			dir := t.TempDir()
			crash := errors.New("crash")
			s.hooks.backupFault = func(p string) error {
				if p == point {
					return crash
				}
				return nil
			}
			if _, err := s.Backup(ctx, dir); !errors.Is(err, crash) {
				t.Fatalf("got %v", err)
			}
			s.hooks.backupFault = nil
			entries, _ := os.ReadDir(dir)
			published := 0
			for _, e := range entries {
				switch {
				case strings.HasPrefix(e.Name(), backupTempPrefix):
				case strings.HasSuffix(e.Name(), ".db.gz"):
					published++
					if err := s.VerifyBackup(ctx, filepath.Join(dir, e.Name()), nil); err != nil {
						t.Fatalf("the published %s: %v", e.Name(), err)
					}
				default:
					t.Fatalf("left %s", e.Name())
				}
			}
			if want := map[bool]int{true: 1, false: 0}[point == "renamed"]; published != want {
				t.Fatalf("%d published after a crash at %s", published, point)
			}
			if _, err := s.Backup(ctx, dir); err != nil {
				t.Fatal(err)
			}
			entries, _ = os.ReadDir(dir)
			if len(entries) != published+1 {
				t.Fatalf("after the next backup: %v", entries)
			}
			for _, e := range entries {
				if err := s.VerifyBackup(ctx, filepath.Join(dir, e.Name()), nil); err != nil {
					t.Fatalf("%s: %v", e.Name(), err)
				}
			}
		})
	}
}

// Temporaries of a restore's verification and a staged source left by a
// crash are removed at the next restore and at the next start.
func TestStaleRestoreTempsRemovedAtRestoreAndOpen(t *testing.T) {
	fx := newRestoreFixture(t)
	stale := []string{restoreTempPrefix + "verify-1.gz", restoreTempPrefix + "verify-2.db", stagedSourceName}
	plant := func(dir string) {
		for _, name := range stale {
			os.WriteFile(filepath.Join(dir, name), []byte("left by a crash"), 0o600)
		}
	}
	t.Run("start", func(t *testing.T) {
		c := fx.clone(t)
		plant(c.dir)
		openTest(t, config{dataDir: c.dir}).Close()
		if left := restoreLeftovers(t, c.dir); len(left) > 0 {
			t.Fatalf("left %v", left)
		}
	})
	forEachFormat(t, func(t *testing.T, format int) {
		c := newRestoreFixtureIn(t, format).clone(t)
		plant(c.dir)
		if _, err := c.restore(config{}); err != nil {
			t.Fatal(err)
		}
		if left := restoreLeftovers(t, c.dir); len(left) > 0 {
			t.Fatalf("left %v", left)
		}
	})
	// A run stopped between copying the backup and recording itself left a
	// whole staged copy, and a staged database with its log, that no restore
	// is recorded for. With exactly the space the restore needs, counting
	// those files as used, the retry removes them before it counts the space,
	// and fits.
	t.Run("an unrecorded staged copy at exactly the space needed", func(t *testing.T) { forEachFormat(t, testUnrecordedStagedCopy) })
}

func testUnrecordedStagedCopy(t *testing.T, format int) {
	c := newRestoreFixtureIn(t, format).clone(t)
	backup, _ := os.ReadFile(c.backup)
	os.WriteFile(filepath.Join(c.dir, stagedSourceName), backup, 0o600)
	os.WriteFile(filepath.Join(c.dir, stagedName), backup[:len(backup)/2], 0o600)
	os.WriteFile(filepath.Join(c.dir, stagedName+"-wal"), make([]byte, 64<<10), 0o600)
	os.WriteFile(filepath.Join(c.dir, stagedName+"-shm"), make([]byte, 32<<10), 0o600)
	if st := readState(t, c.dir); st.Restore != nil {
		t.Fatalf("a restore is recorded: %+v", st.Restore)
	}
	need := restoreNeed(format, int64(len(backup)), int64(len(rawDatabaseOf(t, c.backup))), latestSchema())
	d := &disk{dir: c.dir}
	d.budget.Store(restoreReserve + need)
	stop := d.sample()
	_, err := c.restore(config{free: d.free})
	stop()
	if err != nil {
		t.Fatalf("the retry with exactly the space it needs: %v", err)
	}
	if left := restoreLeftovers(t, c.dir); len(left) > 0 {
		t.Fatalf("left %v", left)
	}
	assertSettled(t, openTest(t, config{dataDir: c.dir}))
}

// A restore admitted with exactly the space it needs fits, its last partial
// chunks included: an incompressible database over 64 MiB, whose copy and
// decoding end with a chunk shorter than 32 MiB, never asks for more than
// the bytes still to come, and the reserve is never taken.
func TestRestoreExactAdmission(t *testing.T) {
	fx := newBlobFixture(t, len(embeddedMigrations), 2, 33<<20)
	forEachFormat(t, func(t *testing.T, format int) {
		c := fx.clone(t)
		c.backup = asFormat(t, fx.backup, fx.dir, format)
		fi, _ := os.Stat(c.backup)
		dbSize := int64(len(rawDatabaseOf(t, c.backup)))
		if dbSize <= 64<<20 || dbSize%spaceChunk == 0 || fi.Size()%spaceChunk == 0 {
			t.Fatalf("the database is %d bytes and the backup %d: not over 64 MiB with partial last chunks", dbSize, fi.Size())
		}
		need := restoreNeed(format, fi.Size(), dbSize, latestSchema())
		d := &disk{dir: c.dir}
		d.budget.Store(restoreReserve + need)
		stop := d.sample()
		_, err := c.restore(config{free: d.free})
		stop()
		if err != nil {
			t.Fatalf("a restore admitted for exactly its size: %v", err)
		}
		t.Logf("database %d bytes, backup %d, admitted %d, the most written at once %d", dbSize, fi.Size(), need, d.peak.Load())
		if d.peak.Load() > need {
			t.Fatalf("the restore wrote %d bytes at once, %d admitted: the reserve was taken", d.peak.Load(), need)
		}
		assertSettled(t, openTest(t, config{dataDir: c.dir}))
	})
}
