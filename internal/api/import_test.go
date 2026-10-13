package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
)

func init() {
	errorStatus["import_exists"] = []int{409}
	errorStatus["account_not_empty"] = []int{409}
	errorStatus["import_not_uploading"] = []int{409}
	errorStatus["insufficient_space"] = []int{507}
}

// withImports serves the library import with a real runner (PROTOCOL §11.5).
func withImports(t *testing.T) func(*Config) {
	return func(c *Config) {
		box, err := sealed.LoadOrCreate(c.Store.Dir())
		if err != nil {
			t.Fatal(err)
		}
		reg := protocol.NewRegistry()
		ops.Register(reg)
		svc := &protocol.Service{Store: c.Store, Registry: reg, Box: box, Now: time.Now}
		r := &importer.Runner{Store: c.Store, Service: svc, Dir: filepath.Join(c.Store.Dir(), "imports"),
			OpenStaging: importer.StagingOpener(c.Store)}
		if err := r.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.Stop)
		c.Imports = r
	}
}

// The import over HTTP: a browser bound to its web installation reserves an
// empty account, uploads the file in base64 chunks, finishes, and follows the
// import to publication; the same file again returns it; another file is
// refused; an unbound session cannot import.
func TestLibraryImportEndpoints(t *testing.T) {
	e := newEnv(t, withImports(t))
	session, _ := e.setupAdmin("owner", "correct horse battery")
	data, err := os.ReadFile("../importer/androiddb/testdata/android-3.12.1/emulator-parity-library.db")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	fp := hex.EncodeToString(sum[:])
	reserve := map[string]any{"kind": "android_db", "fingerprint": fp, "size": len(data), "file_name": "AntennaPodBackup.db"}
	e.do(http.MethodPost, "/v1/import", reserve, browser(session)).expect(http.StatusForbidden, "forbidden")
	e.registerWeb(session, newID(), newID()).must(http.StatusOK)
	r := e.do(http.MethodPost, "/v1/import", reserve, browser(session)).must(http.StatusCreated)
	id := r.str("import_id")
	if r.str("status") != "uploading" || id == "" {
		t.Fatalf("reserved %s", r.raw)
	}
	if again := e.do(http.MethodPost, "/v1/import", reserve, browser(session)).must(http.StatusOK); again.str("import_id") != id {
		t.Fatalf("the same file again: %s", again.raw)
	}
	half := len(data) / 2
	for n, part := range [][]byte{data[:half], data[half:]} {
		e.do(http.MethodPut, "/v1/import/"+id+"/chunks/"+itoa(n), map[string]any{"data": base64.StdEncoding.EncodeToString(part)},
			browser(session)).must(http.StatusOK)
	}
	e.do(http.MethodPost, "/v1/import/"+id+"/finish", nil, browser(session)).must(http.StatusOK)
	deadline := time.Now().Add(30 * time.Second)
	var got *response
	for {
		got = e.do(http.MethodGet, "/v1/import/"+id, nil, browser(session)).must(http.StatusOK)
		if s := got.str("status"); s == "published" || s == "failed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("import still %s", got.str("status"))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got.str("status") != "published" {
		t.Fatalf("import %s", got.raw)
	}
	imported, _ := got.obj("result")["imported"].(map[string]any)
	if imported["episodes"] != float64(58) || imported["podcasts"] != float64(16) {
		t.Errorf("result %v", got.obj("result"))
	}
	if cur := e.do(http.MethodGet, "/v1/import", nil, browser(session)).must(http.StatusOK).obj("import"); cur["import_id"] != id {
		t.Errorf("current import %v", cur)
	}
	other := map[string]any{"kind": "opml", "fingerprint": hex.EncodeToString(make([]byte, 32)), "size": 10, "file_name": "subs.opml"}
	e.do(http.MethodPost, "/v1/import", other, browser(session)).expect(http.StatusConflict, "import_exists")
}

// Parts beyond the reserved size are refused, and so is an import the server's
// disk has no room for.
func TestImportUploadLimits(t *testing.T) {
	e := newEnv(t, withImports(t))
	session, _ := e.setupAdmin("owner", "correct horse battery")
	e.registerWeb(session, newID(), newID()).must(http.StatusOK)
	reserve := map[string]any{"kind": "opml", "fingerprint": hex.EncodeToString(make([]byte, 32)), "size": 4, "file_name": "subs.opml"}
	id := e.do(http.MethodPost, "/v1/import", reserve, browser(session)).must(http.StatusCreated).str("import_id")
	e.do(http.MethodPut, "/v1/import/"+id+"/chunks/0", map[string]any{"data": base64.StdEncoding.EncodeToString([]byte("12345"))},
		browser(session)).expect(http.StatusBadRequest, "invalid_request")
	e.srv.cfg.Imports.FreeBytes = func(string) (int64, error) { return 1 << 20, nil }
	e.do(http.MethodPut, "/v1/import/"+id+"/chunks/0", map[string]any{"data": base64.StdEncoding.EncodeToString([]byte("1234"))},
		browser(session)).expect(http.StatusInsufficientStorage, "insufficient_space")
	e.do(http.MethodPost, "/v1/import/"+id+"/finish", nil, browser(session)).expect(http.StatusBadRequest, "invalid_request")
}

func itoa(n int) string {
	return string(rune('0' + n))
}
