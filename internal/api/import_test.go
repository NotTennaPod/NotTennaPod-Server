package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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

// importOverHTTP uploads a backup the way the web does and follows it to its
// end, returning the final status.
func (e *env) importOverHTTP(session string, data []byte) *response {
	e.t.Helper()
	sum := sha256.Sum256(data)
	reserve := map[string]any{"kind": "android_db", "fingerprint": hex.EncodeToString(sum[:]), "size": len(data),
		"file_name": "AntennaPodBackup.db"}
	id := e.do(http.MethodPost, "/v1/import", reserve, browser(session)).must(http.StatusCreated).str("import_id")
	e.do(http.MethodPut, "/v1/import/"+id+"/chunks/0", map[string]any{"data": base64.StdEncoding.EncodeToString(data)},
		browser(session)).must(http.StatusOK)
	e.do(http.MethodPost, "/v1/import/"+id+"/finish", nil, browser(session)).must(http.StatusOK)
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := e.do(http.MethodGet, "/v1/import/"+id, nil, browser(session)).must(http.StatusOK)
		if s := got.str("status"); s == "published" || s == "failed" {
			return got
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("import still %s", got.str("status"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// An Android backup's stored chapters come back through the read API exactly
// as the backup holds them (PROTOCOL §11.5: SimpleChapters in id order, the
// flag and rows deciding; a merged episode keeps the first stored list among
// its rows), and the import result names what stays on the device:
// changed per-podcast settings and unplayed episodes stopped from downloading
// automatically.
func TestImportedChaptersAndWhatIsNotCarried(t *testing.T) {
	e := newEnv(t, withReads(t), withImports(t))
	session, account := e.setupAdmin("owner", "correct horse battery")
	e.registerWeb(session, newID(), newID()).must(http.StatusOK)
	r := &readEnv{env: e, session: session, account: account, web: browser(session)}
	data, err := os.ReadFile("../../contract/vectors/import/device-only.db")
	if err != nil {
		t.Fatal(err)
	}
	got := r.env.importOverHTTP(r.session, data)
	if got.str("status") != "published" {
		t.Fatalf("import %s", got.raw)
	}
	nc, _ := json.Marshal(got.obj("result")["not_carried"])
	want := `{"episode_auto_download_off":3,"podcasts":[{"settings":["custom_title","tags","playback_speed","skip_silence",` +
		`"skip_intro","skip_ending","volume_adaptation","episode_notification","auto_delete","auto_download_filter",` +
		`"episode_list_filter","sort_order"],"source_feed_id":2,"title":"My Settings Show",` +
		`"url":"https://settings.example.invalid/feed.xml"}]}`
	if string(nc) != want {
		t.Errorf("not carried %s\nwant        %s", nc, want)
	}
	list := r.get("/v1/read/episodes?filter=include_subscribed,include_archived,include_not_subscribed").must(http.StatusOK)
	byTitle := map[string]string{}
	for _, it := range items(t, list) {
		byTitle[it["title"].(string)] = it["episode_id"].(string)
	}
	for title, want := range map[string]string{
		"Chapters out of order": `[{"image_url":"https://chapters.example.invalid/2.jpg","link":"https://chapters.example.invalid/2","start":"60000","title":"Second"},` +
			`{"image_url":null,"link":null,"start":"0","title":"First"},{"image_url":null,"link":null,"start":"60000","title":"Second, again"}]`,
		"Flag without rows":     `null`, // DBReader: no rows is no chapters
		"Stale rows, flag 0":    `null`,
		"Stale rows, flag NULL": `null`,
		// Merged episodes (titled by their first row in date order) keep the
		// first stored list among their rows.
		"Two rows (first)":                `[{"image_url":null,"link":null,"start":"0","title":"Row seven"}]`,
		"Merged: no chapters first":       `[{"image_url":null,"link":null,"start":"0","title":"Second row's"}]`,
		"Merged: chapters first":          `[{"image_url":null,"link":null,"start":"0","title":"First row's"}]`,
		"Merged: flag without rows first": `[{"image_url":null,"link":null,"start":"5000","title":"After an empty flag"}]`,
		"Odd chapters": `[{"image_url":null,"link":null,"start":"0","title":null},` +
			`{"image_url":null,"link":"https://chapters.example.invalid/c","start":"1234567","title":"Precise"}]`,
	} {
		id := byTitle[title]
		if id == "" {
			t.Fatalf("no episode %q in %v", title, byTitle)
		}
		ep := r.get("/v1/read/episodes/" + id).must(http.StatusOK).obj("episode")
		meta, _ := ep["metadata"].(map[string]any)
		chapters, _ := json.Marshal(meta["chapters"])
		if string(chapters) != want {
			t.Errorf("%s: chapters %s\nwant %s", title, chapters, want)
		}
	}
}
