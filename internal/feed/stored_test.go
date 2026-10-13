package feed

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// A private feed whose enclosure URLs carry credentials goes through sealed
// storage, never in the clear, and re-parses to the same result.
func TestStoredResponseSealedAndReparsed(t *testing.T) {
	const secret = "hunter2-enclosure-password"
	body := []byte(`<?xml version="1.0"?><rss version="2.0"><channel><title>Private</title><item><title>One</title>` +
		`<enclosure url="https://me:` + secret + `@cdn.example.com/1.mp3" length="20000" type="audio/mpeg"/></item></channel></rss>`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != "me" || p != "pw" {
			w.WriteHeader(401)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Write(body)
	}))
	defer srv.Close()
	f := testFetcher()
	creds := &Credentials{Username: "me", Password: "pw", Revision: "s1"}
	u, _ := http.NewRequest("GET", srv.URL, nil)
	creds.ApprovedOrigins = []string{Origin(u.URL)}
	res, err := f.Fetch(context.Background(), FetchRequest{URL: srv.URL + "/feed", Credentials: creds})
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(corpusNow)
	first, err := Parse(context.Background(), res.Body, srv.URL+"/feed", now)
	if err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(t.TempDir(), "data")
	store, err := storage.Open(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	box, err := sealed.LoadOrCreate(store.Dir())
	if err != nil {
		t.Fatal(err)
	}
	stored := StoredResponse{RepresentationKey: res.RepresentationKey, ETag: res.ETag, Source: srv.URL + "/feed",
		Now: now.UnixMilli(), ParserVersion: ParserVersion, Body: res.Body}
	var account string
	err = store.Write(context.Background(), func(tx *storage.WriteTx) error {
		a, err := tx.CreateAccount()
		if err != nil {
			return err
		}
		account = a.ID
		acct := tx.Account(a.ID)
		if err := acct.CreatePodcast("p1:x", "op"); err != nil {
			return err
		}
		sealedBytes, err := stored.Seal(box, a.ID, "p1:x")
		if err != nil {
			return err
		}
		if bytes.Contains(sealedBytes, []byte(secret)) || bytes.Contains(sealedBytes, []byte("Private")) {
			t.Error("the sealed response holds the body in the clear")
		}
		return acct.PutFeedResponse("p1:x", sealedBytes)
	})
	if err != nil {
		t.Fatal(err)
	}
	// Nothing in the database file holds the body or its credentials.
	store.Close()
	for _, name := range []string{"nottennapod.db", "nottennapod.db-wal"} {
		if b, err := readIfExists(filepath.Join(dir, name)); err == nil && bytes.Contains(b, []byte(secret)) {
			t.Errorf("%s holds the enclosure credentials in the clear", name)
		}
	}
	store, err = storage.Open(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	var again StoredResponse
	err = store.Write(context.Background(), func(tx *storage.WriteTx) error {
		sealedBytes, err := tx.Account(account).FeedResponse("p1:x")
		if err != nil {
			return err
		}
		if _, err := OpenStoredResponse(box, account, "p1:other", sealedBytes); err == nil {
			t.Error("a stored response opened as another podcast's")
		}
		again, err = OpenStoredResponse(box, account, "p1:x", sealedBytes)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.RepresentationKey != stored.RepresentationKey || again.ETag != `"v1"` || again.Source != stored.Source ||
		again.Now != stored.Now || again.ParserVersion != ParserVersion || !bytes.Equal(again.Body, body) {
		t.Fatalf("stored response changed: %+v", again)
	}
	reparsed, err := Parse(context.Background(), again.Body, again.Source, time.UnixMilli(again.Now))
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(reparsed)
	if !bytes.Equal(a, b) {
		t.Errorf("re-parse differs:\n%s\n%s", a, b)
	}
}

func readIfExists(path string) ([]byte, error) { return os.ReadFile(path) }
