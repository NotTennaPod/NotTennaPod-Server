package api

// Browser media through the API (PROTOCOL §13): the descriptor for an
// episode the account holds, token media URLs that authenticate with the
// session cookie alone and belong to one session, their status, account
// isolation, credentials only to approved origins, the metadata assets, and
// the administrators' view of the compatibility path. The podcast is
// refreshed from a fixture publisher by the real refresher.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/media"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/refresh"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

func init() {
	errorStatus["media_changed"] = []int{412}
	errorStatus["media_unavailable"] = []int{502, 504, 507}
	errorStatus["media_unauthorized"] = []int{502}
	errorStatus["media_not_found"] = []int{502}
	errorStatus["media_refused"] = []int{502}
}

// mediaPublisher serves a feed, its media (with ranges and a strong ETag),
// artwork and chapters, and a private feed behind Basic credentials; it
// records what each request carried.
type mediaPublisher struct {
	srv   *httptest.Server
	audio []byte

	mu        sync.Mutex
	seen      []http.Header
	path      []string
	ep1       string // ep1's enclosure path
	enclosure string // ep1's enclosure at another host, when set
	epImage   string // art.xml's episode artwork path ("" for none)
}

var png1x1 = []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89")

func newMediaPublisher(t *testing.T) *mediaPublisher {
	p := &mediaPublisher{audio: bytes.Repeat([]byte("ID3-not-really-mp3-"), 20000), ep1: "/ep1.mp3"}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		p.seen = append(p.seen, r.Header.Clone())
		p.path = append(p.path, r.URL.Path)
		ep1, elsewhere := p.ep1, p.enclosure
		p.mu.Unlock()
		private := strings.HasPrefix(r.URL.Path, "/private/")
		if private {
			if u, pw, ok := r.BasicAuth(); !ok || u != "listener" || pw != "feed-password" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		switch r.URL.Path {
		case "/feed.xml", "/private/feed.xml":
			w.Header().Set("Content-Type", "application/rss+xml")
			base := p.srv.URL
			if private {
				base += "/private"
			}
			enclosure := base + ep1
			if elsewhere != "" {
				enclosure = elsewhere
			}
			fmt.Fprintf(w, `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel><title>Show</title><link>%[1]s/</link><image><url>%[1]s/cover.png</url></image>
<item><title>One</title><guid>one</guid><pubDate>Mon, 05 Oct 2026 10:00:00 GMT</pubDate>
<enclosure url="%[2]s" length="%[3]d" type="audio/mpeg"/>
<podcast:chapters url="%[1]s/chapters.json" type="application/json+chapters"/></item>
</channel></rss>`, base, enclosure, len(p.audio))
		case "/art.xml":
			// An episode with its own artwork (when set) and a transcript.
			p.mu.Lock()
			image := p.epImage
			p.mu.Unlock()
			art := ""
			if image != "" {
				art = fmt.Sprintf(`<itunes:image href="%s%s"/>`, p.srv.URL, image)
			}
			w.Header().Set("Content-Type", "application/rss+xml")
			fmt.Fprintf(w, `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel><title>Art</title><link>%[1]s/</link><image><url>%[1]s/cover.png</url></image>
<item><title>Drawn</title><guid>drawn</guid><pubDate>Mon, 05 Oct 2026 10:00:00 GMT</pubDate>%[2]s
<enclosure url="%[1]s/ep1.mp3" length="%[3]d" type="audio/mpeg"/>
<podcast:transcript url="%[1]s/transcript.vtt" type="text/vtt"/></item>
</channel></rss>`, p.srv.URL, art, len(p.audio))
		case "/ep-cover.png", "/ep-cover-2.png":
			w.Header().Set("Content-Type", "image/png")
			w.Write(append(append([]byte(nil), png1x1...), r.URL.Path...))
		case "/transcript.vtt":
			w.Header().Set("Content-Type", "text/vtt")
			w.Write([]byte("WEBVTT\n\n00:00.000 --> 00:01.000\nHello\n"))
		case "/ep1.mp3", "/ep1-moved.mp3", "/private/ep1.mp3":
			w.Header().Set("ETag", `"audio-v1"`)
			http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(p.audio))
		case "/cover.png", "/private/cover.png":
			w.Header().Set("Content-Type", "image/png")
			w.Write(png1x1)
		case "/chapters.json", "/private/chapters.json":
			w.Header().Set("Content-Type", "application/json+chapters")
			w.Write([]byte(`{"version":"1.2.0","chapters":[{"startTime":0,"title":"Start"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *mediaPublisher) requests() ([]string, []http.Header) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.path...), append([]http.Header(nil), p.seen...)
}

type mediaEnv struct {
	*readEnv
	pub     *mediaPublisher
	fetcher *feed.Fetcher
	phone   *phone
	ns      string
}

func newMediaEnv(t *testing.T) *mediaEnv {
	fetcher := feed.NewFetcher()
	fetcher.AllowLoopback(func(string) bool { return true })
	e := newEnv(t, withReads(t), func(c *Config) {
		m, err := media.New(media.Config{Fetcher: fetcher, SpoolDir: filepath.Join(c.Store.Dir(), "media-spool"), DataDir: c.Store.Dir(),
			NoTools: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		if err != nil {
			t.Fatal(err)
		}
		m.Verify(context.Background())
		t.Cleanup(m.Close)
		c.Media = m
	})
	session, account := e.setupAdmin("owner", "correct horse battery")
	e.registerWeb(session, newID(), newID()).must(http.StatusOK)
	me := &mediaEnv{readEnv: &readEnv{env: e, session: session, account: account, web: browser(session)},
		pub: newMediaPublisher(t), fetcher: fetcher}
	me.phone = e.phoneOf("owner", "correct horse battery")
	e.store.Read(context.Background(), func(r *storage.ReadTx) error {
		a, err := r.GetAccount(account)
		me.ns = a.Namespace
		return err
	})
	return me
}

// subscribe subscribes the account through its phone (with credentials
// when given), refreshes the podcast with the real refresher, and returns
// the podcast and its episode.
func (m *mediaEnv) subscribe(feedURL string, secret map[string]any) (podcast, generation, episode string) {
	m.t.Helper()
	p := m.phone
	p.seq++
	payload := map[string]any{"feed_url": feedURL}
	if secret != nil {
		payload["credentials_secret"] = secret["secret_id"]
	}
	fact := map[string]any{"op_id": newID(), "author_id": p.author, "author_seq": p.seq, "dependencies": []any{}, "type": "podcast.subscribe",
		"type_version": 1, "client_time": time.Now().UnixMilli(), "targets": []any{}, "payload": payload}
	body := p.pushBody(op{seq: p.seq, fact: fact})
	if secret != nil {
		body["secrets"] = []any{secret}
	}
	if got := p.statuses(m.do(http.MethodPost, "/v1/push", body, p.cred)); len(got) != 1 || got[0] != "accepted" {
		m.t.Fatalf("subscribe: %v", got)
	}
	p.acked = p.seq
	podcast = contract.PodcastID(m.ns, fact["op_id"].(string))
	generation = contract.GenerationID(m.ns, podcast, fact["op_id"].(string))
	m.refresh(podcast)
	return podcast, generation, m.episodeOf(podcast)
}

func (m *mediaEnv) refresh(podcast string) {
	m.t.Helper()
	r := refresh.New(m.srv.cfg.Protocol, refresh.Config{Fetcher: m.fetcher})
	if _, err := r.Request(context.Background(), m.account, podcast); err != nil {
		m.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx)
	defer func() { cancel(); r.Stop() }()
	deadline := time.Now().Add(20 * time.Second)
	for m.episodeOf(podcast) == "" {
		if time.Now().After(deadline) {
			m.t.Fatal("the refresh did not bring the episode")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (m *mediaEnv) episodeOf(podcast string) string {
	r := m.get("/v1/read/podcasts/" + podcast + "/episodes").must(http.StatusOK)
	if list := items(m.t, r); len(list) > 0 {
		id, _ := list[0]["episode_id"].(string)
		return id
	}
	return ""
}

// raw serves a request whose answer is media, not JSON.
func (m *mediaEnv) raw(method, path string, opts ...opt) *httptest.ResponseRecorder {
	m.t.Helper()
	req := httptest.NewRequest(method, testOrigin+path, nil)
	req.RemoteAddr = loopbackPeer
	for _, o := range opts {
		o(req)
	}
	rec := httptest.NewRecorder()
	m.srv.ServeHTTP(rec, req)
	return rec
}

func (m *mediaEnv) play(episode string, o opt, body map[string]any) *response {
	m.t.Helper()
	if body == nil {
		body = map[string]any{"capabilities": map[string]any{"audio/mpeg": "probably"}}
	}
	return m.do(http.MethodPost, "/v1/media/"+episode+"/play", body, o)
}

func TestMediaAPI(t *testing.T) {
	m := newMediaEnv(t)
	podcast, generation, ep := m.subscribe(m.pub.srv.URL+"/feed.xml", nil)

	// The descriptor: an http: enclosure plays through the relay.
	d := m.play(ep, m.web, nil).must(http.StatusOK)
	url, token := d.str("url"), d.str("token")
	if d.str("mode") != "relay" || url != "/v1/media/s/"+token || len(token) != 32 || d.body["seekable"] != true ||
		d.str("representation") != "strong" || d.str("type") != "audio/mpeg" {
		t.Fatalf("descriptor %s", d.raw)
	}
	// The media request carries the session cookie alone, as <audio> does.
	rec := m.raw(http.MethodGet, url, cookieOnly(m.session), hdr("Range", "bytes=100-199"))
	if rec.Code != 206 || !bytes.Equal(rec.Body.Bytes(), m.pub.audio[100:200]) || rec.Header().Get("Content-Range") != fmt.Sprintf("bytes 100-199/%d", len(m.pub.audio)) {
		t.Fatalf("range %d %v", rec.Code, rec.Header())
	}
	h := rec.Header()
	if h.Get("Content-Type") != "audio/mpeg" || h.Get("Content-Security-Policy") != "sandbox; default-src 'none'" || h.Get("X-Content-Type-Options") != "nosniff" ||
		h.Get("Cross-Origin-Resource-Policy") != "same-origin" || h.Get("Content-Disposition") != "inline" || h.Get("Cache-Control") != "private, no-transform" {
		t.Fatalf("media headers %v", h)
	}
	if head := m.raw(http.MethodHead, url, cookieOnly(m.session)); head.Code != 200 || head.Body.Len() != 0 || head.Header().Get("Accept-Ranges") != "bytes" {
		t.Fatalf("HEAD %d %v", head.Code, head.Header())
	}
	if all := m.raw(http.MethodGet, url, cookieOnly(m.session)); all.Code != 200 || !bytes.Equal(all.Body.Bytes(), m.pub.audio) {
		t.Fatalf("whole %d", all.Code)
	}
	st := m.get(url + "/status").must(http.StatusOK)
	if st.str("state") != "complete" || st.body["error"] != nil {
		t.Fatalf("status %s", st.raw)
	}
	// Nothing of the browser's reaches the publisher.
	_, seen := m.pub.requests()
	for _, hh := range seen {
		if hh.Get("Cookie") != "" || hh.Get("Authorization") != "" || hh.Get("Origin") != "" || hh.Get("User-Agent") != "NotTennaPod-Server" {
			t.Fatalf("an upstream request carried %v", hh)
		}
	}

	// Refusals: unknown episodes, devices, other accounts and sessions.
	m.play("e1:"+strings.Repeat("0", 64), m.web, nil).expect(http.StatusNotFound, "not_found")
	m.play(ep, bearer(m.phone.token), nil).expect(http.StatusUnauthorized, "unauthorized")
	if rec := m.raw(http.MethodGet, url, bearer(m.phone.token)); rec.Code != http.StatusUnauthorized {
		t.Fatalf("a device token on a media URL: %d", rec.Code)
	}
	m.play(ep, func(r *http.Request) { browser(m.session)(r); r.Header.Set("X-NotTennaPod-Account", "another") }, nil).
		expect(http.StatusConflict, "account_changed")
	m.play(ep, m.web, map[string]any{"capabilities": map[string]any{"audio/mpeg": "definitely"}}).expect(http.StatusBadRequest, "invalid_request")
	m.play(ep, m.web, map[string]any{"capabilities": map[string]any{}, "force": "remux"}).expect(http.StatusBadRequest, "invalid_request")
	m.play(ep, m.web, map[string]any{"capabilities": map[string]any{}, "url": "http://elsewhere/"}).expect(http.StatusBadRequest, "invalid_request")

	admin := m.session
	m.createAccount(admin, "other", "other-password-1", false)
	other := m.env.session("other", "other-password-1")
	m.registerWeb(other, newID(), newID()).must(http.StatusOK)
	m.play(ep, browser(other), nil).expect(http.StatusNotFound, "not_found")
	if rec := m.raw(http.MethodGet, url, cookieOnly(other)); rec.Code != http.StatusNotFound {
		t.Fatalf("another account's session on the token: %d", rec.Code)
	}
	m.get(url + "/status").must(http.StatusOK)
	m.do(http.MethodGet, url+"/status", nil, browser(other)).expect(http.StatusNotFound, "not_found")
	second := m.env.session("owner", "correct horse battery")
	m.registerWeb(second, newID(), newID()).must(http.StatusOK)
	if rec := m.raw(http.MethodGet, url, cookieOnly(second)); rec.Code != http.StatusNotFound {
		t.Fatalf("another session of the account on the token: %d", rec.Code)
	}
	if rec := m.raw(http.MethodGet, "/v1/media/s/"+strings.Repeat("ab", 16), cookieOnly(m.session)); rec.Code != http.StatusNotFound {
		t.Fatalf("an unknown token: %d", rec.Code)
	}

	// Assets: the episode's artwork (its podcast's), chapters, and the podcast's artwork.
	for _, path := range []string{"/v1/media/" + ep + "/image", "/v1/media/podcasts/" + podcast + "/image"} {
		rec := m.raw(http.MethodGet, path, cookieOnly(m.session))
		if rec.Code != 200 || rec.Header().Get("Content-Type") != "image/png" || !bytes.Equal(rec.Body.Bytes(), png1x1) ||
			rec.Header().Get("Content-Security-Policy") != "sandbox; default-src 'none'" {
			t.Fatalf("%s: %d %v", path, rec.Code, rec.Header())
		}
	}
	if rec := m.raw(http.MethodGet, "/v1/media/"+ep+"/chapters", cookieOnly(m.session)); rec.Code != 200 || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("chapters %d %v", rec.Code, rec.Header())
	}
	m.do(http.MethodGet, "/v1/media/"+ep+"/transcript", nil, m.web).expect(http.StatusNotFound, "not_found")
	m.do(http.MethodGet, "/v1/media/"+ep+"/other", nil, m.web).expect(http.StatusNotFound, "not_found")
	m.do(http.MethodGet, "/v1/media/podcasts/"+podcast+"/image", nil, browser(other)).expect(http.StatusNotFound, "not_found")

	// The capability list, and the compatibility path's check for administrators.
	types := m.get("/v1/media/types").must(http.StatusOK)
	if list, _ := types.body["capabilities"].([]any); len(list) != len(media.Capabilities) {
		t.Fatalf("types %s", types.raw)
	}
	if c := m.get("/v1/admin/media").must(http.StatusOK).obj("compat"); c["state"] != "unavailable" {
		t.Fatalf("admin media %v", c)
	}
	m.do(http.MethodGet, "/v1/admin/media", nil, browser(other)).expect(http.StatusForbidden, "forbidden")
	m.do(http.MethodPost, "/v1/admin/media/verify", nil, m.web).must(http.StatusAccepted)

	// Without ffmpeg, a type the browser cannot play is unsupported, with the reason.
	if d := m.play(ep, m.web, map[string]any{"capabilities": map[string]any{"audio/mpeg": ""}}).must(http.StatusOK); d.str("mode") != "unsupported" || d.str("reason") != "no_ffmpeg" {
		t.Fatalf("unplayable %s", d.raw)
	}

	// The enclosure moves: the old token's requests fail with media_changed.
	m.pub.mu.Lock()
	m.pub.ep1 = "/ep1-moved.mp3"
	m.pub.mu.Unlock()
	m.refreshUntil(podcast, func() bool {
		r := m.get("/v1/read/episodes/" + ep).must(http.StatusOK)
		return strings.Contains(string(r.raw), "ep1-moved.mp3")
	})
	if rec := m.raw(http.MethodGet, url, cookieOnly(m.session), hdr("Range", "bytes=0-9")); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("after the enclosure moved: %d %s", rec.Code, rec.Body)
	}
	if st := m.get(url + "/status").must(http.StatusOK); st.str("state") != "failed" || st.obj("error")["error"] != "media_changed" {
		t.Fatalf("status after the move %s", st.raw)
	}

	// Unsubscribed: the episode is not the account's any more.
	d = m.play(ep, m.web, nil).must(http.StatusOK)
	m.phone.seq++
	unsub := map[string]any{"op_id": newID(), "author_id": m.phone.author, "author_seq": m.phone.seq, "dependencies": []any{}, "type": "podcast.unsubscribe",
		"type_version": 1, "client_time": time.Now().UnixMilli(), "targets": []any{map[string]any{"entity": "podcast", "id": podcast, "generation": generation}},
		"payload": map[string]any{}}
	if got := m.phone.statuses(m.phone.push(op{seq: m.phone.seq, fact: unsub})); len(got) != 1 || got[0] != "accepted" {
		t.Fatalf("unsubscribe %v", got)
	}
	if rec := m.raw(http.MethodGet, d.str("url"), cookieOnly(m.session)); rec.Code != http.StatusNotFound {
		t.Fatalf("after unsubscribing: %d", rec.Code)
	}
	m.play(ep, m.web, nil).expect(http.StatusNotFound, "not_found")
}

func (m *mediaEnv) refreshUntil(podcast string, done func() bool) {
	m.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			m.t.Fatal("the refresh did not bring the change")
		}
		r := refresh.New(m.srv.cfg.Protocol, refresh.Config{Fetcher: m.fetcher})
		ctx, cancel := context.WithCancel(context.Background())
		r.Start(ctx)
		if _, err := r.Request(ctx, m.account, podcast); err != nil {
			m.t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		cancel()
		r.Stop()
	}
}

// A private feed: its media plays through the relay, with the credentials
// sent only to the origin they were given for (the feed's, for a secret
// without approved origins), after the publisher's 401.
func TestMediaAPICredentials(t *testing.T) {
	m := newMediaEnv(t)
	secret := map[string]any{"secret_id": newID(), "username": "listener", "password": "feed-password"}
	_, _, ep := m.subscribe(m.pub.srv.URL+"/private/feed.xml", secret)
	d := m.play(ep, m.web, nil).must(http.StatusOK)
	if d.str("mode") != "relay" {
		t.Fatalf("descriptor %s", d.raw)
	}
	rec := m.raw(http.MethodGet, d.str("url"), cookieOnly(m.session), hdr("Range", "bytes=0-9"))
	if rec.Code != 206 || !bytes.Equal(rec.Body.Bytes(), m.pub.audio[:10]) {
		t.Fatalf("private media %d %s", rec.Code, rec.Body)
	}
	if rec := m.raw(http.MethodGet, "/v1/media/"+ep+"/chapters", cookieOnly(m.session)); rec.Code != 200 {
		t.Fatalf("private chapters %d", rec.Code)
	}
	paths, seen := m.pub.requests()
	sent := false
	for i, h := range seen {
		if h.Get("Authorization") != "" {
			sent = true
			if !strings.HasPrefix(paths[i], "/private/") {
				t.Fatalf("credentials sent for %s", paths[i])
			}
		}
		if h.Get("Cookie") != "" {
			t.Fatalf("the browser's cookie reached the publisher")
		}
	}
	if !sent {
		t.Fatal("the credentials were never sent")
	}
	// The secret never appears in what the browser gets.
	if strings.Contains(string(d.raw), "feed-password") {
		t.Fatal("the descriptor shows the password")
	}
}

// An enclosure with a media fragment (#t=0) plays: the token's re-check
// compares the URL as the relay requests it.
func TestMediaAPIFragment(t *testing.T) {
	m := newMediaEnv(t)
	m.pub.ep1 = "/ep1.mp3#t=0"
	_, _, ep := m.subscribe(m.pub.srv.URL+"/feed.xml", nil)
	if r := m.get("/v1/read/episodes/" + ep).must(http.StatusOK); !strings.Contains(string(r.raw), "ep1.mp3#t=0") {
		t.Fatalf("the fixture's enclosure lost its fragment: %s", r.raw)
	}
	d := m.play(ep, m.web, nil).must(http.StatusOK)
	if d.str("mode") != "relay" {
		t.Fatalf("descriptor %s", d.raw)
	}
	rec := m.raw(http.MethodGet, d.str("url"), cookieOnly(m.session))
	if rec.Code != 200 || !bytes.Equal(rec.Body.Bytes(), m.pub.audio) {
		t.Fatalf("a fresh token for an unchanged enclosure: %d %s", rec.Code, rec.Body.String()[:min(rec.Body.Len(), 200)])
	}
}

// An https enclosure the browser says it plays is direct at once: the server
// asks the publisher nothing. After the browser reports that direct playback
// failed, the account's later descriptors of it are never direct again
// (here the relay cannot reach the host either, so they fail).
func TestMediaAPIFastDirect(t *testing.T) {
	m := newMediaEnv(t)
	var mu sync.Mutex
	conns := 0
	host := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/mpeg")
		w.Write(m.pub.audio)
	}))
	host.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateNew {
			mu.Lock()
			conns++
			mu.Unlock()
		}
	}
	host.StartTLS()
	t.Cleanup(host.Close)
	contacted := func() int {
		mu.Lock()
		defer mu.Unlock()
		return conns
	}
	m.pub.enclosure = host.URL + "/ep1.mp3"
	_, _, ep := m.subscribe(m.pub.srv.URL+"/feed.xml", nil)
	d := m.play(ep, m.web, nil).must(http.StatusOK)
	if d.str("mode") != "direct" || d.str("url") != host.URL+"/ep1.mp3" || d.str("type") != "audio/mpeg" || d.body["token"] != nil || d.body["seekable"] != true {
		t.Fatalf("descriptor %s", d.raw)
	}
	if n := contacted(); n != 0 {
		t.Fatalf("the server contacted the publisher %d times for a direct descriptor", n)
	}
	body := map[string]any{"capabilities": map[string]any{"audio/mpeg": "probably"}, "direct_failed": true}
	if d := m.play(ep, m.web, body).must(http.StatusOK); d.str("mode") == "direct" {
		t.Fatalf("after direct_failed %s", d.raw)
	}
	if contacted() == 0 {
		t.Fatal("after direct_failed the server did not pin the representation")
	}
	if d := m.play(ep, m.web, nil).must(http.StatusOK); d.str("mode") == "direct" {
		t.Fatalf("an ordinary request after the failure %s", d.raw)
	}
}

// An episode's artwork is its own when its feed gives one, else its
// podcast's, and follows the feed as it changes; a declared transcript is
// served. Each request reaches the publisher at the artwork it names.
func TestEpisodeArtworkAndTranscript(t *testing.T) {
	m := newMediaEnv(t)
	set := func(image string) {
		m.pub.mu.Lock()
		m.pub.epImage = image
		m.pub.mu.Unlock()
	}
	set("/ep-cover.png")
	podcast, _, ep := m.subscribe(m.pub.srv.URL+"/art.xml", nil)
	image := func(path string) []byte {
		t.Helper()
		rec := m.raw(http.MethodGet, path, cookieOnly(m.session))
		if rec.Code != 200 || rec.Header().Get("Content-Type") != "image/png" {
			t.Fatalf("%s: %d %v", path, rec.Code, rec.Header())
		}
		return rec.Body.Bytes()
	}
	art := func(path string) []byte { return append(append([]byte(nil), png1x1...), path...) }
	if got := image("/v1/media/" + ep + "/image"); !bytes.Equal(got, art("/ep-cover.png")) {
		t.Fatalf("the episode's own artwork: %q", got)
	}
	if got := image("/v1/media/podcasts/" + podcast + "/image"); !bytes.Equal(got, png1x1) {
		t.Fatalf("the podcast's artwork: %q", got)
	}
	rec := m.raw(http.MethodGet, "/v1/media/"+ep+"/transcript", cookieOnly(m.session))
	if rec.Code != 200 || !strings.HasPrefix(rec.Body.String(), "WEBVTT") {
		t.Fatalf("the transcript: %d %q", rec.Code, rec.Body.String())
	}
	// The feed changes the artwork, then drops it: after each refresh the
	// episode's artwork follows, and without its own it is the podcast's.
	for _, c := range []struct{ image, want string }{{"/ep-cover-2.png", "/ep-cover-2.png"}, {"", ""}} {
		set(c.image)
		m.refreshUntil(podcast, func() bool {
			r := m.get("/v1/read/episodes/" + ep).must(http.StatusOK)
			meta, _ := r.obj("episode")["records"].(map[string]any)["metadata"].(map[string]any)["value"].(map[string]any)
			img, _ := meta["image_url"].(string)
			return img == map[bool]string{true: m.pub.srv.URL + c.image, false: ""}[c.image != ""]
		})
		want := png1x1
		if c.want != "" {
			want = art(c.want)
		}
		if got := image("/v1/media/" + ep + "/image"); !bytes.Equal(got, want) {
			t.Fatalf("after the feed's artwork became %q: %q", c.image, got)
		}
	}
}
