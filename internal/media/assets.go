package media

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
)

// Asset kinds (PROTOCOL §13).
const (
	AssetImage      = "image"
	AssetChapters   = "chapters"
	AssetTranscript = "transcript"
)

var safeImage = regexp.MustCompile(`^image/[a-z0-9][a-z0-9.+-]{0,63}$`)

// imageType is an image type the browser may show from the app's origin:
// never SVG (a document that can carry script) or anything XML.
func imageType(v string) string {
	mt, _, err := mime.ParseMediaType(v)
	if err != nil {
		return ""
	}
	mt = strings.ToLower(mt)
	if !safeImage.MatchString(mt) || strings.Contains(mt, "svg") || strings.Contains(mt, "xml") {
		return ""
	}
	return mt
}

// Asset fetches an episode's artwork, chapters or transcript for a browser
// that cannot fetch it itself (PROTOCOL §13), with the relay's discipline:
// the fetch rules, credentials only to approved origins, and the media
// response headers. Images are image/* (never SVG) up to 10 MiB; chapters
// are JSON and transcripts JSON or text, up to 5 MiB, served as
// application/json or text/plain. declared is the transcript's type. It
// returns an error only when it has written nothing.
//
// The caller has authorized the request. Assets are served private,
// no-cache with an ETag, so a browser revalidates every reuse and never
// shows one account's copy to another; a matching If-None-Match answers 304 without a fetch.
func (s *Service) Asset(w http.ResponseWriter, r *http.Request, account, kind, source, declared string, creds *feed.Credentials) *Error {
	etag := s.assetETag(account, kind, source, declared, creds)
	if matchesETag(r.Header.Get("If-None-Match"), etag) {
		h := w.Header()
		h.Set("Cache-Control", assetCacheControl)
		h.Set("ETag", etag)
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	data, header, e := s.fetchAsset(r.Context(), account, kind, source, creds)
	if e != nil {
		return e
	}
	var ctype string
	switch kind {
	case AssetImage:
		ctype = imageType(header.Get("Content-Type"))
		if ctype == "" {
			ctype = imageType(http.DetectContentType(data))
		}
	case AssetChapters:
		if json.Valid(data) {
			ctype = "application/json"
		}
	case AssetTranscript:
		if strings.Contains(declared, "json") {
			if json.Valid(data) {
				ctype = "application/json"
			}
		} else if utf8.Valid(bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))) {
			ctype = "text/plain; charset=utf-8"
		}
	}
	if ctype == "" {
		return newError(http.StatusBadGateway, "media_unavailable", "unsupported_type")
	}
	// The browser's end is an ordinary response under the server's write
	// deadline; a request that goes away interrupts its write at once.
	rc := http.NewResponseController(w)
	stop := context.AfterFunc(r.Context(), func() { _ = rc.SetWriteDeadline(time.Now()) })
	defer stop()
	h := w.Header()
	writeMediaHeaders(h, ctype)
	h.Set("Cache-Control", assetCacheControl)
	h.Set("ETag", etag)
	h.Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(data)
	}
	return nil
}

// fetchAsset reads an asset from its publisher under the account's asset
// slot, released as soon as the fetch ends: a browser slow to read the
// answer never holds a slot.
func (s *Service) fetchAsset(ctx context.Context, account, kind, source string, creds *feed.Credentials) ([]byte, http.Header, *Error) {
	release, ok := s.takeAsset(ctx, account)
	if !ok {
		return nil, nil, errAccountMax
	}
	defer release()
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Limits.AssetTotal)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	resp, _, err := s.cfg.Fetcher.OpenMedia(ctx, feed.MediaRequest{URL: source, Credentials: creds})
	if err != nil {
		return nil, nil, fetchFailure(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, statusFailure(resp.StatusCode)
	}
	limit := s.cfg.Limits.TextMax
	if kind == AssetImage {
		limit = s.cfg.Limits.ImageMax
	}
	body, derr := decoded(resp.Body, encodingOf(resp.Header))
	if derr != nil {
		return nil, nil, derr
	}
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, nil, fetchFailure(err)
	}
	if int64(len(data)) > limit {
		return nil, nil, newError(http.StatusBadGateway, "media_unavailable", "size_limit")
	}
	return data, resp.Header, nil
}

// assetCacheControl makes a browser revalidate an asset before every reuse.
const assetCacheControl = "private, no-cache"

// assetETag names what this account gets for this asset within the current
// hour: its source, kind and credentials. A revalidation within the hour
// answers 304 without a fetch; the next hour fetches again, as the earlier
// max-age=3600 did.
func (s *Service) assetETag(account, kind, source, declared string, creds *feed.Credentials) string {
	revision := ""
	if creds != nil {
		revision = creds.Revision
	}
	h := sha256.New()
	for _, part := range []string{account, kind, source, declared, revision, strconv.FormatInt(s.now().Unix()/3600, 10)} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return `"` + hex.EncodeToString(h.Sum(nil)[:16]) + `"`
}

// matchesETag reports whether an If-None-Match list names etag (weak
// comparison, as RFC 9110 asks for If-None-Match).
func matchesETag(header, etag string) bool {
	for _, v := range strings.Split(header, ",") {
		if strings.TrimPrefix(strings.TrimSpace(v), "W/") == etag {
			return true
		}
	}
	return false
}

// takeAsset admits an asset fetch: a few at a time per account, the rest
// waiting (a list of episodes asks for many images at once).
func (s *Service) takeAsset(ctx context.Context, account string) (func(), bool) {
	s.assetMu.Lock()
	sem := s.assets[account]
	if sem == nil {
		sem = make(chan struct{}, s.cfg.Limits.AssetsPerAccount)
		s.assets[account] = sem
	}
	s.assetMu.Unlock()
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, true
	case <-ctx.Done():
		return nil, false
	case <-s.ctx.Done():
		return nil, false
	}
}
