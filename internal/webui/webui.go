// Package webui serves the browser app built from NotTennaPod-Web and
// embedded in the binary (PROTOCOL §12): hashed assets are cached for good,
// index.html is revalidated, and any other app path serves index.html for
// client-side routing.
package webui

import (
	"embed"
	"io/fs"
	"mime"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/httpgzip"
)

// dist holds the app installed from NotTennaPod-Web. Only its empty .keep is
// tracked, so a fresh checkout compiles and the installed files stay ignored.
//
//go:embed all:dist
var dist embed.FS

// placeholder is served until an app is installed.
//
//go:embed placeholder
var placeholder embed.FS

// Handler serves the embedded app, or the placeholder page without one.
func Handler() http.Handler {
	root, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err)
	}
	if _, err := fs.Stat(root, "index.html"); err != nil {
		if root, err = fs.Sub(placeholder, "placeholder"); err != nil {
			panic(err)
		}
	}
	return handler{root}
}

// DirHandler serves an app build from a directory instead (development and
// the web client's journeys: a fresh build without rebuilding the server).
func DirHandler(dir string) http.Handler {
	return handler{os.DirFS(dir)}
}

type handler struct{ root fs.FS }

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "Method not allowed.", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if name == "" {
		name = "index.html"
	}
	data, err := fs.ReadFile(h.root, name)
	if err != nil {
		// Not a file: an app route, which the app resolves itself.
		name = "index.html"
		if data, err = fs.ReadFile(h.root, name); err != nil {
			http.Error(w, "Not found.", http.StatusNotFound)
			return
		}
	}
	ct := mime.TypeByExtension(path.Ext(name))
	if ct == "" {
		ct = "application/octet-stream"
	}
	hdr := w.Header()
	hdr.Set("Content-Type", ct)
	if strings.HasPrefix(name, "assets/") {
		hdr.Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		hdr.Set("Cache-Control", "no-cache")
	}
	// Text (the page, scripts, styles, SVG, JSON) is sent gzip-compressed to
	// a browser that accepts it; images and fonts are compressed already.
	if compressible(ct) {
		data = httpgzip.Negotiate(hdr, r, data)
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(data)
	}
}

func compressible(contentType string) bool {
	mt, _, _ := strings.Cut(contentType, ";")
	mt = strings.TrimSpace(mt)
	return strings.HasPrefix(mt, "text/") || mt == "image/svg+xml" ||
		strings.HasSuffix(mt, "javascript") || strings.HasSuffix(mt, "json") || strings.HasSuffix(mt, "xml")
}

// SecurityHeaders sets the headers every response carries (PROTOCOL §12): the content security
// policy is the second wall behind the show-note sanitizer.
func SecurityHeaders(h http.Header) {
	h.Set("Content-Security-Policy", CSP)
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("X-Content-Type-Options", "nosniff")
}

// CSP is the app's content security policy.
const CSP = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' https: data:; " +
	"media-src 'self' https: blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
