package feed

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf16"
)

// Feed discovery from an address, as Android's "add podcast by URL" (PROTOCOL §9.8):
// UrlChecker.prepareUrl, the download, then the feed, or FeedDiscoverer's
// links when the document is an HTML page.

// FeedLink is a feed a page links to: its URL and the label Android shows.
type FeedLink struct {
	URL, Title string
}

// DiscoverResult is what an address leads to: a feed, or a page's feed links,
// or the error Android shows (a page without links, or no feed at all).
type DiscoverResult struct {
	URL         string       // the prepared address fetched, without userinfo
	Credentials *Credentials // the ones used: given, or from the address's userinfo
	Fetch       *FetchResult // the download: redirect trace, final URL, permanent move
	Feed        *Feed
	Links       []FeedLink // in Android's order (ArrayMap)
	Err         *ParseError
}

// ErrAddress is an address Android cannot prepare (it crashes on these).
var ErrAddress = errors.New("feed: not a usable address")

// Discover prepares an address and fetches it. A 401 is a *FetchError with
// label "unauthorized" and the Origin that asked: the caller asks the user for
// credentials approved for that origin and calls Discover again with them.
func (f *Fetcher) Discover(ctx context.Context, address string, creds *Credentials, now time.Time) (*DiscoverResult, error) {
	prepared, err := PrepareURL(address)
	if err != nil {
		return nil, err
	}
	// One deadline for the fetch and the parse.
	ctx, cancel := context.WithTimeout(ctx, f.total)
	defer cancel()
	// Userinfo in the address is the user's credentials for its origin; it
	// never stays in a URL the server keeps.
	if u, err := url.Parse(prepared); err == nil && u.User != nil {
		if creds == nil {
			pass, _ := u.User.Password()
			creds = &Credentials{Username: u.User.Username(), Password: pass, ApprovedOrigins: []string{Origin(u)}}
		}
		u.User = nil
		prepared = u.String()
	}
	res, err := f.Fetch(ctx, FetchRequest{URL: prepared, Credentials: creds})
	if err != nil {
		return nil, err
	}
	out := &DiscoverResult{URL: prepared, Credentials: creds, Fetch: res}
	feed, err := Parse(ctx, res.Body, prepared, now)
	var pe *ParseError
	switch {
	case err == nil:
		out.Feed = feed
		return out, nil
	case errors.Is(err, context.DeadlineExceeded):
		return nil, &FetchError{Label: "time_limit"}
	case !errors.As(err, &pe):
		return nil, err
	}
	out.Err = pe
	if pe.Label == "unsupported_type_html" {
		text, derr := decodeDocument(res.Body)
		if derr != nil {
			text = string(res.Body)
		}
		out.Links = findLinks(ctx, text, res.FinalURL, prepared)
		if err := ctx.Err(); err != nil {
			return nil, &FetchError{Label: "time_limit"}
		}
		if len(out.Links) > 0 {
			out.Err = nil
		}
	}
	return out, nil
}

var subscribeOnAndroid = regexp.MustCompile(`((www.)?(subscribeonandroid.com/))`)

// PrepareURL is UrlChecker.prepareUrl: Java trim; podcast schemes and
// AntennaPod's subscribe links unwrapped; http:// added when the address has
// no http(s) scheme. Inputs on which Android crashes are ErrAddress.
func PrepareURL(address string) (string, error) {
	for n := 0; n < 64; n++ {
		u := javaTrim(address)
		lower := javaLowerUS(u)
		strip := func(prefix string) bool {
			if strings.HasPrefix(lower, prefix) && len(u) >= len(prefix) {
				address = u[len(prefix):]
				return true
			}
			return false
		}
		switch {
		case strip("feed://"), strip("pcast://"), strip("pcast:"), strip("antennapod-subscribe://"):
			continue
		case strings.HasPrefix(lower, "itpc"):
			if len(u) < len("itpc://") {
				return "", ErrAddress
			}
			address = u[len("itpc://"):] // Android drops seven characters after any "itpc"
			continue
		case strings.Contains(lower, "antennapod.org/deeplink/subscribe"):
			q, ok := androidQueryParameter(u, "url")
			if !ok {
				return "", ErrAddress
			}
			d, ok := javaURLDecode(q)
			if !ok {
				return "", ErrAddress
			}
			address = d
			continue
		case strings.Contains(lower, "subscribeonandroid.com"):
			if loc := subscribeOnAndroid.FindStringIndex(u); loc != nil {
				address = u[:loc[0]] + u[loc[1]:]
				continue
			}
		}
		if !strings.HasPrefix(lower, "http://") && !strings.HasPrefix(lower, "https://") {
			return "http://" + u, nil
		}
		return u, nil
	}
	return "", ErrAddress
}

// androidQueryParameter is Uri.getQueryParameter: the first parameter of that
// name in the query (before any fragment), decoded with "+" as a space.
func androidQueryParameter(u, name string) (string, bool) {
	q := u
	if i := strings.IndexByte(q, '#'); i >= 0 {
		q = q[:i]
	}
	i := strings.IndexByte(q, '?')
	if i < 0 {
		return "", false
	}
	for _, part := range strings.Split(q[i+1:], "&") {
		k, v, _ := strings.Cut(part, "=")
		if k == name {
			d, ok := javaURLDecode(v)
			return d, ok
		}
	}
	return "", false
}

// javaURLDecode is URLDecoder.decode(s, "UTF-8"): "+" is a space, %XX bytes
// are UTF-8; a malformed escape is an error.
func javaURLDecode(s string) (string, bool) {
	d, err := url.QueryUnescape(s)
	return d, err == nil
}

// findLinks is FeedDiscoverer.findLinks: the head's link elements with rel
// "alternate" or "feed", type application/rss+xml or application/atom+xml and
// a non-empty href, titled by their title or their href. Two adaptations
// (PROTOCOL §9.2): relative links resolve against the page per RFC 3986, and a link
// to the page itself is dropped (Android downloads the page again forever).
// Android's ArrayMap orders by the URL's hash code; a repeated URL keeps its
// place and takes the later title.
//
// The work is bounded: links are ordered once at the end (equal hashes keep
// their first appearance's order, as ArrayMap inserts after them), and ctx is
// checked as they are read.
// So is the result, which a hostile page could make the product of its link
// count and its URL's length: collecting stops before a distinct link past
// maxDiscoveredLinks, or once the URLs resolved, every occurrence counting
// (repeats, and links back to the page before they are dropped), pass
// maxDiscoveredBytes; the links collected until then are the result.
func findLinks(ctx context.Context, text, pageURL, requested string) []FeedLink {
	type entry struct {
		hash int32
		link FeedLink
	}
	var entries []entry
	at := map[string]int{} // URL → index in entries
	base, err := url.Parse(pageURL)
	n, resolved := 0, 0
	w := &jsoupWork{ctx: ctx} // the links' own work: escaping and resolving
	jsoupParseHead(ctx, text, func(a map[string]string) bool {
		if n++; n%checkEvery == 0 && ctx.Err() != nil {
			return false
		}
		rel, href, typ := a["rel"], a["href"], a["type"]
		if href == "" || rel != "alternate" && rel != "feed" || typ != "application/rss+xml" && typ != "application/atom+xml" {
			return true
		}
		u, ok, over := resolveLink(href, base, err, maxDiscoveredBytes-resolved, w)
		switch {
		case w.stopped:
			return false
		case !ok:
			return true
		case over: // it resolves past the budget: the end, as below
			return false
		}
		if resolved += len(u); resolved > maxDiscoveredBytes {
			return false
		}
		if u == pageURL || u == requested {
			return true
		}
		title := a["title"]
		if title == "" {
			title = href
		}
		if i, seen := at[u]; seen {
			entries[i].link.Title = title
			return true
		}
		if len(entries) == maxDiscoveredLinks {
			return false
		}
		at[u] = len(entries)
		entries = append(entries, entry{javaHashCode(u), FeedLink{u, title}})
		return true
	})
	if ctx.Err() != nil {
		return nil
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].hash < entries[j].hash })
	out := make([]FeedLink, len(entries))
	for i, e := range entries {
		out[i] = e.link
	}
	return out
}

// Discovery's bounds (PROTOCOL §9.2): the distinct feed links a page may offer,
// and the bytes of the link URLs resolved against the page, every occurrence
// counting. A page over them gives the links collected until then; real pages
// offer a handful.
const (
	maxDiscoveredLinks = 1000
	maxDiscoveredBytes = 4 << 20
)

// resolveLink resolves a link per RFC 3986 against the page (base, parsed
// once, or the error parsing it), as NotTennaPod-iOS: trimmed, and characters
// a URL cannot hold percent-encoded first. ok is false where url.Parse fails.
//
// One href can be as long as the page, and net/url parses, resolves and
// prints it without a check (a 64 MiB one takes seconds). A reference longer
// than linkDirect is therefore taken apart as url.Parse does (resolveLong):
// over reports, without resolving it, a link whose URL is longer than the
// remaining budget; any other is resolved by net/url from an equivalent
// short reference. w counts the work and stops it with its context.
func resolveLink(href string, base *url.URL, baseErr error, remaining int, w *jsoupWork) (u string, ok, over bool) {
	if baseErr != nil {
		return "", false, false
	}
	e := percentEncodeLink(strings.TrimSpace(href), w)
	if w.stopped {
		return "", false, false
	}
	if len(e) > linkDirect {
		return resolveLong(e, base, remaining, w)
	}
	u, ok = resolveDirect(e, base)
	return u, ok, false
}

// linkDirect is the longest escaped reference handed to net/url whole.
var linkDirect = 1 << 20

func resolveDirect(e string, base *url.URL) (string, bool) {
	r, err := url.Parse(e)
	if err != nil {
		return "", false
	}
	return base.ResolveReference(r).String(), true
}

// resolveLong is resolveLink for a long escaped reference e, which it takes
// apart as url.Parse (Go 1.27) does: the fragment after the first '#', a
// scheme as getScheme finds it, the query from the first '?', an authority
// after "//". Only the path can shrink when resolved (dot segments), and
// every other part prints at least a third of its bytes (an escape is three
// bytes for one), so the resolved path (linkResolvePath, net/url's own
// algorithm) and those thirds bound the URL's length from below. Over the
// remaining budget, over is reported; otherwise net/url resolves the same
// reference with its path already resolved, which is short. Where url.Parse
// fails, so does this: a missing scheme, a colon in a relative path's first
// segment, a malformed escape in the path or fragment, an invalid authority
// (url.Parse itself decides it, on the authority alone).
// TestResolveLongMatchesNetURL compares it with net/url.
func resolveLong(e string, base *url.URL, remaining int, w *jsoupWork) (u string, ok, over bool) {
	ref, frag := e, ""
	if k := strings.IndexByte(e, '#'); k >= 0 {
		ref, frag = e[:k], e[k+1:]
	}
	scheme, rest, ok := linkScheme(ref)
	if !ok {
		return "", false, false
	}
	query := ""
	if k := strings.IndexByte(rest, '?'); k >= 0 {
		rest, query = rest[:k], rest[k:]
	}
	if !linkEscapesValid(frag, w) || w.stopped {
		return "", false, false
	}
	lb := len(scheme) + len(query) + len(frag)/3 - 4 // what every shape prints at least
	if !strings.HasPrefix(rest, "/") {
		if scheme != "" { // opaque, printed as it is
			if lb += len(rest); lb > remaining {
				return "", true, true
			}
			u, ok = resolveDirect(e, base) // at most about three times remaining
			return u, ok, false
		}
		if segment, _, _ := strings.Cut(rest, "/"); strings.Contains(segment, ":") {
			return "", false, false
		}
	}
	head := ref[:len(ref)-len(rest)-len(query)] // the scheme and its colon
	absolute := scheme != ""
	if (scheme != "" || !strings.HasPrefix(rest, "///")) && strings.HasPrefix(rest, "//") {
		authority := rest[2:]
		rest = ""
		if i := strings.IndexByte(authority, '/'); i >= 0 {
			authority, rest = authority[:i], authority[i:]
		}
		head = ref[:len(head)+2+len(authority)]
		if w.done(len(head)) {
			return "", false, false
		}
		h, err := url.Parse(head)
		if err != nil {
			return "", false, false
		}
		absolute = h.Scheme != "" || h.Host != "" || h.User != nil // ResolveReference's choice
		lb += max(0, len(authority)/3-2)
	}
	if !linkEscapesValid(rest, w) || w.stopped {
		return "", false, false
	}
	var path string
	if absolute {
		path = linkResolvePath(rest, "", w)
	} else {
		path = linkResolvePath(base.EscapedPath(), rest, w)
	}
	if w.stopped {
		return "", false, false
	}
	if lb += len(path); lb > remaining {
		return "", true, true
	}
	if rest == "" { // no path: e is at most about three times remaining
		u, ok = resolveDirect(e, base)
		return u, ok, false
	}
	// "/." keeps the reference absolute however path starts, and resolves to
	// path again.
	short := head + "/." + path + query
	if strings.Contains(e, "#") {
		short += "#" + frag
	}
	u, ok = resolveDirect(short, base)
	return u, ok, false
}

// linkScheme is net/url's getScheme: the scheme before a colon, or none
// when the reference starts otherwise; ok is false for a leading colon.
func linkScheme(ref string) (scheme, rest string, ok bool) {
	for i := 0; i < len(ref); i++ {
		c := ref[i]
		switch {
		case 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z':
		case '0' <= c && c <= '9' || c == '+' || c == '-' || c == '.':
			if i == 0 {
				return "", ref, true
			}
		case c == ':':
			if i == 0 {
				return "", "", false
			}
			return ref[:i], ref[i+1:], true
		default:
			return "", ref, true
		}
	}
	return "", ref, true
}

// linkEscapesValid reports whether every '%' in s starts two hexadecimal
// digits, as net/url's unescape requires of a path or a fragment.
func linkEscapesValid(s string, w *jsoupWork) bool {
	hex := func(c byte) bool { return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F' }
	for i := 0; i < len(s); {
		k := strings.IndexByte(s[i:], '%')
		if k < 0 {
			return !w.done(len(s) - i)
		}
		if i += k; w.done(k+3) || i+2 >= len(s) || !hex(s[i+1]) || !hex(s[i+2]) {
			return false
		}
		i += 3
	}
	return true
}

// linkResolvePath is net/url's resolvePath (Go 1.27): the reference merged
// with the base's directory, dot segments removed. Its steps are counted.
func linkResolvePath(base, ref string, w *jsoupWork) string {
	var full string
	if ref == "" {
		full = base
	} else if ref[0] != '/' {
		i := strings.LastIndex(base, "/")
		full = base[:i+1] + ref
	} else {
		full = ref
	}
	if full == "" {
		return ""
	}
	dst := make([]byte, 0, len(full)+1)
	dst = append(dst, '/')
	elem := ""
	remaining := full
	found := true
	first := true
	for found {
		elem, remaining, found = strings.Cut(remaining, "/")
		if w.done(len(elem) + 1) {
			return ""
		}
		switch elem {
		case ".":
			first = false
			continue
		case "..":
			if i := bytes.LastIndexByte(dst[1:], '/'); i >= 0 {
				dst = dst[:i+1]
			} else {
				dst = dst[:1]
			}
			first = len(dst) == 1
		default:
			if !first {
				dst = append(dst, '/')
			}
			dst = append(dst, elem...)
			first = false
		}
	}
	if elem == "." || elem == ".." {
		dst = append(dst, '/')
	}
	if len(dst) > 1 && dst[1] == '/' {
		return string(dst[1:])
	}
	return string(dst)
}

const linkAllowed = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/?#[]@!$&'()*+,;=%"

var linkAllowedByte = func() (allowed [256]bool) {
	for i := range len(linkAllowed) {
		allowed[linkAllowed[i]] = true
	}
	return allowed
}()

// percentEncodeLink percent-encodes, with upper-case digits, every byte a URL
// cannot hold. Its work is counted; once w stops, its result is incomplete.
func percentEncodeLink(s string, w *jsoupWork) string {
	const digits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if i%jsoupCheckBytes == 0 && w.done(min(jsoupCheckBytes, len(s)-i)*3) {
			return b.String()
		}
		if c := s[i]; linkAllowedByte[c] {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(digits[c>>4])
			b.WriteByte(digits[c&15])
		}
	}
	return b.String()
}

// javaHashCode is String.hashCode over UTF-16 units.
func javaHashCode(s string) int32 {
	var h int32
	for _, u := range utf16.Encode([]rune(s)) {
		h = 31*h + int32(u)
	}
	return h
}
