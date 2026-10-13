package feed

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// Fetch bounds (PROTOCOL §9.2).
const (
	firstByteTimeout = 30 * time.Second
	totalTimeout     = 120 * time.Second
	maxBody          = 64 << 20
	maxRedirects     = 20
	maxHeaderBytes   = 256 << 10 // a response's header section
	maxURLBytes      = 16 << 10  // a URL fetched, the final one included: twice the request line common servers accept
	userAgent        = "NotTennaPod-Server"
)

// FetchLimit is a fetch's total time (PROTOCOL §9.2). A refresh also parses
// within it: "ctx is checked … so the fetch deadline also stops parsing".
const FetchLimit = totalTimeout

// Credentials are a feed's HTTP credentials with the origins they may go to.
type Credentials struct {
	Username, Password string
	Revision           string   // the credential secret's ID: a new one for every edit
	ApprovedOrigins    []string // scheme://host:port, normalized (Origin)
}

// Validators are a stored successful response's, for one representation key.
type Validators struct {
	Key                string // RepresentationKey of the stored response
	ETag, LastModified string
}

// FetchRequest is one fetch in a fetch context (one account and podcast).
type FetchRequest struct {
	URL         string
	Credentials *Credentials
	Validators  *Validators
	Force       bool // a forced refresh, or the first after a failure: no validators
}

// Hop is one response in the redirect trace.
type Hop struct {
	URL    string
	Status int
}

// FetchResult is a completed fetch.
type FetchResult struct {
	NotModified       bool
	Body              []byte
	FinalURL          string
	Trace             []Hop
	PermanentMove     string // Android's rule: the first hop's target if it was 301/308 or an http→https upgrade
	ETag              string
	LastModified      string
	RepresentationKey string
}

// FetchError is a failed fetch, labelled as Android's download errors.
type FetchError struct {
	Label      string // unauthorized, forbidden, not_found, http_status, io, too_many_redirects, size_limit, time_limit, refused_destination
	Code       int    // the HTTP status, for http_status
	Detail     string // sanitized: no credentials, userinfo or query strings
	RetryAfter time.Duration
	Origin     string // unauthorized: the origin that answered 401 (after redirects)
}

func (e *FetchError) Error() string { return "feed: fetch: " + e.Label + " " + e.Detail }

// Fetcher fetches feeds. Its zero value is not usable; use NewFetcher.
type Fetcher struct {
	client    *http.Client
	now       func() time.Time
	allowHost func(host string) bool // tests only: loopback hosts their servers listen on
	bodyLimit int64
	total     time.Duration
	self      atomic.Pointer[[]netip.AddrPort] // the server's own listening addresses (RefuseListener)
}

// NewFetcher returns a fetcher that refuses loopback and unspecified
// destinations after DNS resolution, on every hop.
func NewFetcher() *Fetcher {
	f := &Fetcher{now: time.Now, bodyLimit: maxBody, total: totalTimeout}
	dialer := &net.Dialer{Timeout: firstByteTimeout}
	transport := &http.Transport{
		Proxy:                  nil,
		ResponseHeaderTimeout:  firstByteTimeout,
		MaxResponseHeaderBytes: maxHeaderBytes,
		TLSHandshakeTimeout:    firstByteTimeout,
		MaxIdleConnsPerHost:    2,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, err
			}
			for _, ip := range ips {
				if refused(ip.IP) && (f.allowHost == nil || !f.allowHost(host)) {
					return nil, errRefused
				}
			}
			if len(ips) == 0 {
				return nil, errors.New("no address")
			}
			if f.isSelf(ips, port) {
				return nil, errRefused
			}
			// Each checked address is tried in turn, pinned: an unreachable
			// first answer (IPv6 without a route) falls back to the next.
			var first error
			for _, ip := range ips {
				a := (&net.IPAddr{IP: ip.IP, Zone: ip.Zone}).String()
				conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(a, port))
				if err == nil {
					return conn, nil
				}
				if first == nil {
					first = err
				}
				if ctx.Err() != nil {
					break
				}
			}
			return nil, first
		},
	}
	f.client = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return f
}

var errRefused = errors.New("feed: refused destination")

// AllowLoopback lets the fetcher reach loopback addresses for host names the
// function accepts: test servers on this machine. Production fetchers never
// call it.
func (f *Fetcher) AllowLoopback(allow func(host string) bool) { f.allowHost = allow }

// refused reports loopback and unspecified addresses (PROTOCOL §9.2: LAN addresses
// are allowed; the household may host its own feeds).
func refused(ip net.IP) bool { return ip.IsLoopback() || ip.IsUnspecified() }

// RefuseListener makes the fetcher refuse the server's own listening
// address and port: its own API is never a feed or a media source. A wildcard address
// stands for every address of this machine. Other ports of this machine,
// and LAN hosts, stay allowed.
func (f *Fetcher) RefuseListener(addr net.Addr) {
	ap, err := netip.ParseAddrPort(addr.String())
	if err != nil {
		return
	}
	list := []netip.AddrPort{netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())}
	if old := f.self.Load(); old != nil {
		list = append(list, *old...)
	}
	f.self.Store(&list)
}

// isSelf reports a destination that is one of the server's own listeners.
func (f *Fetcher) isSelf(ips []net.IPAddr, port string) bool {
	self := f.self.Load()
	if self == nil {
		return false
	}
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return false
	}
	var local []netip.Addr // this machine's addresses, read once if a wildcard needs them
	for _, l := range *self {
		if l.Port() != uint16(p) {
			continue
		}
		if l.Addr().IsUnspecified() && local == nil {
			local = []netip.Addr{}
			if addrs, err := net.InterfaceAddrs(); err == nil {
				for _, a := range addrs {
					if pfx, err := netip.ParsePrefix(a.String()); err == nil {
						local = append(local, pfx.Addr().Unmap())
					}
				}
			}
		}
		for _, ip := range ips {
			a, ok := netip.AddrFromSlice(ip.IP)
			if !ok {
				continue
			}
			a = a.Unmap()
			if a == l.Addr() || l.Addr().IsUnspecified() && slices.Contains(local, a) {
				return true
			}
		}
	}
	return false
}

// Origin is scheme://host:port with the scheme and host lower-cased and the
// effective port explicit.
func Origin(u *url.URL) string {
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443"}[strings.ToLower(u.Scheme)]
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Hostname()) + ":" + port
}

// RepresentationKey binds validators to the URL fetched and the credentials used.
func RepresentationKey(u *url.URL, c *Credentials) string {
	rev := ""
	if c != nil {
		rev = c.Revision
	}
	return u.String() + "\x00" + rev
}

// Fetch fetches a feed (PROTOCOL §9.2).
func (f *Fetcher) Fetch(ctx context.Context, req FetchRequest) (*FetchResult, error) {
	ctx, cancel := context.WithTimeout(ctx, f.total)
	defer cancel()
	cur, err := requestURL(req.URL)
	if err != nil || len(cur.String()) > maxURLBytes {
		return nil, &FetchError{Label: "io", Detail: "invalid feed URL"}
	}
	res := &FetchResult{}
	first := cur
	for hops := 0; ; hops++ {
		if hops > maxRedirects {
			return nil, &FetchError{Label: "too_many_redirects"}
		}
		resp, err := f.withCredentials(ctx, cur, req)
		if err != nil {
			return nil, transportError(ctx, err)
		}
		res.Trace = append(res.Trace, Hop{URL: cur.String(), Status: resp.StatusCode})
		switch resp.StatusCode {
		case 300, 301, 302, 303, 307, 308:
			loc := resp.Header.Get("Location")
			resp.Body.Close()
			next, err := cur.Parse(loc)
			if err != nil || loc == "" || (next.Scheme != "http" && next.Scheme != "https") {
				return nil, &FetchError{Label: "http_status", Code: resp.StatusCode, Detail: "redirect without a usable Location"}
			}
			next.User, next.Fragment, next.RawFragment = nil, "", ""
			if len(next.String()) > maxURLBytes {
				return nil, &FetchError{Label: "http_status", Code: resp.StatusCode, Detail: "redirect without a usable Location"}
			}
			if hops == 0 && (resp.StatusCode == 301 || resp.StatusCode == 308 || upgraded(first, next)) {
				res.PermanentMove = next.String()
			}
			cur = next
			continue
		}
		defer resp.Body.Close()
		res.FinalURL = cur.String()
		res.RepresentationKey = RepresentationKey(cur, req.Credentials)
		switch {
		case resp.StatusCode == http.StatusNotModified:
			if req.Force || req.Validators == nil || req.Validators.Key != res.RepresentationKey {
				return nil, &FetchError{Label: "http_status", Code: 304, Detail: "not modified without a matching stored response"}
			}
			res.NotModified = true
			return res, nil
		case resp.StatusCode == 401:
			return nil, &FetchError{Label: "unauthorized", Origin: Origin(cur)}
		case resp.StatusCode == 403:
			return nil, &FetchError{Label: "forbidden"}
		case resp.StatusCode == 404 || resp.StatusCode == 410:
			return nil, &FetchError{Label: "not_found"}
		case resp.StatusCode < 200 || resp.StatusCode > 299:
			return nil, &FetchError{Label: "http_status", Code: resp.StatusCode, RetryAfter: retryAfter(resp, f.now())}
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, f.bodyLimit+1))
		if err != nil {
			return nil, transportError(ctx, err)
		}
		if int64(len(body)) > f.bodyLimit {
			return nil, &FetchError{Label: "size_limit"}
		}
		if !resp.Uncompressed && resp.ContentLength >= 0 && int64(len(body)) != resp.ContentLength {
			return nil, &FetchError{Label: "io", Detail: "the body is not as long as declared"}
		}
		res.Body = body
		res.ETag, res.LastModified = resp.Header.Get("ETag"), resp.Header.Get("Last-Modified")
		return res, nil
	}
}

// withCredentials sends one request, and on a 401 from an approved origin
// retries it with Basic credentials, ISO-8859-1 then UTF-8, as Android does.
func (f *Fetcher) withCredentials(ctx context.Context, u *url.URL, req FetchRequest) (*http.Response, error) {
	send := func(auth string) (*http.Response, error) {
		r, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		r.Header.Set("User-Agent", userAgent)
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}
		// Validators go to the hop whose representation key they belong to,
		// wherever it is in the redirect chain (PROTOCOL §9.2).
		if !req.Force && req.Validators != nil && req.Validators.Key == RepresentationKey(u, req.Credentials) {
			if req.Validators.ETag != "" {
				r.Header.Set("If-None-Match", req.Validators.ETag)
			}
			if req.Validators.LastModified != "" {
				r.Header.Set("If-Modified-Since", req.Validators.LastModified)
			}
		}
		return f.client.Do(r)
	}
	resp, err := send("")
	if err != nil || resp.StatusCode != 401 || req.Credentials == nil || !approved(req.Credentials, u) {
		return resp, err
	}
	for _, latin1 := range []bool{true, false} {
		resp.Body.Close()
		resp, err = send(basic(req.Credentials.Username, req.Credentials.Password, latin1))
		if err != nil || resp.StatusCode != 401 {
			return resp, err
		}
	}
	return resp, nil
}

func approved(c *Credentials, u *url.URL) bool {
	o := Origin(u)
	for _, a := range c.ApprovedOrigins {
		// Imports before the October 2026 fix stored origins without their
		// port; they compare in the normal form too.
		if a == o {
			return true
		}
		if p, err := url.Parse(a); err == nil && p.Host != "" && Origin(p) == o {
			return true
		}
	}
	return false
}

// basic is HttpCredentialEncoder.encode: ISO-8859-1 replaces characters it
// cannot represent with '?'.
func basic(user, pass string, latin1 bool) string {
	s := user + ":" + pass
	var b []byte
	if latin1 {
		for _, r := range s {
			if r > 0xFF {
				r = '?'
			}
			b = append(b, byte(r))
		}
	} else {
		b = []byte(s)
	}
	return "Basic " + base64.StdEncoding.EncodeToString(b)
}

// requestURL is the URL sent on the wire: http(s) only, no userinfo or fragment.
func requestURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("not an http(s) URL")
	}
	u.User, u.Fragment, u.RawFragment = nil, "", ""
	return u, nil
}

func upgraded(from, to *url.URL) bool {
	if from.Scheme != "http" || to.Scheme != "https" {
		return false
	}
	a, b := *from, *to
	a.Scheme = "https"
	return a.String() == b.String()
}

func retryAfter(resp *http.Response, now time.Time) time.Duration {
	if resp.StatusCode != 429 && resp.StatusCode != 503 {
		return 0
	}
	v := strings.TrimSpace(resp.Header.Get("Retry-After"))
	// Seconds are clamped before they become a duration: a huge value is
	// the longest wait, never an overflow.
	if v != "" && strings.Trim(v, "0123456789") == "" {
		s, err := strconv.ParseInt(v, 10, 64)
		if err != nil || s > int64(MaxRetryAfter/time.Second) {
			return MaxRetryAfter
		}
		return time.Duration(s) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return min(t.Sub(now), MaxRetryAfter)
	}
	return 0
}

// MaxRetryAfter is the longest Retry-After a fetch reports: the refresh
// schedule's 30-day cap (PROTOCOL §14).
const MaxRetryAfter = 30 * 24 * time.Hour

func transportError(ctx context.Context, err error) *FetchError {
	switch {
	case errors.Is(err, errRefused):
		return &FetchError{Label: "refused_destination"}
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return &FetchError{Label: "time_limit"}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return &FetchError{Label: "time_limit"}
	}
	return &FetchError{Label: "io", Detail: sanitize(err)}
}

// sanitize names a transport error's kind with a fixed text: never the error's
// own text, which can quote a server's response, a URL with credentials or a
// query string (PROTOCOL §9.2: no credentials, userinfo or query strings).
func sanitize(err error) string {
	var dns *net.DNSError
	var op *net.OpError
	var cert *tls.CertificateVerificationError
	var record tls.RecordHeaderError
	switch {
	case errors.As(err, &dns):
		return "host not found"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection reset"
	case errors.As(err, &cert), errors.As(err, &record):
		return "TLS handshake failed"
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		return "connection closed early"
	case errors.As(err, &op):
		return "network error (" + op.Op + ")"
	}
	return "malformed or failed response"
}
