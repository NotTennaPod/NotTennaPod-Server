package feed

import (
	"context"
	"net/http"
	"net/url"
)

// MediaRequest is one request for an episode's media or one of its metadata
// assets (PROTOCOL §13): a GET or HEAD that follows the fetch rules of
// PROTOCOL §9.2 on every hop.
type MediaRequest struct {
	Method      string // GET (the default) or HEAD
	URL         string
	Range       string // the Range header to send, if any
	IfRange     string // the If-Range header to send (a strong ETag), if any
	Credentials *Credentials
}

// OpenMedia sends req and returns the first response that is not a
// redirect, with its body open for the caller to close, and the URL that
// answered. It follows at most 20 redirects; loopback and unspecified
// destinations are refused after DNS resolution on every hop; credentials go
// only to their approved origins, after a 401 as Android sends them; the
// server's User-Agent is sent and nothing of the browser's request but the
// Range and If-Range the caller names. It asks for the identity encoding.
// A transport failure is a *FetchError; any other status is the caller's.
// There is no total time limit (media streams are long); the first byte is
// limited as for feeds.
func (f *Fetcher) OpenMedia(ctx context.Context, req MediaRequest) (*http.Response, *url.URL, error) {
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	cur, err := requestURL(req.URL)
	if err != nil {
		return nil, nil, &FetchError{Label: "io", Detail: "invalid media URL"}
	}
	for hops := 0; ; hops++ {
		if hops > maxRedirects {
			return nil, nil, &FetchError{Label: "too_many_redirects"}
		}
		resp, err := f.sendMedia(ctx, method, cur, req)
		if err != nil {
			return nil, nil, transportError(ctx, err)
		}
		switch resp.StatusCode {
		case 300, 301, 302, 303, 307, 308:
			loc := resp.Header.Get("Location")
			discard(resp)
			next, err := cur.Parse(loc)
			if err != nil || loc == "" || (next.Scheme != "http" && next.Scheme != "https") || next.Host == "" {
				return nil, nil, &FetchError{Label: "http_status", Code: resp.StatusCode, Detail: "redirect without a usable Location"}
			}
			next.User, next.Fragment, next.RawFragment = nil, "", ""
			cur = next
			continue
		}
		return resp, cur, nil
	}
}

// sendMedia sends one hop's request, and on a 401 from an approved origin
// retries it with Basic credentials, ISO-8859-1 then UTF-8, as Android does.
func (f *Fetcher) sendMedia(ctx context.Context, method string, u *url.URL, req MediaRequest) (*http.Response, error) {
	send := func(auth string) (*http.Response, error) {
		r, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
		if err != nil {
			return nil, err
		}
		r.Header.Set("User-Agent", userAgent)
		r.Header.Set("Accept-Encoding", "identity")
		if req.Range != "" {
			r.Header.Set("Range", req.Range)
		}
		if req.IfRange != "" {
			r.Header.Set("If-Range", req.IfRange)
		}
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}
		return f.client.Do(r)
	}
	resp, err := send("")
	if err != nil || resp.StatusCode != 401 || req.Credentials == nil || !approved(req.Credentials, u) {
		return resp, err
	}
	for _, latin1 := range []bool{true, false} {
		discard(resp)
		resp, err = send(basic(req.Credentials.Username, req.Credentials.Password, latin1))
		if err != nil || resp.StatusCode != 401 {
			return resp, err
		}
	}
	return resp, nil
}

// discard closes a redirect's or a 401's response at once, unread: a body
// that never comes must not hold the request (its connection is not kept).
func discard(resp *http.Response) {
	resp.Body.Close()
}
