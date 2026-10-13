package media

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
)

// Error is a media failure as the API reports it (PROTOCOL §13): whose fault
// it is (Code), the label of Android's download error the player shows
// (Label, the same labels as a feed's refresh status), and the HTTP status
// the API answers with.
type Error struct {
	Status     int
	Code       string // media_unauthorized, media_not_found, media_refused, media_unavailable, media_changed, server_busy, not_found
	Label      string
	HTTPCode   int // the publisher's status, for the http_status label
	Message    string
	RetryAfter time.Duration
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// ErrorDetail is an error as the status endpoint and a failed descriptor
// show it: the code, Android's label and wording, and the publisher's
// status for http_status.
type ErrorDetail struct {
	Error      string `json:"error"`
	Label      string `json:"label"`
	Message    string `json:"message"`
	Code       int    `json:"code,omitempty"`
	RetryAfter int64  `json:"retry_after,omitempty"`
}

// Detail is the error with its label.
func (e *Error) Detail() *ErrorDetail {
	d := &ErrorDetail{Error: e.Code, Label: e.Label, Message: e.Message, Code: e.HTTPCode}
	if e.RetryAfter > 0 {
		d.RetryAfter = e.RetrySeconds()
	}
	return d
}

// Body is the error as an HTTP error response (PROTOCOL §10: error, message,
// retry_after).
func (e *Error) Body() map[string]any {
	m := map[string]any{"error": e.Code, "message": e.Message}
	if e.RetryAfter > 0 {
		m["retry_after"] = e.RetrySeconds()
	}
	return m
}

// RetrySeconds is the Retry-After value in whole seconds, rounded up.
func (e *Error) RetrySeconds() int64 {
	return int64((e.RetryAfter + time.Second - 1) / time.Second)
}

// messages are Android's download-error strings by label (Android 3.12.1
// strings.xml, download_error_*), and the server's own for what Android has
// no string for.
var messages = map[string]string{
	"unauthorized":        "Authentication error. Make sure that username and password are correct.",
	"forbidden":           "The podcast host's server refuses to respond.",
	"not_found":           "The podcast host's server does not know where to find the file. It may have been deleted.",
	"http_status":         "HTTP data error",
	"io":                  "Connection error",
	"time_limit":          "Connection error",
	"too_many_redirects":  "HTTP data error",
	"refused_destination": "Cannot find the server. Check if the address is typed correctly and if you have a working network connection.",
	"wrong_size":          "The server connection was lost before completing the download",
	"insufficient_space":  "There is not enough space left on the server.",
	"media_changed":       "The episode's file changed on the podcast host's server.",
	"server_busy":         "The server is busy; try again shortly.",
	"playback": "The media file could not be played.\n\n- Try deleting and re-downloading the episode.\n- Check your network connection, and make sure no VPN or login page is blocking access.\n" +
		"- Try long-pressing and sharing the \"Media address\" to your web browser to see if it can be played there. If not, contact the podcast creators.",
	"unsupported_type": "The podcast host's server sent a file of another type.",
	"size_limit":       "The file is larger than the server copies for playback.",
}

func newError(status int, code, label string) *Error {
	return &Error{Status: status, Code: code, Label: label, Message: messages[label]}
}

var (
	errChanged    = newError(http.StatusPreconditionFailed, "media_changed", "media_changed")
	errTruncated  = newError(http.StatusBadGateway, "media_unavailable", "wrong_size")
	errStalled    = newError(http.StatusGatewayTimeout, "media_unavailable", "time_limit")
	errBadRange   = newError(http.StatusBadGateway, "media_unavailable", "http_status")
	errNoSpace    = newError(http.StatusInsufficientStorage, "media_unavailable", "insufficient_space")
	errUnplayable = newError(http.StatusBadGateway, "media_unavailable", "playback")
	errNoTools    = errors.New("media: ffmpeg is not available")
	errAccountMax = &Error{Status: http.StatusTooManyRequests, Code: "server_busy", Label: "server_busy",
		Message: messages["server_busy"], RetryAfter: 2 * time.Second}
	errServerBusy = &Error{Status: http.StatusServiceUnavailable, Code: "server_busy", Label: "server_busy",
		Message: messages["server_busy"], RetryAfter: 5 * time.Second}
)

// fetchFailure maps a fetch's failure to whose fault it is (PROTOCOL §13).
func fetchFailure(err error) *Error {
	var me *Error
	if errors.As(err, &me) {
		return me
	}
	var fe *feed.FetchError
	if !errors.As(err, &fe) {
		return newError(http.StatusBadGateway, "media_unavailable", "io")
	}
	switch fe.Label {
	case "unauthorized", "forbidden":
		return newError(http.StatusBadGateway, "media_unauthorized", fe.Label)
	case "not_found":
		return newError(http.StatusBadGateway, "media_not_found", "not_found")
	case "refused_destination":
		return newError(http.StatusBadGateway, "media_refused", "refused_destination")
	case "time_limit":
		return newError(http.StatusGatewayTimeout, "media_unavailable", "time_limit")
	case "too_many_redirects":
		return newError(http.StatusBadGateway, "media_unavailable", "too_many_redirects")
	case "http_status":
		e := newError(http.StatusBadGateway, "media_unavailable", "http_status")
		e.HTTPCode = fe.Code
		return e
	}
	return newError(http.StatusBadGateway, "media_unavailable", "io")
}

// statusFailure is an upstream answer that is neither media nor a redirect.
func statusFailure(code int) *Error {
	switch code {
	case 401:
		return fetchFailure(&feed.FetchError{Label: "unauthorized"})
	case 403:
		return fetchFailure(&feed.FetchError{Label: "forbidden"})
	case 404, 410:
		return fetchFailure(&feed.FetchError{Label: "not_found"})
	}
	return fetchFailure(&feed.FetchError{Label: "http_status", Code: code})
}

// retryHeader sets Retry-After for an error that has one.
func retryHeader(h http.Header, e *Error) {
	if e.RetryAfter > 0 {
		h.Set("Retry-After", strconv.FormatInt(e.RetrySeconds(), 10))
	}
}
