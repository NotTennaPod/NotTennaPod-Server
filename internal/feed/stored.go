package feed

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/sealed"
)

// ParserVersion is the server's declared parser version (PROTOCOL §9). It
// increases whenever Parse's output changes for any input.
const ParserVersion = "ntp-go/1"

// StoredResponse is a fetch context's last successful response, kept so a
// parser upgrade can re-parse it without refetching (PROTOCOL §9.2). A re-parse
// uses the same Source and Now, so only the parser changes the result.
type StoredResponse struct {
	RepresentationKey  string
	ETag, LastModified string
	Source             string // the download URL it was parsed with
	Now                int64  // the evaluation time it was parsed with, ms
	ParserVersion      string
	Body               []byte
	// Revisions are the catalog revisions this response stands for ("feed"
	// or an episode ID), so a re-parse names them as its bases; nil for a response stored before
	// them.
	Revisions map[string]string
	// ETagUnreliable: its host answered an If-None-Match for an unchanged
	// body, with an unchanged Last-Modified, with the whole body again, so
	// its ETag is not sent (PROTOCOL §9.3). It holds for this source and
	// representation key only.
	ETagUnreliable bool
}

// maxStoredHeader bounds a stored response's header (validators, source and
// one revision per record): the fetcher's response headers are bounded by
// maxHeaderBytes, the revisions by the feed's items.
const maxStoredHeader = 16 << 20

// storedHeader is the response without its body, JSON before the body bytes.
type storedHeader struct {
	Key          string            `json:"key"`
	ETag         string            `json:"etag,omitempty"`
	LastModified string            `json:"last_modified,omitempty"`
	Source       string            `json:"source"`
	Now          int64             `json:"now"`
	Parser       string            `json:"parser"`
	Revisions    map[string]string `json:"revisions,omitempty"`
	Unreliable   bool              `json:"etag_unreliable,omitempty"`
}

// storedAD binds a sealed response to its account and podcast.
func storedAD(accountID, podcastID string) []byte {
	return []byte("feed-response\x00" + accountID + "\x00" + podcastID)
}

// Seal compresses and seals the response for an account's podcast: the body
// can hold credentials (a private feed's enclosure URLs), so it is never
// stored in the clear.
func (r StoredResponse) Seal(box *sealed.Box, accountID, podcastID string) ([]byte, error) {
	if int64(len(r.Body)) > maxBody {
		return nil, errors.New("feed: stored response over the body limit")
	}
	h, err := json.Marshal(storedHeader{Key: r.RepresentationKey, ETag: r.ETag, LastModified: r.LastModified,
		Source: r.Source, Now: r.Now, Parser: r.ParserVersion, Revisions: r.Revisions, Unreliable: r.ETagUnreliable})
	if err != nil {
		return nil, err
	}
	if len(h) > maxStoredHeader {
		return nil, errors.New("feed: stored response header over its limit")
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(h)
	gz.Write([]byte{'\n'})
	gz.Write(r.Body)
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return box.Seal(buf.Bytes(), storedAD(accountID, podcastID)), nil
}

// OpenStoredResponse opens a sealed response of an account's podcast.
func OpenStoredResponse(box *sealed.Box, accountID, podcastID string, sealedBytes []byte) (StoredResponse, error) {
	plain, err := box.Open(sealedBytes, storedAD(accountID, podcastID))
	if err != nil {
		return StoredResponse{}, err
	}
	gz, err := gzip.NewReader(bytes.NewReader(plain))
	if err != nil {
		return StoredResponse{}, err
	}
	limit := int64(maxBody + maxStoredHeader + 1)
	data, err := io.ReadAll(io.LimitReader(gz, limit+1))
	if err != nil {
		return StoredResponse{}, err
	}
	if int64(len(data)) > limit {
		return StoredResponse{}, errors.New("feed: stored response over its limit") // never a truncated body
	}
	head, body, ok := bytes.Cut(data, []byte{'\n'})
	if !ok {
		return StoredResponse{}, errors.New("feed: malformed stored response")
	}
	var h storedHeader
	if err := json.Unmarshal(head, &h); err != nil {
		return StoredResponse{}, err
	}
	return StoredResponse{RepresentationKey: h.Key, ETag: h.ETag, LastModified: h.LastModified, Source: h.Source,
		Now: h.Now, ParserVersion: h.Parser, Body: body, Revisions: h.Revisions, ETagUnreliable: h.Unreliable}, nil
}
