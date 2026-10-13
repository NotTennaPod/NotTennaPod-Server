package importer

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
)

// OPMLEntry is one subscription of an OPML file.
type OPMLEntry struct {
	Title string
	URL   string
}

// ErrOPML reports a file that is not well-formed OPML.
var ErrOPML = errors.New("importer: not a readable OPML file")

// ReadOPML reads an OPML file as Android AntennaPod 3.12.1's OpmlReader does
// (and NotTennaPod-iOS's OPMLReader): a UTF-8 BOM is dropped and the bytes are
// read as UTF-8 whatever the declaration says; once inside the opml element,
// every outline with a non-empty xmlUrl is a subscription, titled by its
// title, else its text, else its address. Attributes are found by local name,
// the last one winning, as KXmlParser does. Malformed XML and named entities
// other than XML's own are refused (no DTD is read or fetched). Reading
// stops with ctx's error once ctx is done.
func ReadOPML(ctx context.Context, data []byte) ([]OPMLEntry, error) {
	// Android's InputStreamReader is fixed to UTF-8: malformed bytes become
	// U+FFFD and a declared encoding changes nothing.
	text := feed.DecodeUTF8(bytes.TrimPrefix(data, []byte("\xef\xbb\xbf")))
	d := xml.NewDecoder(strings.NewReader(text))
	d.Strict = true
	d.CharsetReader = func(_ string, r io.Reader) (io.Reader, error) { return r, nil }
	var out []OPMLEntry
	inOPML := false
	for n := 0; ; n++ {
		if n%1024 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		tok, err := d.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrOPML, err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		if se.Name.Local == "opml" {
			inOPML = true
		}
		if !inOPML || se.Name.Local != "outline" {
			continue
		}
		address := attr(se, "xmlUrl")
		if address == "" {
			continue
		}
		title := attr(se, "title")
		if title == "" {
			title = attr(se, "text")
		}
		if title == "" {
			title = address
		}
		// Credentials in an address stay out of titles (they become the
		// feed's sealed secret).
		out = append(out, OPMLEntry{Title: ops.StripUserinfo(title), URL: address})
	}
	return out, nil // a well-formed file without an opml element has no subscriptions
}

// attr is the last attribute with this local name, namespace declarations
// aside.
func attr(se xml.StartElement, name string) string {
	for i := len(se.Attr) - 1; i >= 0; i-- {
		a := se.Attr[i]
		if a.Name.Space == "xmlns" || (a.Name.Space == "" && a.Name.Local == "xmlns") {
			continue
		}
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// ReasonInvalidURL skips an OPML entry whose address is not a web feed.
const ReasonInvalidURL = "invalid_url"

// FromOPML plans the import of OPML subscriptions (PROTOCOL §11.5): subscribed, keep
// updated, the global new-episodes action and no episodes, so each
// generation's first fetch is a first population and files nothing. Until
// that fetch replaces it, the podcast's record holds the entry's title, as
// Android's OpmlImportActivity names the feed it adds (new Feed(xmlUrl, null,
// text)), so a feed that cannot be fetched keeps that name. An address that
// is not an http(s) URL is skipped; the same normalized URL twice imports
// once. Credentials in an address become the feed's secret.
func FromOPML(entries []OPMLEntry, importID string, now int64) Plan {
	plan := Plan{Kind: "opml", Secrets: map[string]protocol.Secret{}}
	seen := map[string]bool{}
	for i, e := range entries {
		raw := strings.TrimSpace(e.URL)
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			plan.Skipped = append(plan.Skipped, Entry{Reason: ReasonInvalidURL, Title: e.Title})
			continue
		}
		feedURL := stripUserinfo(raw)
		n := contract.NormalizeURL(feedURL)
		if seen[n] {
			plan.Merged = append(plan.Merged, Entry{Kind: "podcast", Title: e.Title})
			continue
		}
		seen[n] = true
		source := int64(i + 1)
		title := e.Title
		p := ops.ImportPodcast{ImportID: importID, SourceFeeds: []int64{source}, URL: feedURL, Title: e.Title, State: "subscribed",
			KeepUpdated: true, NewEpisodes: "global", Time: now, Feed: ops.ImportFeedFields(&feed.Feed{Title: &title})}
		if u.User != nil {
			password, _ := u.User.Password()
			id := ops.ImportSecretID(importID, source)
			plan.Secrets[id] = protocol.Secret{ID: id, Username: u.User.Username(), Password: password, ApprovedOrigins: []string{origin(feedURL)}}
			p.SecretID = id
		}
		plan.Podcasts = append(plan.Podcasts, p)
	}
	return plan
}
