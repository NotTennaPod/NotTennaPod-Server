// Package importer turns an uploaded library file (an Android AntennaPod
// database backup, or OPML) into the podcasts a server-side import applies
// (PROTOCOL §11.5).
package importer

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/feed"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer/androiddb"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
)

// Plan is an import ready to apply: its podcasts in source order, the
// secrets they name, and what the file held that is not imported.
type Plan struct {
	Kind     string // android_db or opml
	Podcasts []ops.ImportPodcast
	Secrets  map[string]protocol.Secret // by secret ID
	Skipped  []Entry
	Merged   []Entry // feeds or items imported as one (PROTOCOL §11.5)
	Queue    int     // queue entries imported
}

// Entry names a skipped, merged or ambiguous source row for the result
// screen (the shapes of docs/CONTRACT.md "Import vectors", "result").
type Entry struct {
	Kind          string  `json:"kind,omitempty"` // merged: podcast or episode
	Reason        string  `json:"reason,omitempty"`
	Podcast       string  `json:"podcast,omitempty"` // the podcast's title (ambiguous and merged episodes)
	Title         string  `json:"title"`
	SourceFeedID  int64   `json:"source_feed_id,omitempty"`
	SourceItemID  int64   `json:"source_item_id,omitempty"`
	SourceFeedIDs []int64 `json:"source_feed_ids,omitempty"`
	SourceItemIDs []int64 `json:"source_item_ids,omitempty"`
}

// Skip reasons.
const (
	ReasonLocalFolder   = "local_folder"
	ReasonNoURL         = "no_feed_url"
	ReasonUnusedPreview = "unused_preview" // a preview nobody listened to (PROTOCOL §3.3: never registered)
)

// FromAndroid plans the import of a read Android backup. now is the import's
// server time. Rows Android would never show were already left out by the
// reader (Library.Skipped). Feeds whose URLs normalize equal are one podcast
// (PROTOCOL §11.5): the lowest ID creates it (its URL, type and metadata); its
// subscription properties are the lowest-ID non-preview feed's, its
// credentials that feed's, else the first feed's that has any; a group of
// previews without any user state is not imported.
func FromAndroid(lib *androiddb.Library, importID string, now int64) (Plan, error) {
	plan := Plan{Kind: "android_db", Secrets: map[string]protocol.Secret{}}
	queueIndex := map[int64]int{}
	for i, id := range lib.Queue {
		if _, seen := queueIndex[id]; !seen {
			queueIndex[id] = i
		}
	}
	keys, err := queueKeys(importID, len(lib.Queue))
	if err != nil {
		return plan, err
	}
	queued := map[int64]bool{}
	for _, id := range lib.Queue {
		queued[id] = true
	}
	feedHasUserState := func(f androiddb.Feed) bool {
		for _, e := range f.Episodes {
			if queued[e.ID] {
				return true
			}
		}
		return feedHasUserState(f)
	}
	type group struct{ feeds []androiddb.Feed }
	var groups []*group
	byURL := map[string]*group{}
	for _, f := range lib.Feeds {
		switch {
		case f.Local():
			plan.Skipped = append(plan.Skipped, Entry{Reason: ReasonLocalFolder, Title: feedTitle(f), SourceFeedID: f.ID})
			continue
		case strings.TrimSpace(f.URL) == "":
			plan.Skipped = append(plan.Skipped, Entry{Reason: ReasonNoURL, Title: feedTitle(f), SourceFeedID: f.ID})
			continue
		}
		// A preview nobody used is not imported, and never joins another
		// feed's podcast (PROTOCOL §3.3: previews are registered only when used).
		if f.State == 1 && !feedHasUserState(f) {
			plan.Skipped = append(plan.Skipped, Entry{Reason: ReasonUnusedPreview, Title: feedTitle(f), SourceFeedID: f.ID})
			continue
		}
		n := contract.NormalizeURL(stripUserinfo(f.URL))
		g := byURL[n]
		if g == nil {
			g = &group{}
			byURL[n] = g
			groups = append(groups, g)
		}
		g.feeds = append(g.feeds, f)
	}
	for _, g := range groups {
		creating := g.feeds[0]
		props := creating
		for _, f := range g.feeds {
			if f.State != 1 {
				props = f
				break
			}
		}
		feedType := ""
		if creating.Type != nil {
			feedType = *creating.Type
		}
		var rows []ops.ImportRow
		ids := make([]int64, len(g.feeds))
		for i, f := range g.feeds {
			ids[i] = f.ID
			for _, e := range f.Episodes {
				rows = append(rows, row(feedType, e, queueIndex, keys))
			}
		}
		feedURL := stripUserinfo(creating.URL)
		p := ops.ImportPodcast{ImportID: importID, SourceFeeds: ids, URL: feedURL, Title: feedTitle(creating), KeepUpdated: props.KeepUpdated,
			NewEpisodes: newEpisodesAction(props.NewEpisodesAction), State: subscriptionState(props.State), Rows: rows, Time: now,
			Feed: ops.ImportFeedFields(feedOf(creating, feedType))} // with or without items (PROTOCOL §11.5 "feed metadata")
		credsFrom := []androiddb.Feed{props}
		credsFrom = append(credsFrom, g.feeds...)
		for _, f := range credsFrom {
			user, pass, ok := credentials(f)
			if !ok {
				continue
			}
			id := ops.ImportSecretID(importID, f.ID)
			s := protocol.Secret{ID: id, Username: user, Password: pass}
			if o := origin(feedURL); o != "" {
				s.ApprovedOrigins = []string{o} // the feed URL's origin (PROTOCOL §9.2, §11.5)
			}
			plan.Secrets[id] = s
			p.SecretID = id
			break
		}
		if len(g.feeds) > 1 {
			plan.Merged = append(plan.Merged, Entry{Kind: "podcast", Title: feedTitle(creating), SourceFeedIDs: ids})
		}
		plan.Podcasts = append(plan.Podcasts, p)
	}
	plan.Queue = len(lib.Queue)
	return plan, nil
}

// credentials are a feed's username and password: its columns when either is
// non-empty, else the userinfo of its URL.
func credentials(f androiddb.Feed) (string, string, bool) {
	user, pass := deref(f.Username), deref(f.Password)
	if user != "" || pass != "" {
		return user, pass, true
	}
	if u, err := url.Parse(f.URL); err == nil && u.User != nil {
		p, _ := u.User.Password()
		return u.User.Username(), p, true
	}
	return "", "", false
}

// feedHasUserState reports a feed with any row a person did something with:
// played or NEW, a position, history, last-played or played time, a favorite
// or a queue entry.
func feedHasUserState(f androiddb.Feed) bool {
	for _, e := range f.Episodes {
		if e.Read != 0 || e.Favorite {
			return true
		}
		if m := e.Media; m != nil && (m.Position > 0 || m.CompletionDate > 0 || m.LastPlayedTime > 0 || m.PlayedDuration > 0) {
			return true
		}
	}
	return false
}

// row is one item with its media row, as a fetch item plus its imported state.
func row(feedType string, e androiddb.Episode, queueIndex map[int64]int, keys []queuePlace) ops.ImportRow {
	it := &feed.Item{GUID: e.GUID, Title: e.Title, Description: e.Description, Link: e.Link, ImageURL: e.ImageURL,
		PaymentLink: e.PaymentLink, ChapterURL: e.ChapterURL, TranscriptURL: e.TranscriptURL, TranscriptType: e.TranscriptType,
		SocialInteractURL: e.SocialURL}
	if e.PubDate != 0 {
		pub := e.PubDate
		it.PubDate = &pub
	}
	r := ops.ImportRow{SourceItem: e.ID, PlayState: "unplayed", Favorite: e.Favorite, QueueIndex: -1}
	switch e.Read {
	case 1:
		r.PlayState = "played"
	case -1:
		r.PlayState = "new"
	}
	if m := e.Media; m != nil {
		if m.URL != nil && *m.URL != "" {
			it.Media = &feed.Media{URL: m.URL, Size: m.Size, MIMEType: m.MimeType, Duration: m.Duration}
		}
		r.SourceMedia = m.ID
		r.Position = int64(m.Position)
		r.LastPlayed = m.LastPlayedTime
		r.PlayedTime = m.PlayedDuration
		if m.Duration > 0 {
			r.Duration = int64(m.Duration)
		}
		if m.CompletionDate > 0 {
			h := m.CompletionDate
			r.History = &h
		}
	}
	if i, ok := queueIndex[e.ID]; ok {
		r.QueueIndex, r.QueueKey, r.PositionID = i, keys[i].key, keys[i].positionID
	}
	r.Item = ops.ImportItem(feedType, it)
	return r
}

// feedOf is a Feeds row as a parsed feed (its podcast metadata record).
func feedOf(f androiddb.Feed, feedType string) *feed.Feed {
	out := &feed.Feed{Type: feedType, Identifier: f.FeedIdentifier, Title: f.Title, Description: f.Description, Author: f.Author,
		Language: f.Language, Link: f.Link, ImageURL: f.ImageURL, Paged: f.IsPaged, NextPageURL: f.NextPageLink}
	out.Funding = funding(f.PaymentLink)
	return out
}

// funding is FeedFunding.extractPaymentLinks (model/FeedFunding.java): entries
// separated by U+001E, each a URL and an optional title separated by U+001F;
// a value without separators is one URL.
func funding(v *string) []feed.Funding {
	if v == nil || strings.TrimSpace(*v) == "" {
		return []feed.Funding{}
	}
	if !strings.ContainsAny(*v, "\x1e\x1f") {
		u, empty := *v, ""
		return []feed.Funding{{URL: &u, Content: &empty}}
	}
	parts := strings.Split(*v, "\x1e")
	for len(parts) > 0 && parts[len(parts)-1] == "" { // Java's split drops trailing empties
		parts = parts[:len(parts)-1]
	}
	out := []feed.Funding{}
	for _, p := range parts {
		fields := strings.Split(p, "\x1f")
		u := fields[0]
		if strings.TrimSpace(u) == "" {
			continue
		}
		content := ""
		if len(fields) > 1 && strings.TrimSpace(fields[1]) != "" {
			content = fields[1]
		}
		out = append(out, feed.Funding{URL: &u, Content: &content})
	}
	return out
}

type queuePlace struct{ key, positionID string }

// queueKeys allocates n queue keys front to back, with stable position IDs
// (PROTOCOL §11.5: the Queue table's order).
func queueKeys(importID string, n int) ([]queuePlace, error) {
	out := make([]queuePlace, n)
	var prev *contract.QueueKey
	for i := 0; i < n; i++ {
		id := ops.ImportPositionID(importID, i)
		var u [16]byte
		copy(u[:], uuidBytes(id))
		k, err := contract.QueueKeyBetween(prev, nil, u)
		if err != nil {
			return nil, err
		}
		out[i] = queuePlace{key: k.String(), positionID: id}
		prev = &k
	}
	return out, nil
}

func uuidBytes(id string) []byte {
	b := make([]byte, 0, 16)
	for i := 0; i+1 < len(id); i++ {
		if id[i] == '-' {
			continue
		}
		v, err := strconv.ParseUint(id[i:i+2], 16, 8)
		if err != nil {
			return make([]byte, 16)
		}
		b = append(b, byte(v))
		i++
	}
	return b
}

func subscriptionState(s int32) string {
	switch s {
	case 1:
		return "not_subscribed"
	case 2:
		return "archived"
	}
	return "subscribed"
}

func newEpisodesAction(a int32) string {
	switch a {
	case 0:
		return "global"
	case 2:
		return "nothing"
	case 3:
		return "queue"
	}
	return "inbox"
}

// feedTitle is the custom title, else the title, else the URL; never with
// credentials (a URL's userinfo stays in the sealed secret).
func feedTitle(f androiddb.Feed) string {
	for _, t := range []*string{f.CustomTitle, f.Title} {
		if t != nil && strings.TrimSpace(*t) != "" {
			return ops.StripUserinfo(*t)
		}
	}
	return ops.StripUserinfo(f.URL)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// stripUserinfo is a feed URL as the import stores it: without userinfo by the
// server's one rule (ops.StripUserinfo), whatever its spelling, so an @ written
// as a character reference leaves none in the podcast's identity. Its
// credentials, read from the URL itself, go to the feed's secret.
func stripUserinfo(raw string) string {
	return ops.StripUserinfo(raw)
}

// origin is a URL's origin in the fetcher's normal form (feed.Origin:
// scheme://host:port, lower case, the port explicit), the form the fetcher
// compares approved origins in.
func origin(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	return feed.Origin(u)
}
