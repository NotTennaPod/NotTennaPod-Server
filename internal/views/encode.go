package views

import "fmt"

// The JSON the read API returns for rows. Each row carries the derived
// values a screen shows and its records with their provenance: an action's
// fact names the provenance of what it acted on (PROTOCOL §12).

var playStates = map[int]string{-1: "new", 0: "unplayed", 1: "played"}

// EpisodeJSON is an episode row of version v: its podcast is v's. Its
// podcast's live generation lets the browser name the episode in a fact (an
// EpisodeRef).
func EpisodeJSON(v *Version, e *EpisodeItem) (map[string]any, error) {
	p, ok := v.podcasts.Get(e.PodcastID)
	if !ok {
		return nil, fmt.Errorf("views: episode %s of a podcast the version does not show", e.ID)
	}
	work.rowsEncoded.Add(1)
	var duration, size, history any
	if e.HasMedia {
		duration, size = e.Duration, e.Size
		if e.History > 0 {
			history = e.History
		}
	}
	recs, err := records(e.Records, "")
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"episode_id": e.ID, "podcast_id": e.PodcastID, "title": e.Title, "pub_date": e.PubDate, "link": e.Link,
		"image_url": e.ImageURL, "has_media": e.HasMedia, "duration_ms": duration, "size": size,
		"play_state": playStates[e.Read], "position": e.Position, "favorite": e.Favorite, "in_queue": e.Queued,
		"history": history, "last_played": e.LastPlayed, "placeholder": e.Placeholder,
		"podcast": map[string]any{"title": p.Title, "display_title": p.DisplayTitle, "image_url": p.ImageURL, "state": p.State,
			"generation_id": p.generation},
		"records": recs,
	}, nil
}

// PodcastJSON is a podcast row. Credentials show only as set; their username
// comes with the podcast view (PROTOCOL §12).
func PodcastJSON(p *PodcastItem) (map[string]any, error) {
	recs, err := records(p.Records, "credentials")
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"podcast_id": p.ID, "title": p.Title, "display_title": p.DisplayTitle, "image_url": p.ImageURL, "state": p.State,
		"generation_id": p.generation,
		"keep_updated":  p.KeepUpdated, "credentials": p.HasCredentials, "records": recs,
	}, nil
}
