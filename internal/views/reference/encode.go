package reference

// The JSON the read API returns for rows. Each row carries the derived
// values a screen shows and its records with their provenance: an action's
// fact names the provenance of what it acted on (PROTOCOL §12).

var playStates = map[int]string{-1: "new", 0: "unplayed", 1: "played"}

// EpisodeJSON is an episode row. Its podcast's live generation lets the
// browser name the episode in a fact (an EpisodeRef).
func EpisodeJSON(e *Episode) map[string]any {
	var duration, size, history any
	if e.HasMedia {
		duration, size = e.Duration, e.Size
		if e.History > 0 {
			history = e.History
		}
	}
	image, _ := e.Records["metadata"].Field("image_url").(string)
	recs := map[string]Record{}
	for g, r := range e.Records {
		recs[g] = r
	}
	return map[string]any{
		"episode_id": e.ID, "podcast_id": e.Podcast.ID, "title": e.Title, "pub_date": e.PubDate, "link": e.Link,
		"image_url": image, "has_media": e.HasMedia, "duration_ms": duration, "size": size,
		"play_state": playStates[e.Read], "position": e.Position, "favorite": e.Favorite, "in_queue": e.Queued,
		"history": history, "last_played": e.LastPlayed, "placeholder": e.Placeholder,
		"podcast": map[string]any{"title": e.Podcast.Title, "display_title": e.Podcast.DisplayTitle, "image_url": e.Podcast.ImageURL, "state": e.Podcast.State,
			"generation_id": e.Podcast.Records["subscription"].Field("generation_id")},
		"records": recs,
	}
}

// PodcastJSON is a podcast row. Credentials show only as set; their username
// comes with the podcast view (PROTOCOL §12).
func PodcastJSON(p *Podcast) map[string]any {
	recs := map[string]Record{}
	for g, r := range p.Records {
		if g != "credentials" {
			recs[g] = r
		}
	}
	_, hasCredentials := p.Records["credentials"]
	return map[string]any{
		"podcast_id": p.ID, "title": p.Title, "display_title": p.DisplayTitle, "image_url": p.ImageURL, "state": p.State,
		"generation_id": p.Records["subscription"].Field("generation_id"),
		"keep_updated":  p.KeepUpdated, "credentials": hasCredentials, "records": recs,
	}
}
