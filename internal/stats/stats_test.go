package stats

import "testing"

// Android's rules (PROTOCOL §8; the import reference's projection): played time
// filtered by last played, an absent last played counting as 0; marked-played
// counts the duration of PLAYED parts without played time; started needs a
// history time and played time, or (marked) PLAYED or a position; every part
// counts as an episode with its duration.
func TestPerPodcastAndMonthly(t *testing.T) {
	jan, feb := int64(1_767_225_600_000), int64(1_769_904_000_000) // 2026-01-01, 2026-02-01 UTC
	parts := []Part{
		{PlayedMS: 600_000, LastPlayed: jan + 1, History: true, DurationMS: 1_800_000},
		{PlayedMS: 0, LastPlayed: 0, DurationMS: 1_500_000, Played: true},
		{PlayedMS: 200_000, LastPlayed: feb + 1, DurationMS: 900_000, PositionMS: 30_000},
		{PlayedMS: 1_200_000, LastPlayed: 0, History: true, DurationMS: 2_000_000}, // pre-1.4: no last played
	}
	all := PerPodcast(parts, 0, Forever)
	want := Podcast{PlayedMS: 2_000_000, PlayedMarkedMS: 3_500_000, EpisodesStarted: 2, EpisodesStartedMarked: 4, Episodes: 4, TotalMS: 6_200_000}
	if all != want {
		t.Fatalf("all time %+v, want %+v", all, want)
	}
	if got := PerPodcast(parts, jan, feb).PlayedMS; got != 600_000 {
		t.Errorf("January %d", got)
	}
	m := Monthly(parts)
	if len(m) != 2 || m[Month{2026, 1}] != 600_000 || m[Month{2026, 2}] != 200_000 {
		t.Errorf("monthly %v", m)
	}
}

// Parts reads an episode's records and its units.
func TestParts(t *testing.T) {
	recs := map[string]map[string]any{
		"play_state":       {"play_state": "played"},
		"played_time":      {"played_ms": float64(300_000)},
		"last_played":      {"time": float64(1_000)},
		"history":          {"time": float64(1_000)},
		"statistics_units": {"units": []any{map[string]any{"played_ms": float64(5), "last_played": float64(7), "history": nil, "duration_ms": float64(9), "played": true, "position": float64(0)}}},
	}
	meta := map[string]any{"media": map[string]any{"duration": float64(42)}}
	parts := Parts(recs, meta)
	if len(parts) != 2 || parts[0] != (Part{PlayedMS: 300_000, LastPlayed: 1_000, History: true, DurationMS: 42, Played: true}) ||
		parts[1] != (Part{PlayedMS: 5, LastPlayed: 7, DurationMS: 9, Played: true}) {
		t.Fatalf("parts %+v", parts)
	}
}
