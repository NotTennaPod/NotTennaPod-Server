// Package stats is PROTOCOL §8 statistics projection from episode records, as
// AntennaPod 3.12.1 shows statistics: per podcast (played time with a date
// filter, "include marked as played", episodes started, episodes, total
// duration) and per month. Clients and the read API compute it from the
// records the server publishes; an imported row merged into an episode counts
// as its own unit (PROTOCOL §11.5).
package stats

import (
	"encoding/json"
	"time"
)

// Part is one statistical unit: an episode, or an imported row merged into
// one (its statistics_units entry).
type Part struct {
	PlayedMS   int64 // the residual played time
	LastPlayed int64 // ms; 0 when absent (Android's filter counts it as 0)
	History    bool  // a history time is present
	DurationMS int64
	Played     bool // PLAYED: "include marked as played" counts its duration
	PositionMS int64
}

// Podcast is a podcast's statistics row.
type Podcast struct {
	PlayedMS              int64
	PlayedMarkedMS        int64
	EpisodesStarted       int64
	EpisodesStartedMarked int64
	Episodes              int64
	TotalMS               int64
}

// Forever is the open end of Android's date filter (Long.MAX_VALUE).
const Forever = int64(1<<63 - 1)

// PerPodcast is a podcast's row with Android's date filter [from, to) on last
// played; the marked-played values and the counts are not filtered (Android
// forces all-time for "include marked as played").
func PerPodcast(parts []Part, from, to int64) Podcast {
	var p Podcast
	for _, x := range parts {
		p.Episodes++
		p.TotalMS += x.DurationMS
		if from <= x.LastPlayed && x.LastPlayed < to {
			p.PlayedMS += x.PlayedMS
		}
		if 0 <= x.LastPlayed {
			if x.PlayedMS != 0 {
				p.PlayedMarkedMS += x.PlayedMS
			} else if x.Played {
				p.PlayedMarkedMS += x.DurationMS
			}
		}
		started := x.History && x.PlayedMS > 0
		if started {
			p.EpisodesStarted++
		}
		if started || x.Played || x.PositionMS > 0 {
			p.EpisodesStartedMarked++
		}
	}
	return p
}

// Month is a UTC calendar month.
type Month struct{ Year, Month int }

// Monthly groups every positive played total by the UTC month of its
// positive last played (PROTOCOL §8: whole totals, never split).
func Monthly(parts []Part) map[Month]int64 {
	out := map[Month]int64{}
	for _, x := range parts {
		if x.LastPlayed > 0 && x.PlayedMS > 0 {
			t := time.UnixMilli(x.LastPlayed).UTC()
			out[Month{t.Year(), int(t.Month())}] += x.PlayedMS
		}
	}
	return out
}

// Parts reads an episode's statistical parts from its records, by field
// group name (play_state, position, played_time, last_played, history,
// measured_duration, statistics_units) and its catalog metadata (the feed
// duration when no measured duration exists). Absent records count as zero.
func Parts(records map[string]map[string]any, metadata map[string]any) []Part {
	num := func(v any) int64 {
		switch n := v.(type) {
		case float64:
			return int64(n)
		case int64:
			return n
		case json.Number:
			i, _ := n.Int64()
			return i
		}
		return 0
	}
	ep := Part{
		PlayedMS:   num(records["played_time"]["played_ms"]),
		LastPlayed: num(records["last_played"]["time"]),
		History:    records["history"] != nil && records["history"]["time"] != nil,
		Played:     records["play_state"]["play_state"] == "played",
		PositionMS: num(records["position"]["position"]),
	}
	if d, ok := records["measured_duration"]; ok {
		ep.DurationMS = num(d["duration_ms"])
	} else if m, ok := metadata["media"].(map[string]any); ok {
		ep.DurationMS = num(m["duration"])
	}
	parts := []Part{ep}
	if list, ok := records["statistics_units"]["units"].([]any); ok {
		for _, x := range list {
			u, _ := x.(map[string]any)
			parts = append(parts, Part{PlayedMS: num(u["played_ms"]), LastPlayed: num(u["last_played"]), History: u["history"] != nil,
				DurationMS: num(u["duration_ms"]), Played: u["played"] == true, PositionMS: num(u["position"])})
		}
	}
	return parts
}

// PlayedMarkedBetween is "include marked as played" with Android's date
// filter applied, as Home's classics section asks (SubscriptionsSection passes
// the last three years with the statistics screen's setting): the played time,
// or the duration of a PLAYED part without played time, of parts last played
// in [from, to).
func PlayedMarkedBetween(parts []Part, from, to int64) int64 {
	var n int64
	for _, x := range parts {
		if from <= x.LastPlayed && x.LastPlayed < to {
			if x.PlayedMS != 0 {
				n += x.PlayedMS
			} else if x.Played {
				n += x.DurationMS
			}
		}
	}
	return n
}
