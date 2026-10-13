package projection

import (
	"fmt"
	"testing"
	"time"
)

// An automatic admission files the episode as NEW only when the scope has no
// barrier at all, whatever the order; favorites are not barriers.
func TestAdmissionBarrier(t *testing.T) {
	on := true
	admission := Contribution{Node: "admit", Rank: 2, Effect: Effect{Kind: KindAdmissionNew}}
	for _, tc := range []struct {
		name    string
		other   Effect
		rank    int64
		want    string
		outcome string
	}{
		{"none", Effect{}, 0, "new", "applied"},
		{"favorite before", Effect{Kind: KindFavorite, On: &on}, 1, "new", "applied"},
		{"favorite after", Effect{Kind: KindFavorite, On: &on}, 3, "new", "applied"},
		{"dismiss after", Effect{Kind: KindDismiss}, 3, "unplayed", "not_applied"},
		{"dismiss before", Effect{Kind: KindDismiss}, 1, "unplayed", "not_applied"},
		{"mark unplayed before", Effect{Kind: KindMarkUnplayed}, 1, "unplayed", "not_applied"},
		{"mark played after", Effect{Kind: KindMarkPlayed}, 3, "played", "not_applied"},
		{"reset position before", Effect{Kind: KindResetPosition}, 1, "unplayed", "not_applied"},
	} {
		cs := []Contribution{admission}
		if tc.other.Kind != "" {
			cs = append(cs, Contribution{Node: "other", Rank: tc.rank, Effect: tc.other})
		}
		st := FoldEpisode(cs)
		if st.PlayState != tc.want || st.Status["admit"].Outcome != tc.outcome {
			t.Errorf("%s: play state %s, admission %+v; want %s, %s", tc.name, st.PlayState, st.Status["admit"], tc.want, tc.outcome)
		}
		if tc.other.Kind == KindDismiss && st.Status["other"].Outcome != "applied" {
			t.Errorf("%s: dismissal %+v, want applied", tc.name, st.Status["other"])
		}
	}
}

// One long session folds in linear time (the per-save ordinal check was once quadratic).
func TestLongSessionFoldIsLinear(t *testing.T) {
	const n = 20_000
	cs := make([]Contribution, n+1)
	zero := int64(0)
	cs[0] = Contribution{Node: "start", Rank: 1, Effect: Effect{Kind: KindSessionStart, Session: "s", Position: &zero}}
	for i := 1; i <= n; i++ {
		pos := int64(i) * 60_000
		cs[i] = Contribution{Node: fmt.Sprint(i), Rank: int64(i + 1), Effect: Effect{Kind: KindCheckpoint, Session: "s", Ordinal: int64(i), Position: &pos, Contribution: pos, Activity: true, Time: int64(i)}}
	}
	begin := time.Now()
	st := FoldEpisode(cs)
	if elapsed := time.Since(begin); elapsed > 2*time.Second {
		t.Fatalf("folding %d saves took %s", n, elapsed)
	}
	if st.Position != n*60_000 || st.PlayedTime != n*60_000 {
		t.Fatalf("position %d, played %d", st.Position, st.PlayedTime)
	}
}
