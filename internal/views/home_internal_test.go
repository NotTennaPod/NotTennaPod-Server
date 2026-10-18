package views

import "testing"

// A tie of Surprise scores goes to the lower rank, whatever the IDs
// (a real tie needs two IDs with one 64-bit hash: the rule is checked on
// the scores).
func TestSurpriseTieGoesToTheLowerRank(t *testing.T) {
	low, high := &EpisodeItem{ID: "e1:b", Rank: 1}, &EpisodeItem{ID: "e1:a", Rank: 2} // rank order opposite to ID order
	if !betterScore(7, 7, low, high) || betterScore(7, 7, high, low) {
		t.Fatal("a tie went to the higher rank")
	}
	same := &EpisodeItem{ID: "e1:c", Rank: 1}
	if !betterScore(7, 7, low, same) || betterScore(7, 7, same, low) {
		t.Fatal("a tie of ranks went to the higher ID")
	}
	if !betterScore(8, 7, high, low) {
		t.Fatal("a higher score lost")
	}
}
