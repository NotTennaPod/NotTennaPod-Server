package projection

import "testing"

// A node's several observations in one item scope (a
// compound) are folded in the compound's order, as separate contributions of
// that node would be; the compound used to be skipped, losing the record.
func TestFoldCatalogFoldsCompoundMembers(t *testing.T) {
	obs := func(title, member string, fetchTime int64) Effect {
		return Effect{Kind: KindObservation, Observed: map[string]any{"title": title}, FetchTime: fetchTime, Member: member}
	}
	earlier := Contribution{Node: "fetch", Rank: 1, Effect: obs("Fetched", "", 100)}
	compound := Contribution{Node: "device", Rank: 2, Effect: ComposeAt("e1:b", obs("Second", "", 200), obs("First", "e1:a", 200))}
	rec, ok := FoldCatalog([]Contribution{compound, earlier})
	if !ok || rec.Value["title"] != "First" || rec.Revision != "device" || rec.FetchTime != 200 {
		t.Errorf("got %+v %v, want the compound's first member (by its episode) at the device's revision", rec, ok)
	}
	// A compound alone is a record.
	if rec, ok := FoldCatalog([]Contribution{compound}); !ok || rec.Value["title"] != "First" {
		t.Errorf("a compound alone: got %+v %v", rec, ok)
	}
}
