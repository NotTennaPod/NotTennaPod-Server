package projection

import "testing"

func TestLostCompletionKeepsAutomaticQueue(t *testing.T) {
	for _, kind := range []string{KindSessionEnd, KindFinalize} {
		t.Run(kind, func(t *testing.T) {
			cs := []Contribution{
				{Node: "auto", Rank: 1, Effect: Effect{Kind: KindQueueAdd, Automatic: true, QueueKey: "key", PositionID: "id"}},
				{Node: "start-a", Rank: 2, Effect: Effect{Kind: KindSessionStart, Session: "a"}},
				{Node: "start-b", Rank: 3, Effect: Effect{Kind: KindSessionStart, Session: "b"}},
				{Node: "late-completion", Rank: 4, Effect: Effect{Kind: kind, Session: "a", Ordinal: 1, Completed: true, Claim: "start-a"}},
			}
			st := FoldEpisode(cs)
			if !st.InQueue || !st.HasMembership {
				t.Fatalf("a completion that lost its claim removed an automatic queue entry: in=%v hasMembership=%v auto=%+v", st.InQueue, st.HasMembership, st.Status["auto"])
			}
		})
	}
}

// Controls: a completion that fires, or a skip removal, is an explicit decision
// and bars an automatic add whatever the order.
func TestEffectiveRemovalBarsAutomaticQueue(t *testing.T) {
	for _, c := range []struct {
		name   string
		effect Effect
	}{
		{"fired completion", Effect{Kind: KindSessionEnd, Session: "a", Ordinal: 1, Completed: true}},
		{"skip removal", Effect{Kind: KindSessionEnd, Session: "a", Ordinal: 1, RemoveFromQueue: true}},
	} {
		cs := []Contribution{
			{Node: "start-a", Rank: 1, Effect: Effect{Kind: KindSessionStart, Session: "a"}},
			{Node: "end", Rank: 2, Effect: c.effect},
			{Node: "auto", Rank: 3, Effect: Effect{Kind: KindQueueAdd, Automatic: true, QueueKey: "key", PositionID: "id"}},
		}
		st := FoldEpisode(cs)
		if st.InQueue || st.Status["auto"].Outcome != "not_applied" {
			t.Fatalf("%s: in=%v auto=%+v", c.name, st.InQueue, st.Status["auto"])
		}
	}
}
