package queue

import "testing"

// A long history replayed from the start (a cold start's or a fallback's
// full replay): the persistent state against the frozen reference.
func BenchmarkReplayFull(b *testing.B) {
	g := newGen(11, 26000)
	var nodes []Node
	ref := newRefState()
	ref.Fallback = g.fallback
	for range 70000 {
		n := g.node(ref.Visible())
		ref.Apply(n)
		nodes = append(nodes, n)
	}
	b.Run("persistent", func(b *testing.B) {
		for range b.N {
			s := NewState()
			s.Fallback = g.fallback
			for _, n := range nodes {
				s.Apply(n)
			}
		}
	})
	b.Run("reference", func(b *testing.B) {
		for range b.N {
			s := newRefState()
			s.Fallback = g.fallback
			for _, n := range nodes {
				s.Apply(n)
			}
		}
	})
}
