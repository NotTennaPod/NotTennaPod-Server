package clock

import (
	"testing"
	"time"
)

const days3 = 72 * time.Hour

// The offset moves the wall clock both ways, and only the wall clock: the
// interval between two readings, timers and elapsed time are the system's.
func TestTheOffsetMovesOnlyTheWallClock(t *testing.T) {
	for _, offset := range []time.Duration{0, days3, -days3} {
		w := Offset(offset)
		if w.Offset() != offset {
			t.Errorf("Offset() = %v, want %v", w.Offset(), offset)
		}
		before := time.Now()
		got := w.Now()
		after := time.Now()
		if lo, hi := before.Add(offset).UnixMilli(), after.Add(offset).UnixMilli(); got.UnixMilli() < lo || got.UnixMilli() > hi {
			t.Errorf("offset %v: Now() = %d, want within [%d, %d]", offset, got.UnixMilli(), lo, hi)
		}
		elapsed := w.Elapsed()
		start := time.Now()
		first := w.Now()
		<-w.After(20 * time.Millisecond)
		waited := time.Since(start)
		if waited < 20*time.Millisecond || waited > 10*time.Second {
			t.Errorf("offset %v: After(20ms) fired after %v", offset, waited)
		}
		if d := w.Now().Sub(first); d < 20*time.Millisecond || d > waited+time.Second {
			t.Errorf("offset %v: two readings %v apart over a wait of %v", offset, d, waited)
		}
		if d := w.Elapsed() - elapsed; d < 20*time.Millisecond || d > waited+time.Second {
			t.Errorf("offset %v: Elapsed moved %v over a wait of %v", offset, d, waited)
		}
	}
	if System() != Offset(0) {
		t.Error("System() is not the unmoved clock")
	}
}
