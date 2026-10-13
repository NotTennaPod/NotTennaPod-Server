package backupsched

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/clock"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The daily backup runs on the server's one wall clock (PROTOCOL §16, --dev-clock-offset), the
// store's included. With the clock three days ahead or behind, a first schedule is the moved
// clock's next quiet hour, and a backup is taken, named and recorded at the moved clock's
// time, with the next one the moved clock's next day. A restart with the clock corrected is a
// jump the schedule follows: a deadline the clock left far ahead comes back within a day and
// takes nothing; one it left behind is taken once.
func TestTheDailySlotRunsOnTheServersClock(t *testing.T) {
	for _, offset := range []time.Duration{72 * time.Hour, -72 * time.Hour} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			wall := clock.Offset(offset)
			cfg := Config{Clock: wall, Random: func() float64 { return 0.5 }, // no jitter
				FreeBytes: func(string) (int64, error) { return 100 << 30, nil }}
			store, err := storage.Open(ctx, dir, storage.WithClock(wall.Now))
			if err != nil {
				t.Fatal(err)
			}
			s := New(store, cfg)
			now := wall.Now()
			if err := s.load(); err != nil {
				t.Fatal(err)
			}
			next := time.UnixMilli(s.Status().NextDue)
			if !next.After(now) || next.Sub(now) > 24*time.Hour || next.Hour() != 3 || next.Minute() != 30 {
				t.Fatalf("a first schedule at %v, want the next 03:30 after the moved clock's %v", next, now)
			}
			// The backup comes due: taken at the moved clock's time.
			s.state.NextDue = now.UnixMilli()
			before := wall.Now().Truncate(time.Second)
			s.check(ctx)
			after := wall.Now()
			st := s.Status()
			taken := time.UnixMilli(st.LastSuccess)
			if st.Kept != 1 || taken.Before(before) || taken.After(after) {
				t.Fatalf("status %+v, want one backup taken within [%v, %v]", st, before, after)
			}
			if m := dailyName.FindStringSubmatch(st.LastFile); m == nil || m[2] != taken.UTC().Format("20060102-150405") {
				t.Errorf("backup named %s, taken at %v", st.LastFile, taken.UTC())
			}
			if next := time.UnixMilli(st.NextDue); !next.After(taken) || next.Sub(taken) > 24*time.Hour+jitter || next.Hour() != 3 {
				t.Errorf("after the backup the next is %v, want the moved clock's next day", next)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			// The server restarts with its clock corrected.
			store, err = storage.Open(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			cfg.Clock = clock.System()
			s = New(store, cfg)
			if err := s.load(); err != nil {
				t.Fatal(err)
			}
			s.check(ctx)
			s.check(ctx)
			st = s.Status()
			if offset > 0 {
				// Ahead: brought back within a day, nothing taken.
				if next := time.UnixMilli(st.NextDue); st.Kept != 1 || !next.After(time.Now()) || time.Until(next) > 24*time.Hour+jitter {
					t.Errorf("after the clock went back: %d kept, next %v", st.Kept, next)
				}
				return
			}
			// Behind: the missed days are one backup, now; the next is tomorrow's.
			if taken := time.UnixMilli(st.LastSuccess); st.Kept != 2 || time.Since(taken) > time.Minute || time.Since(taken) < 0 {
				t.Errorf("after the clock went forward: %d kept, the last taken at %v", st.Kept, taken)
			}
			if next := time.UnixMilli(st.NextDue); !next.After(time.Now()) || time.Until(next) > 24*time.Hour+jitter {
				t.Errorf("after the catch-up the next is %v", next)
			}
		})
	}
}
