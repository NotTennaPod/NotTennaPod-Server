// Package clock is the server's one wall clock (PROTOCOL §14, §16). The
// development flag --dev-clock-offset moves it by a fixed offset, to stand for
// a server whose clock is wrong; a restart with another offset is a jump of the
// clock.
//
// It is the time the server accepts by and schedules by:
//   - acceptance: every receive time, so time normalization (PROTOCOL §6.9),
//     and the times recovery, imports and server commits record;
//   - wall-clock scheduling: the refresh schedule's due times and backoff
//     deadlines, and the daily backup's slot and quiet hour;
//   - the times the server records and compares with those: the store's
//     timestamps, a backup's time, session expiry, and the read API's "now".
//
// It never moves monotonic time. A reading keeps its monotonic part, so the
// interval between two readings is exact; timers (After), elapsed time
// (Elapsed), retries and time-outs run on the system clock, as do request
// deadlines and the expiry of what the operating system timestamps (files).
package clock

import "time"

// Wall is the wall clock, moved by its offset.
type Wall struct{ offset time.Duration }

// System is the wall clock as the system keeps it.
func System() Wall { return Wall{} }

// Offset is the wall clock moved by d (positive: ahead).
func Offset(d time.Duration) Wall { return Wall{offset: d} }

// Now is the moved wall-clock time. Its monotonic reading moves with it, so
// readings of one Wall subtract exactly; never compare one with time.Now.
func (w Wall) Now() time.Time { return time.Now().Add(w.offset) }

// Offset is how far the clock is moved.
func (w Wall) Offset() time.Duration { return w.offset }

// After is a timer on the system clock: an offset never shortens or
// lengthens a wait.
func (w Wall) After(d time.Duration) <-chan time.Time { return time.After(d) }

var start = time.Now()

// Elapsed is the monotonic time since the process started, which no offset
// or clock change moves (the refresh schedule's sleep and jump detection).
func (w Wall) Elapsed() time.Duration { return time.Since(start) }
