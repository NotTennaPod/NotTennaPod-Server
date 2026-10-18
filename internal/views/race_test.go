//go:build race

package views_test

// Under the race detector the randomized sequences run a tenth of their
// steps, and the single-goroutine tests at scale are skipped
// (skipUnderRace); all of them run in full without it.
func init() { raceFactor = 10 }
