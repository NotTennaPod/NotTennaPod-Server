package feed

// Feed dates: RSS <pubDate> and <dc:date>, Atom <published> and <updated>.
//
// They are read as AntennaPod 3.12.1's DateUtils.parse reads them on Android
// 16, with the Locale.US weekday and month names, ICU's English time-zone
// names and metazones, and the zone rules of Android's tzdata 2025a (all in
// the generated datezones.go):
//
//  1. The RFC 822 pattern "EEE, dd MMM yyyy HH:mm:ss Z" on the text as
//     given, leniently: out-of-range fields roll over, the weekday is not
//     checked and text after the zone is ignored.
//  2. Otherwise the text is cleaned up (trimmed, '/' to '-', runs of spaces to
//     one, the colon of a final "+hh:mm" dropped, a final CEST or CET to its
//     offset, the word "Sept" to "Sep", a fraction of a second to three
//     digits) and DateUtils' thirty fallback patterns are tried in order,
//     strictly: every field in range, the weekday right, the whole text read.
//  3. Otherwise, if the text starts with a word followed by ", ", the same
//     again on the text after that comma.
//
// Two-digit years fall in the 100 years starting 80 years before now. A
// standard zone name means the zone's standard time at that date; a daylight
// name adds the zone's current daylight saving, or follows the zone's rules
// when it has none ahead. Numbers are read as ICU reads integers: any
// script's decimal digits, Unicode minus signs, exponents, NaN and ∞.
//
// Deliberate differences from Android, part of the contract: the two-digit
// year window is computed in UTC on the proleptic Gregorian calendar;
// numbers too long for an exact integer are converted with strconv.ParseFloat;
// 64-bit overflow wraps; Unicode properties are Go's; nothing is printed for
// an unparseable date; and step 3 is done in linear time (Android's retry is
// quadratic in the number of leading words) with the same answers.
//
// A date can be as long as the feed's text, so the parse counts its steps and
// checks its context (the fetch deadline, PROTOCOL §9.2) every 65,536 steps
// and before each retry of step 3, panicking with parseCanceled when it is
// done. saxParse recovers it.

import (
	"context"
	"unicode/utf16"
)

//go:generate go run ./internal/gendatezones -out datezones.go

// dateZoneName is one of ICU's English zone names (datezones.go).
type dateZoneName struct {
	name string // the name as written
	dst  bool   // a daylight-time name
	tz   string // the zone the name belongs to, if only one
	mz   string // the metazone the name belongs to, if any
}

// dateZoneRule is a zone's rules from Android's tzdata (datezones.go).
type dateZoneRule struct {
	offsets []int32 // the UTC offset of each local-time type, in seconds
	isDst   string  // '1' for each daylight type, '0' for each standard one
	trans   string  // standard base64 of (zigzag varint seconds since the previous transition, type index) pairs
}

// parseDateOrNullIfFuture is DateUtils.parseOrNullIfFuture: the date in
// milliseconds since the epoch, or nil when it does not parse or is after
// now.
func parseDateOrNullIfFuture(ctx context.Context, s string, now int64) *int64 {
	ms, ok := parseDate(ctx, s, now)
	if !ok || ms > now {
		return nil
	}
	return &ms
}

// parseDate is DateUtils.parse: the date in milliseconds since the epoch, or
// false. now (milliseconds) decides two-digit years and daylight names.
func parseDate(ctx context.Context, s string, now int64) (int64, bool) {
	w := &dateWork{ctx: ctx}
	return parseDateUnits(w, w.encode(s), now)
}

// parseDateUnits parses a date given as UTF-16 code units, which may hold
// unpaired surrogates.
func parseDateUnits(w *dateWork, in []uint16, now int64) (int64, bool) {
	p := &dateParser{w: w, now: now}
	if ms, ok := p.rfc822(in); ok {
		return ms, true
	}
	text := p.normalize(in)
	// The weekday word must have no line terminator after it, anywhere: as
	// each retry reads a suffix, the last terminator answers for all.
	lastBreak := p.lastLineTerminator(text)
	start := 0
	for {
		suffix := text[start:]
		if ms, _, ok := p.fallback(suffix); ok {
			return ms, true
		}
		if lastBreak >= start {
			return 0, false
		}
		comma, ok := p.weekdayWord(suffix)
		if !ok {
			return 0, false
		}
		// DateUtils parses the text after the comma from the start. The RFC
		// 822 pattern cannot match it (it starts with a space), and cleaning
		// it up again only trims its start: the fallback patterns on the
		// trimmed suffix give the same answer.
		w.check()
		start += comma + 1
		for start < len(text) && text[start] <= ' ' {
			w.step()
			start++
		}
	}
}

// dateCheckEvery is how many steps a parse takes between context checks.
const dateCheckEvery = 1 << 16

// dateWork counts a parse's steps and checks its context.
type dateWork struct {
	ctx    context.Context
	steps  int // since the last check
	total  int // all steps, for tests
	checks int // all checks, for tests
}

// step counts one unit of input-dependent work.
func (w *dateWork) step() {
	w.total++
	if w.steps++; w.steps >= dateCheckEvery {
		w.check()
	}
}

// check panics with parseCanceled when the context is done.
func (w *dateWork) check() {
	w.steps = 0
	w.checks++
	if err := w.ctx.Err(); err != nil {
		panic(parseCanceled{err})
	}
}

// encode converts s to UTF-16, one step per code point. Invalid UTF-8 bytes
// become U+FFFD.
func (w *dateWork) encode(s string) []uint16 {
	out := make([]uint16, 0, len(s))
	for _, r := range s {
		w.step()
		out = utf16.AppendRune(out, r)
	}
	return out
}

// dateParser is the state of one parse.
type dateParser struct {
	w      *dateWork
	now    int64
	window *dateYearWindow // computed on first use
	digits []byte          // scratch for numbers
}
