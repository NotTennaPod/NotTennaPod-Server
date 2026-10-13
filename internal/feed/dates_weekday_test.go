package feed

// The weekday retry: DateUtils parses the text after a leading "word, " again
// from the start, which takes quadratic time in the number of such words.
// The parser does it in linear time; these tests check that its answers are
// those of the retry taken literally, that it is linear, and that it checks
// the parse's context before each retry.

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf16"
)

// dateRetryStats records what the literal retry did over many dates.
type dateRetryStats struct {
	firstRound   [len(dateNormalizeSteps)]int // dates each clean-up step changed
	retries      int                          // retries taken
	retryTrimmed int                          // retries whose input clean-up trimmed
	retryChanged [len(dateNormalizeSteps)]int // retries a step changed beyond trimming the start
	rfcOnRetry   int                          // retries the RFC 822 pattern answered
	answered     [len(dateFallbacks)]int      // dates each fallback pattern answered
}

// dateUtilsParseRenormalizing is DateUtils.parse taken literally: on a
// leading weekday-like word it starts again, RFC 822 attempt and clean-up
// included, on the cleaned-up text after the word's comma.
func dateUtilsParseRenormalizing(in []uint16, now int64, stats *dateRetryStats) (int64, bool) {
	if stats == nil {
		stats = &dateRetryStats{}
	}
	p := &dateParser{w: &dateWork{ctx: context.Background()}, now: now}
	for retry := 0; ; retry++ {
		if ms, ok := p.rfc822(in); ok {
			if retry > 0 {
				stats.rfcOnRetry++
			}
			return ms, true
		}
		text := in
		for i, step := range dateNormalizeSteps {
			next := step.apply(p, text)
			if changed := !slices.Equal(next, text); changed && retry == 0 {
				stats.firstRound[i]++
			} else if changed && i == 0 && slices.Equal(next, text[len(text)-len(next):]) {
				stats.retryTrimmed++
			} else if changed {
				stats.retryChanged[i]++
			}
			text = next
		}
		if ms, i, ok := p.fallback(text); ok {
			stats.answered[i]++
			return ms, true
		}
		// ^\w+, .*$, the whole text: no line terminator anywhere.
		comma, ok := p.weekdayWord(text)
		if !ok || slices.ContainsFunc(text, dateIsLineTerminator) {
			return 0, false
		}
		stats.retries++
		in = text[comma+1:]
	}
}

// dateParseCounted parses s with a fresh step counter: the answer, the steps
// after the UTF-16 conversion and the context checks.
func dateParseCounted(s string, now int64) (ms int64, ok bool, steps, checks int, units int) {
	w := &dateWork{ctx: context.Background()}
	in := w.encode(s)
	conversion := w.total
	ms, ok = parseDateUnits(w, in, now)
	return ms, ok, w.total - conversion, w.checks, len(in)
}

// dateRetryShapes are dates of many repeated "word, " prefixes.
var dateRetryShapes = []struct{ prefix, tail string }{
	{"a, ", "Sat, 09 Oct 2021 10:00:00 +0000"},
	{"Sat, ", "Sat, 09 Oct 2021 10:00:00 +0000"},
	{"Saturday, ", "Saturday, 09 Oct 21 10:00:00"},
	{"Mon, ", "nothing"},
	{"1, ", "09 Oct 21 10:00:00 +0000"},
	{"2021, ", "2021-10-09T10:00:00.123456+01:00"},
	{"été, ", "Sat 9 Oct 2021 10:00:00 GMT+0100 (Pacific Standard Time)"},
	{"x_y, ", "Sat Oct 9 10:00:00 2021"},
	{"\u0301, ", "Sat, 09 Oct 2021 10:00 EDT"},
	{"日本, ", "2021-10-09"},
	{"𝐀, ", "2021-10-09 PST"},
	{"Sept, ", "09 Sept 2021 10:00:00 CEST"},
	{"a,  ", "1985/11/05T13:12:25/0000"},
	{"a, \t", "Sat, 09 Oct 2021 10:00:00 +0000"},
	{"1E1, ", "Sat, 1E1 Oct 2021 10:00:00 x"},
}

// Each retry costs the length of the word it drops, not of the whole date.
func TestDateWeekdayRetryIsLinear(t *testing.T) {
	if ms, ok := parseDate(context.Background(), strings.Repeat("a, ", 3000)+"Sat, 09 Oct 2021 10:00:00 +0000", corpusNow); !ok || ms != 1633773600000 {
		t.Fatalf("3,000 prefixes: %d, %v", ms, ok)
	}
	build := func(prefix, tail string, size int) string {
		n := (size + len(utf16.Encode([]rune(prefix))) - 1) / len(utf16.Encode([]rune(prefix)))
		return strings.Repeat(prefix, n) + tail
	}
	for _, sh := range dateRetryShapes {
		want, wantOK := parseDate(context.Background(), sh.tail, corpusNow)
		date := build(sh.prefix, sh.tail, 8<<10)
		ms, ok, steps, _, units := dateParseCounted(date, corpusNow)
		if ok != wantOK || ms != want {
			t.Errorf("%q × 8 KiB: %d, %v; the date alone gives %d, %v", sh.prefix, ms, ok, want, wantOK)
		}
		if steps > 64*units {
			t.Errorf("%q × 8 KiB: %d steps for %d units (%.1f per unit)", sh.prefix, steps, units, float64(steps)/float64(units))
		}
		t.Logf("%-12q %d units, %.1f steps per unit", sh.prefix, units, float64(steps)/float64(units))

		small := utf16.Encode([]rune(build(sh.prefix, sh.tail, 1<<10)))
		lms, lok := parseDateUnits(&dateWork{ctx: context.Background()}, small, corpusNow)
		rms, rok := dateUtilsParseRenormalizing(small, corpusNow, nil)
		if lok != rok || lms != rms {
			t.Errorf("%q × 1 KiB: %d, %v; the literal retry gives %d, %v", sh.prefix, lms, lok, rms, rok)
		}
	}
	// The time grows with the length, not its square.
	elapsed := func(n int) time.Duration {
		date := strings.Repeat("a, ", n) + "nothing"
		start := time.Now()
		for range 5 {
			parseDate(context.Background(), date, corpusNow)
		}
		return time.Since(start)
	}
	elapsed(1000)
	if small, large := elapsed(4000), elapsed(64000); large > 64*small+50*time.Millisecond {
		t.Errorf("4,000 prefixes took %s, 64,000 took %s", small, large)
	}
}

// Each retry checks the context once; the steps before and between retries
// are too few for a check of their own.
func TestDateWeekdayRetryChecksEachRetry(t *testing.T) {
	date := strings.Repeat("a, ", 1000) + "Sat, 09 Oct 2021 10:00:00 +0000"
	ms, ok, _, checks, _ := dateParseCounted(date, corpusNow)
	if !ok || ms != 1633773600000 || checks != 1000 {
		t.Fatalf("1,000 prefixes: %d, %v after %d checks, want 1633773600000 after 1000", ms, ok, checks)
	}
	if _, _, _, checks, _ := dateParseCounted("Sat, 09 Oct 2021 10:00:00", corpusNow); checks != 0 {
		t.Errorf("a date without retries checked %d times", checks)
	}
	ctx := &budgetCancelAfter{Context: context.Background(), checks: 500}
	func() {
		defer func() {
			c, ok := recover().(parseCanceled)
			if !ok || !errors.Is(c.err, context.Canceled) {
				t.Errorf("cancelled at the 501st retry: recovered %#v", c)
			}
		}()
		parseDate(ctx, date, corpusNow)
		t.Error("cancelled at the 501st retry: no panic")
	}()
	if ctx.checks != -1 {
		t.Errorf("the parse stopped after %d checks, want 501", 500-ctx.checks)
	}
}

// The linear retry answers as the literal one: on the vectors and on random
// chains of words and separators before many kinds of dates. Cleaning up a
// retry's input again only ever trims its start.
func TestDateWeekdayRetryAnswersAsRenormalizing(t *testing.T) {
	n := 40_000
	if raceEnabled {
		n = 4_000
	}
	if s := os.Getenv("NTP_DATE_RETRY_CASES"); s != "" {
		var err error
		if n, err = strconv.Atoi(s); err != nil {
			t.Fatal(err)
		}
	}
	vectors, _ := dateLoadCases(t, "date_parse")
	var inputs []string
	for _, c := range vectors {
		inputs = append(inputs, c.Input)
	}
	r := rand.New(rand.NewPCG(20261011, 2))
	tails := append(slices.Clone(inputs), dateRetryTails...)
	for range n {
		inputs = append(inputs, dateRandomChain(r, tails))
	}

	var stats dateRetryStats
	nows := []int64{corpusNow, corpusNow, corpusNow, 0, 946684800000, 4102444800000}
	differ := 0
	for i, s := range inputs {
		now := nows[i%len(nows)]
		in := utf16.Encode([]rune(s))
		lms, lok := parseDateUnits(&dateWork{ctx: context.Background()}, in, now)
		rms, rok := dateUtilsParseRenormalizing(in, now, &stats)
		if lok != rok || lms != rms {
			if differ++; differ <= 20 {
				t.Errorf("%q at %d: linear %d, %v; literal %d, %v", s, now, lms, lok, rms, rok)
			}
		}
	}
	t.Logf("%d dates (%d random), %d retries, %d trimmed on retry", len(inputs), n, stats.retries, stats.retryTrimmed)
	if differ > 0 {
		t.Errorf("%d dates differ", differ)
	}
	if stats.rfcOnRetry != 0 {
		t.Errorf("the RFC 822 pattern answered %d retries", stats.rfcOnRetry)
	}
	if stats.retries == 0 || stats.retryTrimmed == 0 {
		t.Errorf("%d retries, %d trimmed", stats.retries, stats.retryTrimmed)
	}
	for i, step := range dateNormalizeSteps {
		if stats.retryChanged[i] != 0 {
			t.Errorf("the %s step changed %d retries' inputs beyond trimming their start", step.name, stats.retryChanged[i])
		}
		if stats.firstRound[i] == 0 {
			t.Errorf("the %s step never changed a date", step.name)
		}
	}
	for i, f := range dateFallbacks {
		if !f.shadowed && stats.answered[i] == 0 {
			t.Errorf("F%d (%s) never answered", i, f.pattern.text)
		}
		if f.shadowed && stats.answered[i] != 0 {
			t.Errorf("shadowed F%d answered", i)
		}
	}
}

// dateRetryTails are dates that exercise the clean-up steps and patterns.
var dateRetryTails = []string{
	"nothing", "", " ", "Sat, ", "x", "09 Sept 2021 10:00:00 CEST", "09 Sept. 2021 10:00:00 CET",
	"2021-10-09T10:00:00.5Z", "2021-10-09T10:00:00.12X", "2021/10/09", "2021-10-09T10:00:00+01:00\u2028",
	"Sat, 09 Oct 2021 10:00:00  +0000", "Sat,  09 Oct 2021 10:00:00 +01:00", "Sat, 09 Oct 2021 10:00:00 GMT\t+0100",
	"Sat, 09 Oct 2021 10:00:00\t", "\tSat Oct 9 10:00:00 2021", "Saturday, 09 Oct 21 10:00:00",
	"Sat 9 Oct 2021 10:00:00 GMT+0100 (Moscow Summer Time)", "28 Mar 10 02:30:00 Moscow Summer Time",
	"2021-10-09T10:00:00.1E1", "2021-10-09T10:00:00.𝟓𝟓𝟓", "2024-01-02T12:34:56.೧೨೩Z", "1900-08-05T00:34:03.CST",
	"Sat, 09 Oct 2021 10:00:00 +0000\u2028", "a, \n09 Oct 2021 10:00:00 +0000",
}

// dateRandomChain builds a date of random "word, " prefixes before a tail,
// sometimes a rendered date.
func dateRandomChain(r *rand.Rand, tails []string) string {
	// Mostly links a retry passes; sometimes one that ends the chain.
	words := []string{
		"a", "Sat", "Saturday", "Mon", "x_y", "été", "Day1", "日本", "Ⅳ", "\u0301", "𝐀𝐁", "1", "2021", "09",
		"Sept", "NaN", "GMT", "EST", "Oct", "T", "a\u0301", "𝟓",
	}
	breakingWords := []string{"a\u200b", "Sat.", "x-y", "", "∞", "1.5"}
	seps := []string{
		",  ", ", \t", ", \x01", ", \u00a0", ", \n", ",", " , ", ",, ", ", \u2028", ",\u3000", ", \r\n", "/ ",
		", /", ", \u0085",
	}
	var b strings.Builder
	if r.IntN(8) == 0 {
		b.WriteString([]string{" ", "\t", "\r\n", "\u00a0"}[r.IntN(4)])
	}
	for range r.IntN(36) {
		if r.IntN(40) == 0 {
			b.WriteString(breakingWords[r.IntN(len(breakingWords))])
		} else {
			b.WriteString(words[r.IntN(len(words))])
		}
		if r.IntN(20) == 0 {
			b.WriteString(seps[r.IntN(len(seps))])
		} else {
			b.WriteString(", ")
		}
	}
	if r.IntN(2) == 0 {
		b.WriteString(dateRandomRendered(r))
	} else {
		b.WriteString(tails[r.IntN(len(tails))])
	}
	if r.IntN(10) == 0 {
		b.WriteString([]string{" ", "\u2028", "\n", "Z", "  "}[r.IntN(5)])
	}
	return b.String()
}

// dateRandomRendered renders a random date in one of the fallback patterns'
// shapes, with occasional variations.
func dateRandomRendered(r *rand.Rand) string {
	layouts := []string{
		"Mon, 02 Jan 2006 15:04:05 -0700", "02 Jan 06 15:04:05 -0700", "02 Jan 06 15:04 -0700",
		"Mon, 02 Jan 2006 15:04:05", "Monday, 02 Jan 06 15:04:05 -0700", "Monday, 02 Jan 06 15:04:05",
		"Mon Jan 2 15:04:05 2006", "Mon, 02 Jan 2006 15:04 -0700", "Mon, 02 Jan 2006 15:04",
		"Monday, 02 Jan 06 15:04 -0700", "Monday, 02 Jan 06 15:04", "Mon Jan 2 15:04 2006",
		"2006-01-02T15:04:05", "2006-01-02T15:04:05.000 -0700", "2006-01-02T15:04:05.000@",
		"2006-01-02T15:04:05.000", "2006-01-02T15:04:05-0700", "2006-01-02T15:04:05@",
		"2006-01-02T15:04:05.000-0700", "2006-01-02-0700", "2006-01-02", "Mon 2 Jan 2006 15:04:05 GMT-0700",
		"2006-01-02T15:04:05.999999-07:00", "Mon, 02 January 2006 15:04:05 -0700", "02 Jan 2006 15:04:05 MST",
	}
	zones := []*time.Location{
		time.UTC, time.FixedZone("CET", 3600), time.FixedZone("EST", -5*3600), time.FixedZone("IST", 19800),
		time.FixedZone("CEST", 7200),
	}
	at := time.Unix(r.Int64N(2840140800)-631152000, 0).In(zones[r.IntN(len(zones))]) // 1950 to 2040
	if r.IntN(3) == 0 {
		at = at.Add(time.Duration(r.IntN(1000)) * time.Millisecond)
	}
	layout := layouts[r.IntN(len(layouts))]
	s := strings.ReplaceAll(at.Format(layout), "@", "Z")
	if strings.HasSuffix(layout, "GMT-0700") {
		s += " (" + []string{"EST", "Pacific Standard Time", "GMT", "Moscow Summer Time", "UTC", "CET"}[r.IntN(6)] + ")"
	}
	switch r.IntN(12) {
	case 0:
		s = strings.Replace(s, " ", "  ", 1)
	case 1:
		s = strings.ToUpper(s)
	case 2:
		s = strings.ReplaceAll(s, "-", "/")
	case 3:
		s = strings.Replace(s, "Sep ", "Sept ", 1)
	case 4:
		s += []string{" CEST", " CET", "CET", "\u2028", "Z", " "}[r.IntN(6)]
	case 5:
		if len(s) >= 5 && (s[len(s)-5] == '+' || s[len(s)-5] == '-') {
			s = s[:len(s)-2] + ":" + s[len(s)-2:]
		}
	}
	return s
}
