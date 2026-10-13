package feed

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

// TestDatesGoldenCorpus checks the parser against testdata/dates_golden.json:
// 43,705 dates with their clocks and answers, covering every pattern, the
// clean-up steps, zone names and offsets, two-digit years, calendar and
// 64-bit edges, numbers, whitespace, weekday retries and Unicode. Cases with
// unpaired surrogates (no Go string can hold them) are given as UTF-16 units.
func TestDatesGoldenCorpus(t *testing.T) {
	b, err := os.ReadFile("testdata/dates_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Header struct {
			Count int `json:"count"`
		} `json:"header"`
		Cases []struct {
			Category string   `json:"category"`
			Input    *string  `json:"input"`
			Units    []uint16 `json:"units"`
			Now      int64    `json:"now"`
			Expected *int64   `json:"expected"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Cases) != doc.Header.Count || len(doc.Cases) == 0 {
		t.Fatalf("%d cases, header says %d", len(doc.Cases), doc.Header.Count)
	}
	failed, units := 0, 0
	fail := func(category, input string, now int64, what string, got, want *int64) {
		if failed++; failed <= 30 {
			t.Errorf("%s %s at now %d: %s = %s, want %s", category, input, now, what, dateFormatMillis(got), dateFormatMillis(want))
		}
	}
	for _, c := range doc.Cases {
		if c.Input == nil {
			units++
			w := &dateWork{ctx: context.Background()}
			var got *int64
			if ms, ok := parseDateUnits(w, c.Units, c.Now); ok {
				got = &ms
			}
			if !dateSameMillis(got, c.Expected) {
				fail(c.Category, jsonQuoteUnits(c.Units), c.Now, "parse", got, c.Expected)
			}
			continue
		}
		var got *int64
		if ms, ok := parseDate(context.Background(), *c.Input, c.Now); ok {
			got = &ms
		}
		if !dateSameMillis(got, c.Expected) {
			fail(c.Category, jsonQuote(*c.Input), c.Now, "parse", got, c.Expected)
		}
		want := c.Expected
		if want != nil && *want > c.Now {
			want = nil
		}
		if got := parseDateOrNullIfFuture(context.Background(), *c.Input, c.Now); !dateSameMillis(got, want) {
			fail(c.Category, jsonQuote(*c.Input), c.Now, "parseOrNullIfFuture", got, want)
		}
	}
	t.Logf("%d of %d cases agree (%d as UTF-16 units)", len(doc.Cases)-failed, len(doc.Cases), units)
	if failed > 0 {
		t.Errorf("%d checks differ", failed)
	}
}

func dateSameMillis(a, b *int64) bool {
	return (a == nil) == (b == nil) && (a == nil || *a == *b)
}

func jsonQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func jsonQuoteUnits(u []uint16) string {
	b, _ := json.Marshal(u)
	return "units " + string(b)
}
