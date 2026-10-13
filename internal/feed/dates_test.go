package feed

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"
)

// dateCase is a date case of contract/vectors/feed_values.json, recorded
// from DateUtils on the API 36 emulator.
type dateCase struct {
	Kind   string `json:"kind"`
	Input  string `json:"input"`
	Output *int64 `json:"output"`
}

// dateLoadCases returns the vectors' date cases of a kind and the generation
// time, which stands for the device clock.
func dateLoadCases(t *testing.T, kind string) ([]dateCase, int64) {
	t.Helper()
	b, err := os.ReadFile("../../contract/vectors/feed_values.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		GeneratedAt string            `json:"generated_at"`
		Cases       []json.RawMessage `json:"cases"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	at, err := time.Parse(time.RFC3339, doc.GeneratedAt)
	if err != nil {
		t.Fatal(err)
	}
	var out []dateCase
	for _, raw := range doc.Cases {
		var c dateCase
		if json.Unmarshal(raw, &c) == nil && c.Kind == kind {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		t.Fatalf("no %s cases", kind)
	}
	return out, at.UnixMilli()
}

func dateFormatMillis(p *int64) string {
	if p == nil {
		return "null"
	}
	return strconv.FormatInt(*p, 10) + " (" + time.UnixMilli(*p).UTC().Format("2006-01-02T15:04:05.000Z") + ")"
}

func dateCheck(t *testing.T, kind string, fn func(string, int64) *int64) {
	cases, now := dateLoadCases(t, kind)
	failed := 0
	for _, c := range cases {
		got := fn(c.Input, now)
		if (got == nil) != (c.Output == nil) || (got != nil && *got != *c.Output) {
			if failed++; failed <= 20 {
				t.Errorf("%s(%q) = %s, want %s", kind, c.Input, dateFormatMillis(got), dateFormatMillis(c.Output))
			}
		}
	}
	t.Logf("%s: %d of %d cases agree", kind, len(cases)-failed, len(cases))
	if failed > 0 {
		t.Errorf("%s: %d of %d cases differ", kind, failed, len(cases))
	}
}

func TestDateParseVectors(t *testing.T) {
	dateCheck(t, "date_parse", func(s string, now int64) *int64 {
		if ms, ok := parseDate(context.Background(), s, now); ok {
			return &ms
		}
		return nil
	})
}

func TestDateParseOrNullIfFutureVectors(t *testing.T) {
	dateCheck(t, "date_parse_or_null_if_future", func(s string, now int64) *int64 {
		return parseDateOrNullIfFuture(context.Background(), s, now)
	})
}
