package feed

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
)

// valuesCase is one case of contract/vectors/feed_values.json, recorded from
// Android's own classes on the API 36 emulator.
type valuesCase struct {
	Kind   string          `json:"kind"`
	Input  json.RawMessage `json:"input"`
	Output json.RawMessage `json:"output"`
}

func valuesCases(t *testing.T, kind string) []valuesCase {
	t.Helper()
	b, err := os.ReadFile("../../contract/vectors/feed_values.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []valuesCase `json:"cases"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	var out []valuesCase
	for _, c := range doc.Cases {
		if c.Kind == kind {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		t.Fatalf("no %s cases", kind)
	}
	return out
}

// valuesCheck runs fn over a kind's cases and reports mismatches.
func valuesCheck(t *testing.T, kind string, fn func(input json.RawMessage) any) {
	t.Helper()
	cases := valuesCases(t, kind)
	failed := 0
	for _, c := range cases {
		got, _ := json.Marshal(fn(c.Input))
		var want any
		json.Unmarshal(c.Output, &want)
		w, _ := json.Marshal(want)
		if string(got) != string(w) {
			if failed++; failed <= 10 {
				t.Errorf("%s(%s) = %s, want %s", kind, c.Input, got, w)
			}
		}
	}
	if failed > 0 {
		t.Errorf("%s: %d of %d cases differ", kind, failed, len(cases))
	}
}

func valuesString(t *testing.T, raw json.RawMessage) *string {
	var s *string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

// longOrError renders a Java long as Android's exporter does: a decimal
// string, or "error" for a NumberFormatException.
func valuesLongOrError(v int64, ok bool) any {
	if !ok {
		return "error"
	}
	return strconv.FormatInt(v, 10)
}

func TestValuesDurations(t *testing.T) {
	valuesCheck(t, "duration_in_millis", func(in json.RawMessage) any {
		return valuesLongOrError(durationMillis(*valuesString(t, in)))
	})
	valuesCheck(t, "parse_time_string", func(in json.RawMessage) any {
		return valuesLongOrError(parseTimeString(*valuesString(t, in)))
	})
}

func TestValuesMIMETypes(t *testing.T) {
	valuesCheck(t, "mime_type", func(in json.RawMessage) any {
		var pair [2]*string
		if err := json.Unmarshal(in, &pair); err != nil {
			t.Fatal(err)
		}
		return mimeType(pair[0], pair[1])
	})
	valuesCheck(t, "mime_type_from_extension", func(in json.RawMessage) any {
		return mimeTypeFromExtension(*valuesString(t, in))
	})
}

// DateUtils.parseOrNullIfFuture drops a date after now, with now injected
// (the emulator's clock cannot be: PROTOCOL §9.2).
func TestValuesFutureDateBoundary(t *testing.T) {
	const s = "Thu, 01 Jan 2026 00:00:00 +0000"
	at := int64(1767225600000)
	if v, ok := parseDate(context.Background(), s, at); !ok || v != at {
		t.Fatalf("parseDate(%q) = %d, %v", s, v, ok)
	}
	for now, want := range map[int64]bool{at - 1: false, at: true, at + 1: true} {
		if got := parseDateOrNullIfFuture(context.Background(), s, now); (got != nil) != want {
			t.Errorf("now %+d ms from the date: kept %v, want %v", now-at, got != nil, want)
		}
	}
}
