package feed

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"testing"
)

// htmlVectorCase is an html_compact or html_legacy case of
// contract/vectors/feed_values.json: HtmlCompat.fromHtml(input, mode).toString()
// recorded on the API 36 emulator.
type htmlVectorCase struct {
	Kind   string          `json:"kind"`
	Input  json.RawMessage `json:"input"`
	Output json.RawMessage `json:"output"`
}

func htmlVectorCases(t *testing.T) []htmlVectorCase {
	t.Helper()
	b, err := os.ReadFile("../../contract/vectors/feed_values.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []htmlVectorCase `json:"cases"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	var out []htmlVectorCase
	for _, c := range doc.Cases {
		if c.Kind == "html_compact" || c.Kind == "html_legacy" {
			out = append(out, c)
		}
	}
	return out
}

// htmlLoneSurrogate finds a \u escape of a surrogate that is not part of a
// pair: encoding/json reads it as U+FFFD.
var htmlLoneSurrogate = regexp.MustCompile(`(?i)\\ud[89ab][0-9a-f]{2}(?:\\ud[c-f][0-9a-f]{2})?|\\ud[c-f][0-9a-f]{2}`)

func htmlHasLoneSurrogate(raw json.RawMessage) bool {
	for _, m := range htmlLoneSurrogate.FindAll(raw, -1) {
		if len(m) != 12 {
			return true
		}
	}
	return false
}

func TestHTMLVectors(t *testing.T) {
	cases := htmlVectorCases(t)
	if len(cases) != 12000 {
		t.Errorf("%d html cases, want 12000", len(cases))
	}
	counts := map[string]int{}
	surrogates, surrogateMismatches, failed := 0, 0, 0
	for _, c := range cases {
		var in string
		var want *string
		if err := json.Unmarshal(c.Input, &in); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(c.Output, &want); err != nil {
			t.Fatal(err)
		}
		lone := htmlHasLoneSurrogate(c.Input) || htmlHasLoneSurrogate(c.Output)
		if lone {
			surrogates++
		}
		got := htmlText(context.Background(), in, c.Kind == "html_compact")
		counts[c.Kind]++
		if want != nil && got == *want {
			continue
		}
		if lone {
			surrogateMismatches++
		}
		if failed++; failed <= 15 {
			w := "null"
			if want != nil {
				w = *want
			}
			t.Errorf("%s %s:\n got %q\nwant %q", c.Kind, c.Input, got, w)
		}
	}
	t.Logf("html_compact %d, html_legacy %d; %d with unpaired surrogates (%d differ)", counts["html_compact"],
		counts["html_legacy"], surrogates, surrogateMismatches)
	if failed > 0 {
		t.Errorf("%d of %d html cases differ", failed, len(cases))
	}
}

// TestHTMLThrows checks the inputs on which Html.fromHtml throws, and near
// misses that do not.
func TestHTMLThrows(t *testing.T) {
	throws := []string{`<font color="-">x</font>`, "<font color=&#45;>x", "<!DOCTYPE html\u3000>", "<!x \u2028>",
		"<!DOCTYPE html SYSTEM '>", `<!doctype html PUBLIC "a" ">`, `<!DOCTYPE html PUBLIC '>`}
	for _, in := range throws {
		func() {
			defer func() {
				if err, ok := recover().(error); !ok || !errors.Is(err, htmlErrException) || !errors.Is(err, errNull) {
					t.Errorf("htmlText(%q) did not throw", in)
				}
			}()
			htmlText(context.Background(), in, true)
		}()
	}
	fine := map[string]string{
		`<font color="-1">x</font>`:                "x",
		`<font color="">x</font>`:                  "x",
		"<!DOCTYPE html>x":                         "x",
		"<!x '\u2028'>x":                           "x",
		"<!DOCTYPE html SYSTEM ''>x":               "x",
		"<!DOCTYPE html system '>x":                "x",
		"<!DOCTYPE html><!DOCTYPE html SYSTEM '>x": "x",
		`<!DOCTYPE html PUBLIC "a" "b" '>x`:        "x",
	}
	for in, want := range fine {
		if got := htmlText(context.Background(), in, false); got != want {
			t.Errorf("htmlText(%q) = %q, want %q", in, got, want)
		}
	}
}
