package feed

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// The feed corpus (contract/vectors/feeds): Android 3.12.1's own parser's
// results on the API 36 emulator, one row per file (PROVENANCE.md there).
const (
	corpusDir    = "../../contract/vectors/feeds"
	corpusSource = "https://feeds.example.invalid/"
	corpusNow    = 1791227192000 // the generation time: every date is a day before it or after 2100
)

type corpusRow struct {
	File   string         `json:"file"`
	Result map[string]any `json:"result"`
}

func corpusRows(t *testing.T) []corpusRow {
	t.Helper()
	f, err := os.Open(filepath.Join(corpusDir, "expected.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<20), 64<<20)
	var rows []corpusRow
	for sc.Scan() {
		var r corpusRow
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// corpusResult is Parse's outcome in the rows' schema.
func corpusResult(t *testing.T, file string) map[string]any {
	raw, err := os.ReadFile(filepath.Join(corpusDir, file))
	if err != nil {
		t.Fatal(err)
	}
	f, err := Parse(context.Background(), raw, corpusSource+file, time.UnixMilli(corpusNow))
	var pe *ParseError
	switch {
	case errors.As(err, &pe):
		var detail any
		if pe.Detail != "" {
			detail = pe.Detail
		}
		return map[string]any{"error": pe.Label, "detail": detail}
	case err != nil:
		t.Fatalf("%s: %v", file, err)
	}
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// corpusDiff lists the paths where two results differ (items by index).
func corpusDiff(path string, a, b any, out *[]string) {
	am, aok := a.(map[string]any)
	bm, bok := b.(map[string]any)
	if aok && bok {
		keys := map[string]bool{}
		for k := range am {
			keys[k] = true
		}
		for k := range bm {
			keys[k] = true
		}
		for k := range keys {
			corpusDiff(path+"."+k, am[k], bm[k], out)
		}
		return
	}
	as, aok := a.([]any)
	bs, bok := b.([]any)
	if aok && bok && len(as) == len(bs) {
		for i := range as {
			corpusDiff(path+"[]", as[i], bs[i], out)
		}
		return
	}
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	if string(x) != string(y) {
		*out = append(*out, path)
	}
}

// TestCorpus parses every corpus file and compares the result with Android's.
// Hostile files compare only the outcome: the feed type, or the error label
// (PROTOCOL §9.2: Android has no budgets).
func TestCorpus(t *testing.T) {
	rows := corpusRows(t)
	byField := map[string][]string{}
	failed := 0
	for _, r := range rows {
		got, want := corpusResult(t, r.File), r.Result
		if strings.HasPrefix(r.File, "hostile-") {
			got = map[string]any{"error": got["error"], "type": got["type"]}
			want = map[string]any{"error": want["error"], "type": want["type"]}
		}
		var paths []string
		corpusDiff("", got, want, &paths)
		if only := os.Getenv("NTP_CORPUS_FIELDS"); only != "" { // while investigating: these paths only
			var kept []string
			for _, p := range paths {
				if strings.Contains(","+only+",", ","+p+",") {
					kept = append(kept, p)
				}
			}
			paths = kept
		}
		if len(paths) == 0 {
			continue
		}
		failed++
		seen := map[string]bool{}
		for _, p := range paths {
			if !seen[p] {
				seen[p] = true
				byField[p] = append(byField[p], r.File)
			}
		}
		if failed <= 5 {
			g, _ := json.Marshal(got)
			w, _ := json.Marshal(want)
			t.Errorf("%s differs at %v:\n  go:      %.600s\n  android: %.600s", r.File, paths, g, w)
		}
	}
	if failed > 0 {
		fields := make([]string, 0, len(byField))
		for f := range byField {
			fields = append(fields, f)
		}
		sort.Slice(fields, func(i, j int) bool { return len(byField[fields[i]]) > len(byField[fields[j]]) })
		var b strings.Builder
		for _, f := range fields {
			fmt.Fprintf(&b, "\n  %-28s %4d  e.g. %s", f, len(byField[f]), strings.Join(byField[f][:min(3, len(byField[f]))], ", "))
		}
		t.Errorf("%d of %d files differ; by field:%s", failed, len(rows), b.String())
	}
}
