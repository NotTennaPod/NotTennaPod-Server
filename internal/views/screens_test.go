package views

import (
	"testing"
)

func TestCompareIgnoreCase(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		sign int
	}{
		{"alpha", "Alpha", 0}, {"alpha", "beta", -1}, {"Zeta", "alpha", 1}, {"", "a", -1},
		{"Émile", "émile", 0}, {"éclair", "zeta", 1}, {"ß", "SS", 1}, {"abc", "ab", 1},
	} {
		got := compareIgnoreCase(tc.a, tc.b)
		if (got < 0 && tc.sign >= 0) || (got > 0 && tc.sign <= 0) || (got == 0 && tc.sign != 0) {
			t.Errorf("compareIgnoreCase(%q, %q) = %d, want sign %d", tc.a, tc.b, got, tc.sign)
		}
	}
}
