package feed

import (
	"testing"
	"unicode"
)

// dateDigitValue relies on Go's Nd table being runs of whole sets of ten
// digits, each starting at a zero.
func TestDateDigitValues(t *testing.T) {
	check := func(lo, hi, stride uint32) {
		if stride != 1 || (hi-lo+1)%10 != 0 {
			t.Errorf("Nd range %#x-%#x (stride %d) is not whole sets of ten", lo, hi, stride)
			return
		}
		for r := lo; r <= hi; r++ {
			if got, want := dateDigitValue(rune(r)), int(r-lo)%10; got != want {
				t.Errorf("value of %#x = %d, want %d", r, got, want)
			}
		}
	}
	for _, r := range unicode.Nd.R16 {
		check(uint32(r.Lo), uint32(r.Hi), uint32(r.Stride))
	}
	for _, r := range unicode.Nd.R32 {
		check(r.Lo, r.Hi, r.Stride)
	}
	for r, want := range map[rune]int{'7': 7, 0x0665: 5, 0xFF11: 1, 0x0CE9: 3, 0x1D7D3: 5, 0x1D7DA: 2} {
		if got := dateDigitValue(r); got != want {
			t.Errorf("value of %U = %d, want %d", r, got, want)
		}
	}
}
