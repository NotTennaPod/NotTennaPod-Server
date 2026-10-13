package feed

import (
	"context"
	"math/rand/v2"
	"net/url"
	"strings"
	"testing"
)

// resolveLong decides like net/url: a long reference resolves to the same
// URL, or to "over" only where net/url's URL is longer than the remaining
// budget, and fails where url.Parse fails. Every reference is forced
// through it here.
func TestResolveLongMatchesNetURL(t *testing.T) {
	saved := linkDirect
	linkDirect = 0
	defer func() { linkDirect = saved }()
	bases := []string{"https://example.org/page", "https://example.org/a/b/c?q=1#f", "http://h:8080/", "https://example.org",
		"https://example.org/a/./b/../c/", "https://u@example.org/x/y/"}
	pieces := []string{"/", "/", "//", ".", "..", "./", "../", "/..", "/.", "a", "b", "feed", "%2E", "%2e", "%41", "%zz", "%", "%4",
		":", "?", "??", "#", "##", "@", "[", "]", "^", " ", "é", "http:", "HTTP:", "s:", "mailto:", "//h", "//u@h", "//h:80",
		"//[::1]", "//h:x", "//%zz", "x:y", "1:", "-a:", ":a", "&", "=", ";", "///", "\t"}
	rng := rand.New(rand.NewPCG(20261010, 3))
	check := func(ref string) {
		t.Helper()
		for _, b := range bases {
			base, _ := url.Parse(b)
			u1, ok1 := resolveDirect(percentEncodeLink(strings.TrimSpace(ref), &jsoupWork{ctx: context.Background()}), base)
			for _, remaining := range []int{0, 8, 30, 60, 1 << 30} {
				u2, ok2, over := resolveLink(ref, base, nil, remaining, &jsoupWork{ctx: context.Background()})
				switch {
				case ok1 != ok2:
					t.Fatalf("%q against %s: ok %v, net/url %v (%q)", ref, b, ok2, ok1, u1)
				case over && len(u1) <= remaining:
					t.Fatalf("%q against %s: over %d, but net/url's %q is %d bytes", ref, b, remaining, u1, len(u1))
				case !over && u1 != u2:
					t.Fatalf("%q against %s: %q, net/url %q", ref, b, u2, u1)
				}
			}
		}
	}
	for _, ref := range []string{"", "/", "//", "///", ".", "..", "/feed", "feed", "?", "#", "/a/../b", "a:b", ":a", "//h/./x/..",
		"/" + strings.Repeat("x/", 50) + strings.Repeat("../", 50) + "feed", "http://h/a/b/../../c?q#f", "s:/a/../b", "s:a/../b", "?q#"} {
		check(ref)
	}
	for range 200_000 {
		var b strings.Builder
		for range rng.IntN(10) {
			b.WriteString(pieces[rng.IntN(len(pieces))])
		}
		check(b.String())
	}
}
