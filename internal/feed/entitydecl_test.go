package feed

import (
	"context"
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// entityDeclReference is the regular expression the declarations were read
// with before entityDeclarations, which must find exactly its matches.
var entityDeclReference = regexp.MustCompile(`<!ENTITY\s+([^\s%]+)\s+(?:"([^"]*)"|'([^']*)'|(SYSTEM|PUBLIC)\b[^>]*)\s*>`)

func TestEntityDeclarationsMatchTheExpression(t *testing.T) {
	check := func(dir string) {
		t.Helper()
		var want, got [][2]string
		for _, m := range entityDeclReference.FindAllStringSubmatch(dir, -1) {
			want = append(want, [2]string{m[1], m[2] + m[3]})
		}
		if err := entityDeclarations(dir, &entityWork{ctx: context.Background()}, func(n, v string) {
			got = append(got, [2]string{n, v})
		}); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("%q:\n got %q\nwant %q", dir, got, want)
		}
	}
	for _, dir := range []string{
		``, `DOCTYPE rss`, `DOCTYPE rss [<!ENTITY a "x">]`, `DOCTYPE rss [<!ENTITY a 'x'><!ENTITY b "y" >]`,
		`DOCTYPE rss [<!ENTITY % pe "x"><!ENTITY a SYSTEM "u"><!ENTITY b PUBLIC "p" "u">]`,
		`DOCTYPE rss [<!ENTITY a "x <!ENTITY b "y">]`, `<!ENTITY a SYSTEMx>`, `<!ENTITY a SYSTEM_>`, `<!ENTITY a SYSTEM>`,
		`<!ENTITY a "x"y>`, `<!ENTITY a%b "x">`, `<!ENTITY	a` + "\f" + `"x"` + "\r\n>", "<!ENTITY a\v\"x\">", `<!ENTITY a "x`,
		`<!ENTITY a "x" <!ENTITY b 'y'>`, `<!ENTITY a "x"` + strings.Repeat(" ", 100) + `>`, `<!ENTITY é "\xff">`, `<!ENTITY a ""><!ENTITY a "">`,
		`"<!ENTITY a SYSTEM <!ENTITY b SYSTEM <!ENTITY c SYSTEM`, `<!ENTITY a "x' <!ENTITY b 'y" >'>`,
		`<!ENTITY<!ENTITY<!ENTITY>>>`, `<!ENTITY<!ENTITY a "x">>`, `<!ENTITY <!ENTITY a "x">>`, `<!ENTITY<!ENTITY a<!ENTITY b 'y'>>`,
		`<!ENTITY a<!ENTITY b "x">`, `<!ENTITY% a "x"><!ENTITY b "y">`,
	} {
		check(dir)
	}
	// Linear where every attempt fails late: no '>' after many SYSTEM
	// declarations, no closing quote after many opening ones, and markers
	// nested without whitespace (each one's name would run to the end).
	for _, dir := range []string{strings.Repeat("<!ENTITY a SYSTEM ", 100_000), strings.Repeat(`<!ENTITY a "x" `, 100_000),
		strings.Repeat("<!ENTITY", 100_000) + strings.Repeat(">", 100_000),
		strings.Repeat("<!ENTITY%", 100_000) + strings.Repeat(">", 100_000),
		strings.Repeat("<!ENTITY ", 100_000) + strings.Repeat(">", 100_000)} {
		check(dir)
		quadraticWithin(t, 5*time.Second, dir[:18], func() {
			if err := entityDeclarations(dir, &entityWork{ctx: context.Background()}, func(string, string) {}); err != nil {
				t.Fatal(err)
			}
		})
	}
	pieces := []string{"<!ENTITY", "<!ENTITY", "<!ENTITY ", " ", " ", "\t", "\n", "\f", "\r", "\v", "%", "a", "b", "é", "x1", `"`, `"`,
		"'", "'", ">", ">", "<", "!", "SYSTEM", "PUBLIC", "SYSTEMx", "PUBLIC_", "S", "P", "&#65;", "\xff", "]", "[", "--"}
	rng := rand.New(rand.NewPCG(20261009, 7))
	for range 300_000 {
		var b strings.Builder
		for range rng.IntN(16) {
			b.WriteString(pieces[rng.IntN(len(pieces))])
		}
		check(b.String())
	}
	// Markers nested without whitespace, among declarations.
	nested := []string{"<!ENTITY", "<!ENTITY", "<!ENTITY ", ">", ">", " ", "a", `"x"`, `'y'`, "%", "SYSTEM"}
	for range 100_000 {
		var b strings.Builder
		for range rng.IntN(24) {
			b.WriteString(nested[rng.IntN(len(nested))])
		}
		check(b.String())
	}
	// Mostly well-formed declarations, mutated.
	parts := [][]string{{"<!ENTITY"}, {" ", "\t", "", "  "}, {"a", "b", "%", "é", "a%"}, {" ", "", "\n"},
		{`"v"`, `'v'`, `"v`, `'v"`, "SYSTEM 'u'", "PUBLIC \"p\" \"u\"", "SYSTEMx", "\"a>b\""}, {"", " ", "\r"}, {">", "", "x>", " >"}}
	for range 100_000 {
		var b strings.Builder
		for range 1 + rng.IntN(4) {
			for _, p := range parts {
				b.WriteString(p[rng.IntN(len(p))])
			}
			if rng.IntN(4) == 0 {
				b.WriteString(pieces[rng.IntN(len(pieces))])
			}
		}
		check(b.String())
	}
}
