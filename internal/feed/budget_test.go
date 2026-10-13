package feed

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func budgetParse(ctx context.Context, doc string) (*Feed, error) {
	return Parse(ctx, []byte(doc), "https://feeds.example.invalid/budget.xml", time.UnixMilli(corpusNow))
}

func budgetIsParserException(err error) bool {
	var pe *ParseError
	return errors.As(err, &pe) && pe.Label == "parser_exception"
}

// budgetEntities builds a feed whose title uses an entity of size bytes n
// times, after pad bytes of comment.
func budgetEntities(size, n, pad int) string {
	return `<?xml version="1.0"?><!DOCTYPE rss [<!ENTITY big "` + strings.Repeat("x", size) + `">]>` +
		`<!--` + strings.Repeat("p", pad) + `--><rss version="2.0"><channel><title>` + strings.Repeat("&big;", n) +
		`</title></channel></rss>`
}

func TestEntityBudgets(t *testing.T) {
	// Nested entities expand as Expat does.
	f, err := budgetParse(context.Background(), `<?xml version="1.0"?><!DOCTYPE rss [<!ENTITY b "x&#65;"><!ENTITY a "&b;&b;&amp;">]>`+
		`<rss version="2.0"><channel><title>&a;</title></channel></rss>`)
	if err != nil || f.Title == nil || *f.Title != "xAxA&" {
		t.Fatalf("nested entities: %v %v", err, f)
	}
	for _, doc := range []string{
		`<!DOCTYPE rss [<!ENTITY a "&b;"><!ENTITY b "&a;">]><rss version="2.0"><channel><title>&a;</title></channel></rss>`,
		`<!DOCTYPE rss [<!ENTITY a "&nope;">]><rss version="2.0"><channel><title>&a;</title></channel></rss>`,
	} {
		if _, err := budgetParse(context.Background(), doc); !budgetIsParserException(err) {
			t.Errorf("a recursive or undefined entity: %v", err)
		}
	}
	// 1 MiB of expansions from a small document: within both limits.
	if f, err := budgetParse(context.Background(), budgetEntities(1024, 1024, 0)); err != nil || len(*f.Title) != 1<<20 {
		t.Errorf("1 MiB of expansions: %v", err)
	}
	// Over Expat's amplification limit (Android refuses it too): 9 MiB from 9 KiB.
	if _, err := budgetParse(context.Background(), budgetEntities(9*1024, 1024, 0)); !budgetIsParserException(err) {
		t.Errorf("over Expat's amplification: %v", err)
	}
	// Below Expat's limit (amplification under 100 thanks to 200 KiB of
	// input) but over the server's 16 MiB budget.
	if _, err := budgetParse(context.Background(), budgetEntities(64*1024, 260, 200*1024)); !budgetIsParserException(err) {
		t.Errorf("over the absolute budget: %v", err)
	}
}

func TestStructureBudgets(t *testing.T) {
	nested := func(depth int) string {
		return `<rss version="2.0"><channel><title>t</title>` + strings.Repeat("<x>", depth-2) + strings.Repeat("</x>", depth-2) + `</channel></rss>`
	}
	if _, err := budgetParse(context.Background(), nested(maxDepth)); err != nil {
		t.Errorf("%d levels: %v", maxDepth, err)
	}
	if _, err := budgetParse(context.Background(), nested(maxDepth+1)); !budgetIsParserException(err) {
		t.Errorf("%d levels: %v", maxDepth+1, err)
	}
	items := func(n int) string {
		return `<rss version="2.0"><channel><title>t</title>` + strings.Repeat("<item><title>i</title></item>", n) + `</channel></rss>`
	}
	if f, err := budgetParse(context.Background(), items(maxItems)); err != nil || len(f.Items) != maxItems {
		t.Errorf("%d items: %v", maxItems, err)
	}
	if _, err := budgetParse(context.Background(), items(maxItems+1)); !budgetIsParserException(err) {
		t.Errorf("%d items: %v", maxItems+1, err)
	}
}

// budgetCancelAfter is a context cancelled after a number of Err checks.
type budgetCancelAfter struct {
	context.Context
	checks int
}

func (c *budgetCancelAfter) Err() error {
	if c.checks--; c.checks < 0 {
		return context.Canceled
	}
	return nil
}

// Cancellation stops a parse midway (the fetch deadline covers parsing).
func TestParseCancellation(t *testing.T) {
	doc := `<rss version="2.0"><channel><title>t</title>` + strings.Repeat("<item><title>i</title></item>", 10_000) + `</channel></rss>`
	ctx := &budgetCancelAfter{Context: context.Background(), checks: 3}
	if _, err := budgetParse(ctx, doc); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled midway: %v", err)
	}
	if ctx.checks >= 0 {
		t.Error("the parse never checked its context")
	}
}

// The HTML conversion of a title stops at the deadline too: TagSoup needs
// quadratic time for some inputs.
func TestParseCancellationInHTML(t *testing.T) {
	doc := `<rss version="2.0"><channel><title>` + strings.Repeat("&lt;/b&gt;", 50_000) + `</title></channel></rss>`
	ctx := &budgetCancelAfter{Context: context.Background(), checks: 1} // the parse's first check passes
	if _, err := budgetParse(ctx, doc); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled in the title's conversion: %v", err)
	}
}
