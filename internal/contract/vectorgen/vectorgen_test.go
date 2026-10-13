package vectorgen

import (
	"bytes"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// TestVectorsUpToDate fails when the checked-in vectors differ from what the Go
// implementation produces now: either the implementation changed behaviour (a
// contract break) or the vectors were not regenerated with cmd/contract-vectors.
func TestVectorsUpToDate(t *testing.T) {
	dir := filepath.Join("..", "..", "..", "contract", "vectors")
	for _, s := range Fixed() {
		want, err := s.Encode()
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(dir, s.Name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s.json is out of date with the Go implementation", s.Name)
		}
	}
}

func TestRandomIsReproducible(t *testing.T) {
	a, b := Random(7, 2000), Random(7, 2000)
	total := 0
	for i := range a {
		x, _ := a[i].Encode()
		y, _ := b[i].Encode()
		if !bytes.Equal(x, y) {
			t.Fatalf("suite %s not reproducible", a[i].Name)
		}
		total += len(a[i].Cases)
	}
	if total != 2000 {
		t.Fatalf("got %d cases, want 2000", total)
	}
}

// TestURLNormalizationIdempotent checks normalize(normalize(x)) == normalize(x)
// over the randomized URL generator (PROTOCOL §3).
func TestURLNormalizationIdempotent(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for i := 0; i < 200000; i++ {
		u := randomURL(r)
		once := contract.NormalizeURL(u)
		if twice := contract.NormalizeURL(once); twice != once {
			t.Fatalf("not idempotent: %q -> %q -> %q", u, once, twice)
		}
	}
}
