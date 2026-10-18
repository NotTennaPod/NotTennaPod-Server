package views_test

// Source integrity of the read model's tests: the oracle stays frozen and
// independent of the code it checks, and nothing a server runs reaches the
// oracle, the test harness or the storage seams for tests.

import (
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/NotTennaPod/NotTennaPod-Server"

// The oracle's files: library.go, filter.go, screens.go and encode.go are
// views' at 9ee1e1d with only the package name and the reads changed;
// queries.go holds its own copies of the four SQL reads (the json_remove
// literal included); export.go its accessors for the tests; api.go the
// read API's composition of responses at 9ee1e1d (api/read.go); surprise.go
// a naive implementation of Surprise's specification.
var referenceHashes = map[string]string{
	"api.go":      "2f40c778cbf0080826b74a0ed79ad8250f62a358aa6a70f431a640b9facf1c1c",
	"surprise.go": "6181ce52fa497e8f5a733561bde01d4c7feab5ba26bf24a397a1fd313c047074",
	"encode.go":   "6b2bb28bb59eb1f963652cea4852a818ec2c28ae39cc2f9999c6f59a2c8a776a",
	"export.go":   "a81afcdae16fe22ad8349af78a561d166dc358b48cfdf8f5d4cf3dcd46a13e3b",
	"filter.go":   "975bbc5f5728150067eb0bdbeef3c1556118f55e26d3083802699a3baa91e310",
	"library.go":  "54bf9966d78d4866f3edae80a10b8c0a8f723b5d90328cb85e2cb000c6765de5",
	"queries.go":  "09c773523de58ea43355aaabbac7dd8a1dfa863cc4c6d486988fadc5efa0149e",
	"screens.go":  "348e90ae63f135ccebea344ddd92a97ab96248a2066d6137aaeeb36166cec137",
}

// The storage names the oracle may use: row types, and the raw read.
var referenceStorageNames = []string{"CatalogRow", "EpisodeRow", "ErrNoAccount", "Head", "RawReadForTesting", "ReadTx", "RecordRow"}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test")
		}
		dir = parent
	}
}

// TestReferenceIsFrozen: the oracle's files are exactly the pinned ones; it
// imports neither the read model nor its harness, reaches storage only
// through its row types and the raw read, and reads records with its own
// copy of the list views' projection.
func TestReferenceIsFrozen(t *testing.T) {
	dir := filepath.Join(moduleRoot(t), "internal", "views", "reference")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	fset := token.NewFileSet()
	for _, e := range entries {
		names = append(names, e.Name())
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		if got := hex.EncodeToString(sum[:]); got != referenceHashes[e.Name()] {
			t.Errorf("reference/%s changed (sha256 %s): the oracle is frozen", e.Name(), got)
		}
		f, err := parser.ParseFile(fset, e.Name(), b, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if strings.HasPrefix(p, module+"/internal/views") {
				t.Errorf("reference/%s imports %s", e.Name(), p)
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "storage" && !slices.Contains(referenceStorageNames, sel.Sel.Name) {
					t.Errorf("reference/%s uses storage.%s", e.Name(), sel.Sel.Name)
				}
			}
			return true
		})
	}
	if len(names) != len(referenceHashes) {
		t.Errorf("the oracle's files are %v", names)
	}
	q, _ := os.ReadFile(filepath.Join(dir, "queries.go"))
	if !strings.Contains(string(q), `json_remove(value, '$.description', '$.chapters')`) {
		t.Error("the oracle's queries lost their own copy of the projection")
	}
}

type goPackage struct {
	imports []string
	calls   map[string][]string // *ForTesting names → where
}

// productionPackages parses every non-test Go file of the module.
func productionPackages(t *testing.T, root string) map[string]*goPackage {
	t.Helper()
	pkgs := map[string]*goPackage{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); path != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, _ := filepath.Rel(root, filepath.Dir(path))
		ip := module
		if rel != "." {
			ip += "/" + filepath.ToSlash(rel)
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		p := pkgs[ip]
		if p == nil {
			p = &goPackage{calls: map[string][]string{}}
			pkgs[ip] = p
		}
		for _, imp := range f.Imports {
			s, _ := strconv.Unquote(imp.Path.Value)
			if strings.HasPrefix(s, module) {
				p.imports = append(p.imports, s)
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			var name string
			switch x := n.(type) {
			case *ast.SelectorExpr:
				name = x.Sel.Name
			case *ast.CallExpr:
				if id, ok := x.Fun.(*ast.Ident); ok {
					name = id.Name
				}
			}
			if strings.HasSuffix(name, "ForTesting") {
				p.calls[name] = append(p.calls[name], fset.Position(n.Pos()).String())
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return pkgs
}

// TestTestSeamsStayInTests: the programs (cmd/...) reach neither the oracle
// nor the harness, and outside them only storage itself uses its seams for
// tests.
func TestTestSeamsStayInTests(t *testing.T) {
	root := moduleRoot(t)
	pkgs := productionPackages(t, root)
	harness := []string{module + "/internal/views/reference", module + "/internal/views/viewstest"}
	reached := map[string]bool{}
	var walk func(string)
	walk = func(p string) {
		if reached[p] {
			return
		}
		reached[p] = true
		if pkg := pkgs[p]; pkg != nil {
			for _, q := range pkg.imports {
				walk(q)
			}
		}
	}
	programs := 0
	for p := range pkgs {
		if strings.HasPrefix(p, module+"/cmd/") {
			programs++
			walk(p)
		}
	}
	if programs == 0 || !reached[module+"/internal/views"] || !reached[module+"/internal/storage"] {
		t.Fatalf("the programs' import graph looks wrong: %d programs, %d packages", programs, len(reached))
	}
	for _, h := range harness {
		if reached[h] {
			t.Errorf("a program reaches %s", h)
		}
	}
	for p, pkg := range pkgs {
		if p == module+"/internal/storage" || slices.Contains(harness, p) {
			continue
		}
		for name, where := range pkg.calls {
			t.Errorf("%s uses %s outside the tests: %v", p, name, where)
		}
	}
}
