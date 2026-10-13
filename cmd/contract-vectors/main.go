// Command contract-vectors writes the contract-v1 test vectors (docs/CONTRACT.md).
//
//	contract-vectors -out contract/vectors                       # checked-in vectors
//	contract-vectors -random 10000 -seed 1 -out .work/contract-random
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract/vectorgen"
)

func main() {
	out := flag.String("out", "contract/vectors", "output directory")
	random := flag.Int("random", 0, "write this many randomized cases instead of the fixed vectors")
	seed := flag.Uint64("seed", 1, "seed for -random")
	flag.Parse()

	suites := vectorgen.Fixed()
	if *random > 0 {
		suites = vectorgen.Random(*seed, *random)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, s := range suites {
		data, err := s.Encode()
		if err == nil {
			err = os.WriteFile(filepath.Join(*out, s.Name+".json"), data, 0o644)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, s.Name+":", err)
			os.Exit(1)
		}
	}
}
