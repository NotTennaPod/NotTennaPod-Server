GO ?= go
PYTHON ?= python3
VERSION ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X github.com/NotTennaPod/NotTennaPod-Server/internal/version.Version=$(VERSION)
ifndef CONTRACT_SEED
CONTRACT_SEED := $(shell date +%s)
endif

.PHONY: build check cross-build media-acceptance acceptance-witness

build:
	mkdir -p .work/bin
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o .work/bin/nottennapod-server ./cmd/nottennapod-server

check:
	$(PYTHON) -m unittest discover -s scripts -p 'test_check_ops.py'
	test -z "$$($(GO) fmt ./...)"
	$(GO) vet ./...
	$(PYTHON) scripts/check_ops.py other-race --go '$(GO)'
	$(PYTHON) scripts/check_ops.py race --go '$(GO)'
	$(PYTHON) spec/conformance/model_checks.py
	$(PYTHON) contract/reference/selftest.py
	$(PYTHON) contract/reference/check_vectors.py contract/vectors
	@echo "contract random seed: $(CONTRACT_SEED)"
	rm -rf .work/contract-random
	$(GO) run ./cmd/contract-vectors -random 10000 -seed $(CONTRACT_SEED) -out .work/contract-random
	$(PYTHON) contract/reference/check_vectors.py .work/contract-random
	$(PYTHON) contract/reference/selftest_rules.py --quick
	$(PYTHON) contract/reference/selftest_import.py
	$(PYTHON) contract/reference/check_rules.py contract/vectors
	rm -rf .work/rules-random-check
	$(PYTHON) contract/reference/gen_rule_cases.py .work/rules-random-check --cases 40 --seed $(CONTRACT_SEED)
	NTP_RULE_CASES=$(CURDIR)/.work/rules-random-check $(PYTHON) scripts/check_ops.py rules --go '$(GO)'
	rm -rf .work/rules-recovery-check
	$(PYTHON) contract/reference/gen_rule_cases.py .work/rules-recovery-check --recovery --cases 40 --seed 8001
	NTP_RULE_CASES=$(CURDIR)/.work/rules-recovery-check $(PYTHON) scripts/check_ops.py rules --go '$(GO)' --case-prefix rules_random_
	$(PYTHON) scripts/check_ops.py loads --go '$(GO)'

cross-build:
	mkdir -p .work/bin
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o .work/bin/nottennapod-server-darwin-arm64 ./cmd/nottennapod-server
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o .work/bin/nottennapod-server-darwin-amd64 ./cmd/nottennapod-server
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o .work/bin/nottennapod-server-linux-arm64 ./cmd/nottennapod-server
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o .work/bin/nottennapod-server-linux-amd64 ./cmd/nottennapod-server

# The media acceptance needs ffmpeg and ffprobe (PROTOCOL §13): here the
# compatibility tests fail instead of skipping without them.
media-acceptance:
	NTP_REQUIRE_FFMPEG=1 $(GO) test -race -count=1 -timeout 30m ./internal/media

# The server's acceptance witness (PROTOCOL §16): on a copy of a stopped
# server's data directory, the database's integrity, every account's log
# digest chain and the rebuild invariant. Only this target runs it: without
# NTP_WITNESS_DATA the test skips.
acceptance-witness:
	@test -n "$(DATA)" || { echo "usage: make acceptance-witness DATA=<a stopped server's data directory>" >&2; exit 2; }
	NTP_WITNESS_DATA="$(abspath $(DATA))" $(GO) test -count=1 -timeout 60m -run '^TestWitness$$' -v ./internal/acceptancecheck
