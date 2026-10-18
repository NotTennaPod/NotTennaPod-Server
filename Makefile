GO ?= go
PYTHON ?= python3
VERSION ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X github.com/NotTennaPod/NotTennaPod-Server/internal/version.Version=$(VERSION)
ifndef CONTRACT_SEED
CONTRACT_SEED := $(shell date +%s)
endif

# The storage and importer tests that run smaller under the race detector
# (raceEnabled): check's last step runs them at full size without it.
FULL_SIZE_TESTS = ^(TestQueueAdmissionMatchesTheRangeScan|TestImportQueueAdmissionIsLinear|TestOutOfRankQueueAdmissionIsLinear|TestQueueAdmissionIsFlatInUnrelatedHistory|TestSmallCommandsAfterAWideFact|TestConnectedHistoryCostsOneReadOfIt|TestOfflineBurstPlacementReadsAFewRows|TestRanksArePlacedAsTheEarlierServerPlacedThem|TestPlacementIsTheEarlierLookups|TestNodeAfterManyDependencies|TestImportQueueAdmissionBudget)$$

.PHONY: build check cross-build media-acceptance acceptance-witness docker

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
	$(GO) test -count=1 -timeout 0 -run '^TestCatalogRevisionsResultsUnchanged$$' ./internal/storage
	# Budgets and oracles the race detector runs smaller, at their full size.
	$(GO) test -count=1 -timeout 0 -run '$(FULL_SIZE_TESTS)' ./internal/storage ./internal/importer

cross-build:
	mkdir -p .work/bin
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o .work/bin/nottennapod-server-darwin-arm64 ./cmd/nottennapod-server
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o .work/bin/nottennapod-server-darwin-amd64 ./cmd/nottennapod-server
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o .work/bin/nottennapod-server-linux-arm64 ./cmd/nottennapod-server
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o .work/bin/nottennapod-server-linux-amd64 ./cmd/nottennapod-server

# The media acceptance needs ffmpeg and ffprobe (PROTOCOL §13): here the
# compatibility tests fail instead of skipping without them.
media-acceptance:
	NTP_REQUIRE_FFMPEG=1 $(GO) test -race -count=1 -timeout 0 ./internal/media

# The server's acceptance witness (PROTOCOL §16): on a copy of a stopped
# server's data directory, the database's integrity, every account's log
# digest chain and the rebuild invariant. Only this target runs it: without
# NTP_WITNESS_DATA the test skips.
acceptance-witness:
	@test -n "$(DATA)" || { echo "usage: make acceptance-witness DATA=<a stopped server's data directory>" >&2; exit 2; }
	NTP_WITNESS_DATA="$(abspath $(DATA))" $(GO) test -count=1 -timeout 0 -run '^TestWitness$$' -v ./internal/acceptancecheck

# The container image (docs/OPERATIONS.md, "Running in Docker"), for this
# machine's platform by default; the build fails unless the image's ffmpeg
# passes the media self-check. The Web source is the Dockerfile's pinned
# WEB_COMMIT, fetched with an exported read-only GIT_AUTH_TOKEN while the
# repository is private (passed as a BuildKit secret, never written down), or a
# local checkout: WEB_SRC=../NotTennaPod-Web. RELEASE=1 refuses a dirty Server
# checkout and a WEB_SRC that is not exactly WEB_COMMIT without local changes.
# IMAGE, DOCKER_PLATFORM (e.g. linux/amd64,linux/arm64) and DOCKER_OUTPUT
# (default --load) adjust the result. The build runs in its own BuildKit
# container, DOCKER_BUILDER, created on first use with TZ=UTC and kept for its
# cache, so the image's timestamps (config, history, provenance) are UTC
# whatever the machine's time zone; Docker's own builders are not touched.
IMAGE ?= nottennapod-server
DOCKER_BUILDER ?= ntp-utc
DOCKER_PLATFORM ?=
DOCKER_OUTPUT ?= --load
WEB_SRC ?=
RELEASE ?=
WEB_COMMIT := $(shell sed -n 's/^ARG WEB_COMMIT=//p' Dockerfile)
DOCKER_VERSION := $(VERSION)$(if $(WEB_SRC),$(if $(RELEASE),,+web-local))
DOCKER_TAG := $(subst +,-,$(DOCKER_VERSION))

docker:
	@set -e; \
	if [ -n "$(RELEASE)" ]; then \
		case "$(VERSION)" in *-dirty) echo "make docker: a release needs a clean checkout, not $(VERSION)" >&2; exit 1;; esac; \
		if [ -n "$(WEB_SRC)" ]; then \
			head=$$(git -C "$(WEB_SRC)" rev-parse HEAD); \
			if [ "$$head" != "$(WEB_COMMIT)" ]; then echo "make docker: $(WEB_SRC) is at $$head, not the pinned WEB_COMMIT $(WEB_COMMIT)" >&2; exit 1; fi; \
			if [ -n "$$(git -C "$(WEB_SRC)" status --porcelain)" ] || \
			   [ -n "$$(git -C "$(WEB_SRC)" status --porcelain --ignored -- package.json package-lock.json tsconfig.json scripts src)" ]; then \
				echo "make docker: $(WEB_SRC) has local changes; a release builds only the pinned WEB_COMMIT" >&2; exit 1; fi; \
		fi; \
	fi; \
	src=""; secret=""; \
	if [ -n "$(WEB_SRC)" ]; then \
		src="--build-context web-src=$(abspath $(WEB_SRC))"; \
	elif [ -n "$${GIT_AUTH_TOKEN:-}" ]; then \
		secret="--secret id=GIT_AUTH_TOKEN.github.com,env=GIT_AUTH_TOKEN"; \
	else \
		echo "make docker: GIT_AUTH_TOKEN is not exported, so the private NotTennaPod-Web repository cannot be fetched;" \
			"export a read-only token (GIT_AUTH_TOKEN=\$$(gh auth token)) or build from a checkout with WEB_SRC=../NotTennaPod-Web" >&2; \
	fi; \
	if docker buildx inspect $(DOCKER_BUILDER) >/dev/null 2>&1; then \
		docker buildx inspect $(DOCKER_BUILDER) | grep -q 'env.TZ="UTC"' || { \
			echo "make docker: the builder $(DOCKER_BUILDER) does not run in UTC; remove it (docker buildx rm $(DOCKER_BUILDER)) or choose another DOCKER_BUILDER" >&2; exit 1; }; \
	else \
		docker buildx create --name $(DOCKER_BUILDER) --driver docker-container --driver-opt env.TZ=UTC --bootstrap >/dev/null; \
	fi; \
	set -x; \
	docker buildx build --builder $(DOCKER_BUILDER) $(if $(DOCKER_PLATFORM),--platform $(DOCKER_PLATFORM)) $(DOCKER_OUTPUT) \
		--build-arg SERVER_VERSION=$(DOCKER_VERSION) --build-arg SERVER_REVISION=$$(git rev-parse HEAD 2>/dev/null) --build-arg BUILD_DATE=$$(date -u +%Y-%m-%dT%H:%M:%SZ) \
		$$src $$secret \
		-t $(IMAGE):$(DOCKER_TAG) -t $(IMAGE):$(DOCKER_TAG)-$$(date -u +%Y%m%d) -t $(IMAGE):local .
