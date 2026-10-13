# NotTennaPod Server

The cross-platform server for NotTennaPod's account sync and browser client. Its
behaviour is defined by the [protocol](docs/PROTOCOL.md) and the executable
[contract vectors](docs/CONTRACT.md). Phones remain independently usable when the
server is unavailable.

**AntennaPod version.** The target is AntennaPod's latest tagged release. The parser, import and views are currently built around AntennaPod 3.12.1 and are updated as new releases come out; version numbers in the code, tests and test data record what each part was built and checked against.

**Status:** in development. Implemented: the HTTP process foundation, the
executable wire contract ([`contract/`](docs/CONTRACT.md)), durable storage,
setup, accounts, sign-in and installations, sync, feed refresh and discovery,
backups, restore and imports, the read API and browser client, the browser media
relay and service operation. Integration acceptance with the real clients
([PROTOCOL §16](docs/PROTOCOL.md#16-acceptance-scenarios)) is under way. On first start the server logs a setup code (also in `setup-code` in
its data directory) that the first administrator enters to set it up.

## Stack

Go, standard-library HTTP, and SQLite. Supported deployment targets are macOS and
Linux on ARM64 and x86-64. The language and protocol are independent of the phone
platforms. [ARCHITECTURE](docs/ARCHITECTURE.md) describes the code layout and
[OPERATIONS](docs/OPERATIONS.md) how to run the server for a household.

## Development

Install a supported stable Go release (minimum Go 1.26; from https://go.dev/dl/ or
`brew install go`) and Python 3 for the contract checks. The server is tested with
Go 1.27.1. A server that a browser can set up also needs Node 24 or later and npm
(from https://nodejs.org/ or `brew install node`; see the web client below). From
this directory:

```sh
make check
make build
.work/bin/nottennapod-server -listen 127.0.0.1:8080 -data-dir .work/data
curl --fail http://127.0.0.1:8080/healthz
make cross-build
```

**Web client.** `make build` embeds the browser app from `internal/webui/dist`; on a
fresh checkout there is none, and the browser shows a placeholder page instead of
setup and sign-in. Clone NotTennaPod-Web next to this checkout (as
`../NotTennaPod-Web`), install it and build again:

```sh
cd ../NotTennaPod-Web
npm ci
npm run check                # type check, unit tests, build
npm run install-into-server  # copies dist/ into ../NotTennaPod-Server/internal/webui/dist
cd ../NotTennaPod-Server
make build
```

The installed files are ignored, so the checkout stays clean. Install again and
rebuild after updating NotTennaPod-Web.

Ctrl-C or SIGTERM shuts down the process. Build products belong in `.work/`.
Without `-data-dir` the server keeps its database in `~/Library/Application
Support/NotTennaPod-Server` on macOS and `$XDG_DATA_HOME/nottennapod-server` (or
`~/.local/share/nottennapod-server`) on Linux; only one server may use a data
directory at a time.

**Browser media.** The media relay needs nothing extra. Episodes in a format a
browser cannot play (chiefly Ogg in Safari) are converted with ffmpeg and ffprobe
6 or later, with the `aac` and `libopus` encoders: `brew install ffmpeg` on macOS,
the distribution's `ffmpeg` package on Linux (`apt install ffmpeg` on Debian and
Ubuntu). The server finds them on `PATH`, or through `--ffmpeg PATH` and
`--ffprobe PATH`, and checks every conversion at start (`GET /v1/admin/media`).
Without them the relay still works and such episodes are reported as unsupported.
`make media-acceptance` runs the conversion tests, which fail rather than skip
when ffmpeg is missing.

`make acceptance-witness DATA=<dir>` is the server's acceptance witness: on a copy
of a stopped server's data directory (which it never writes) it checks the
database's integrity, authenticates the archive and the sealed values with the
directory's own keys as a restore does, verifies every account's log digest chain
and runs the rebuild invariant, and names the account and the first divergence of
anything that fails. `make check` never runs it.

`GO=/absolute/path/to/go make check` selects a local toolchain. Cross-compilation
checks build portability; release readiness also requires runtime tests on Linux.
The only external Go dependency is the pure-Go SQLite driver `modernc.org/sqlite`
(and its own dependencies).

`make check` runs packages other than `internal/ops` with the race detector, then
runs ops serially in bounded shards. Each invocation keeps `-count=1`; race and
rule phases keep `-timeout 30m`, and default loads keep Go's original 10-minute
deadline explicitly as `-timeout 10m`. Ops test names come from fresh compiled race and nonrace
inventories, which must agree. All non-rule-parent tests run once under race;
rule bodies are inventoried independently from shared vectors, Server testdata
and any `NTP_RULE_CASES` directory. Exact, escaped selectors run each selected
body once per recipe phase. Shards balance top-level test counts and rule arrival
counts, with at most eight top-level tests or 24 rule bodies per shard (384
arrivals, except a single larger case). No deadline or test assertion is changed.

Every runner phase retains raw Go JSON, stderr, commands and a fresh
`coverage.json` under a unique `.work/check-ops/` directory, including on failure.
Missing, extra or duplicate receipts, unexpected ops skips, empty selections,
changed inputs and invocation/deadline failures fail the command. Repeated rule
parents are setup receipts, separate from unique case-body receipts. A change to
the rule parent's input-discovery algorithm fails explicitly until the runner's
small source contract and roots are updated; adding files or cases to existing
roots needs no runner change.

The seven default load tests intentionally skip under race and then each run
without race at their source defaults: unsubscribe scopes, clear/reset listening
history, offline session upload, rebuild, identity rotation, long queue history
and concentrated staged history. The runner rejects their size/staging environment
overrides. `TestQueueReplayMeasure` is a separate opt-in measurement; leave
`NTP_QUEUE_MEASURE` unset for `make check`, and invoke it directly when needed.
Other packages retain their existing platform, media and witness skip policies.
Model/reference checks, randomized vectors, their reported `CONTRACT_SEED`, and
fixed-seed recovery vectors remain downstream checks. The recovery phase retains
its `rules_random_` subset and records the excluded input cases explicitly.

The runner's focused selection and failure-path tests use a fake Go process,
without compiling or executing the Server tests:

```sh
python3 -m unittest discover -s scripts -p 'test_check_ops.py'
```

## Completion

The [acceptance scenarios](docs/PROTOCOL.md#16-acceptance-scenarios) remain the
completion criteria; a passing health check does not satisfy them.

## Licence

GNU GPL v3.0; see [LICENSE](LICENSE), matching the NotTennaPod app. Not affiliated
with AntennaPod. Any future source adaptations must retain their attribution.
