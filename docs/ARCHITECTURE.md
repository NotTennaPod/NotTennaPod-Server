# Architecture

NotTennaPod-Server is one Go binary (`cmd/nottennapod-server`) with one SQLite database per data directory. It speaks the protocol in [PROTOCOL.md](PROTOCOL.md), refreshes feeds, and serves the web client. The only external Go dependency is the pure-Go SQLite driver `modernc.org/sqlite`; ffmpeg and ffprobe are an optional runtime dependency of the media relay.

## Packages

| Package | Role |
| --- | --- |
| `cmd/nottennapod-server` | the command: serving (no subcommand), `backup`, `keys export`/`install`, `restore`, `service …`, `--version`; flags, startup and shutdown |
| `cmd/contract-vectors` | generates randomized contract vectors for the Python cross-check (`make check`) |
| `internal/contract` | contract-v1 primitives: canonical JSON, strict decoding, IDs, URL normalization, GUIDs, queue keys, hashes, MACs, digests, cursors, causal order, time normalization (PROTOCOL §3, §6.9) |
| `internal/storage` | the store: SQLite connections and write serialization, migrations and the exact-schema check, the account-scoped API (`tx.Account(id)`), commits and the digest chain, facts, receipts, decisions, projections, backups, restore metadata and the restore transition, keys, imports' staging and publication |
| `internal/protocol` | the sync protocol: typed envelopes, the operation registry, push admission and transport slots, framed parts, pulls, snapshot views, recovery, staged rebuilds, server-originated commits |
| `internal/projection` | the episode, podcast and account folds over contributions, and the incremental causal order (PROTOCOL §7.1, §8.3) |
| `internal/ops` | the operation types and decisions: subscriptions, episode state, Inbox, favorites, listening, history, statistics cuts, the queue and its replay and layouts, catalog observations, discovery and admission, identity reconciliation, imports |
| `internal/queue` | the queue replay and Android's canonical sort orders (PROTOCOL §7.6) |
| `internal/feed` | the fetcher and the port of Android's feed parser; date and HTML tables generated from AOSP (`internal/feed/internal/gendatezones`, `gentagsoup`) |
| `internal/refresh` | the server's refresh scheduler: persistent deadlines, fairness between accounts, backoff, wake catch-up, re-parse |
| `internal/importer` | reading Android database backups (and upgrading older versions) and OPML files into import decisions |
| `internal/views` | the read API's views, materialized at one head; Android's filters, sort orders and Home sections |
| `internal/stats` | the statistics projection (per podcast, per month) |
| `internal/media` | the browser media relay: probing, descriptors, range-exact relaying, spooling, ffmpeg conversions |
| `internal/api` | the HTTP API: routes, authentication middleware, request bounds, errors, every endpoint |
| `internal/auth` | Argon2id passwords, tokens and sessions, login rate limiting |
| `internal/sealed` | AES-256-GCM sealing of feed credentials, stored responses and staged parts |
| `internal/keys` | the passphrase-protected key bundle |
| `internal/httpserver` | the HTTP server's lifecycle, deadlines and Host check |
| `internal/listen` | where the server may listen (loopback, verified Tailscale addresses), TLS and its reload |
| `internal/backupsched` | the daily backup schedule and retention |
| `internal/service` | installing and updating the launchd agent or systemd unit, and the update transaction |
| `internal/private` | file permissions, ACLs, trusted paths and real directories for everything the server owns |
| `internal/logfile` | the service's private, rotated log |
| `internal/webui` | the embedded web client build (`internal/webui/dist`, installed from NotTennaPod-Web and ignored), or the placeholder page in `internal/webui/placeholder` without one |
| `internal/clock` | the server's single wall clock |
| `internal/version` | the build version, set at link time |
| `internal/acceptancecheck` | the read-only witness check of a stopped server's data directory (`make acceptance-witness`) |
| `internal/rebuildtest` | the rebuild invariant shared by tests |

`Dockerfile`, `docker-compose.yml` and `docker/` hold the container image ([OPERATIONS](OPERATIONS.md#running-in-docker)).

`contract/` holds the shared vectors (`contract/vectors/`, byte-checked copies of which live in the client repositories) and the independent Python reference (`contract/reference/`), written from the protocol and never from the Go code. `spec/conformance/model_checks.py` holds focused executable counterexamples that `make check` also runs.

Schema migrations (`internal/storage/migrations/`) are checksummed into every database they were applied to, so they are never edited; `TestHistoricalMigrationBytes` pins their bytes. The protocol they implement is in [PROTOCOL.md](PROTOCOL.md).

## How a request flows

1. `internal/httpserver` accepts the connection, applies read and write deadlines and refuses unknown `Host` names (`421`).
2. `internal/api` matches the route, bounds and strictly decodes the body, checks the content type and `Origin`, and resolves the bearer token or session cookie to a principal (account, installation, kind, administrator). Handlers reach data only through that principal's account scope.
3. A **push** (`/v1/push`) goes to `internal/protocol`: in one write transaction it re-validates the credential, checks the lineage, and admits each operation in order (slots, parts, envelope and payload checks, dependencies, normalized time). Each admitted fact is applied by its `internal/ops` handler, which records contributions and creates decisions; `internal/projection` refolds every touched scope; the changed records, the accepted entries, the decisions and the consumed slots become one commit appended by `internal/storage` with the next `seq` and digest. The response lists one result per processed operation.
4. A **pull** reads commits from one read snapshot and pages them with MAC-protected tokens; a **snapshot** materializes a durable view file outside the write lock and pages it.
5. **Server-originated work** (feed refreshes, admissions found by the server, re-parses, import publication) fetches and parses outside any transaction, then commits its decisions through the same store in one short write transaction.
6. Every commit notifies `/v1/events` listeners of the account's new head; the web client then re-reads its views from `internal/views`.

All writes are serialized through one writer connection, so `seq` is allocated in commit order and no write transaction does network I/O.

## Test data

Every test input is synthetic, generated by the project, copied unchanged from AntennaPod (GPL-3.0) or produced by AntennaPod's own code.

- `contract/vectors/`: the shared vectors ([CONTRACT.md](CONTRACT.md)). `contract/vectors/feeds/` holds the feed parser corpus and the outputs of Android AntennaPod 3.12.1's own parser for it; its `PROVENANCE.md` is written by `contract/reference/gen_feed_corpus.py` with the corpus and copied with it into the client repositories, so it stays beside the files it describes.
- `internal/importer/androiddb/testdata/`: Android backup fixtures copied from NotTennaPod-iOS `TestSupport/Data/database-restore/` (synthetic test libraries; feeds point at local fixture servers or `example.invalid`):
  - `android-3.12.1/`: libraries made by AntennaPod 3.12.1 on the Android emulator.
  - `older-versions/`: backups generated from the table definitions of older AntennaPod releases (`PodDBAdapter` at the release named in each file name, 0.8 to 3.8.0), and `v3110000-edges.db`, edge values in the 3.12.1 schema.
  - `android-3.12.1-upgraded/`: the same older files after AntennaPod 3.12.1 itself upgraded them (the oracle for the ported upgrader).
  - `invalid/`: files the import must refuse (NotTennaPod-iOS's `invalid/invalid.json` records what Android does with each).
- `internal/importer/testdata/opml/`: OPML documents and `oracle.json`, unchanged copies from NotTennaPod-iOS `Tests/NotTennaPodCoreTests/Fixtures/OPMLFixtures/`, where AntennaPod's unchanged reader with Android's own KXmlParser produced the oracle. Only the documents of the cases the test reads (scope `supported`) and `prefix-before-root.opml` are copied; the oracle also lists the other pending cases, whose documents stay in NotTennaPod-iOS.
- `internal/feed/testdata/discovery/`: the feed discovery pages, unchanged copies of NotTennaPod-iOS `TestSupport/Data/add-podcast-discovery/pages/`; `expected.json` takes their outcomes from that repository's `expected/discovery.json` (its `_comment` says how).
- `internal/feed/testdata/dates_golden.json`: the date parser's golden corpus, generated by this server's parser from a fixed seed (its header says how). NotTennaPod-iOS keeps an unchanged copy as `Tests/NotTennaPodCoreTests/Fixtures/ParityDatesGolden.json`, and NotTennaPod-Web keeps unchanged copies of `contract/vectors/queue_*.json` in `test/vectors/`.
