# NotTennaPod Server: agent instructions

## The goal

The self-hosted server for NotTennaPod. It keeps one person's library in sync across NotTennaPod phones and the web client, serves the web client, refreshes feeds, and handles backups and imports. Phones stay fully usable without it. Out of scope: what [PROTOCOL §1.4](docs/PROTOCOL.md#14-what-is-synced) never syncs (app and playback settings, downloads, local-folder podcasts and other device-only state).

It is one of four NotTennaPod repositories, checked out side by side: NotTennaPod-iOS, the iOS app, whose tests pin this server's commit; NotTennaPod-Web, the browser client, which this server embeds from `internal/webui/dist`; and NotTennaPod-Android, upstream AntennaPod plus test-only exporters that produce the Android reference results in the contract vectors.

[`docs/PROTOCOL.md`](docs/PROTOCOL.md) and the contract vectors ([`contract/`](docs/CONTRACT.md)) define its behaviour. It preserves AntennaPod's meaning (Inbox, played, queue, statistics), and parses feeds as the reference AntennaPod release does. The target is AntennaPod's latest tagged release. The parser, import and views are currently built around AntennaPod 3.12.1; when a new release comes out, compare it with what they were built around and update the code, the contract vectors and the tests. Integration acceptance with the real clients ([PROTOCOL §16](docs/PROTOCOL.md#16-acceptance-scenarios)) is under way; its scenarios are the completion criteria. [ARCHITECTURE](docs/ARCHITECTURE.md) describes the code and [OPERATIONS](docs/OPERATIONS.md) how to run it.

## Rules that matter

- Never lose or change a user's data: not in sync, a restore, an import, an upgrade or a crash.
- The contract vectors are the executable definition. Change the code and the vectors together, and keep `docs/PROTOCOL.md` saying the same thing. The phones carry byte-checked copies of the vectors.
- Accounts are isolated: no request, view or backup ever reaches another account's data. Credentials and secrets never appear in logs, errors or facts.
- Stay compatible with older clients: a client that lacks a newer capability or operation version keeps working ([PROTOCOL §10](docs/PROTOCOL.md#10-capabilities-errors-and-versioning)).
- Migrations are checksummed into existing databases: add a new one, never edit an applied one.

## How to build and test

Go 1.26 or later (the module's `go` line) and Python 3.

```sh
make build    # .work/bin/nottennapod-server
make check    # gofmt, vet, race tests, and the contract checks below
```

The contract checks, which `make check` runs and which can be run alone:

```sh
python3 contract/reference/selftest.py
python3 contract/reference/check_vectors.py contract/vectors
python3 contract/reference/check_rules.py contract/vectors
python3 contract/reference/selftest_rules.py --quick
python3 contract/reference/selftest_import.py
go test -count=1 -timeout 0 -run TestRuleVectors ./internal/ops    # after changing rule vectors
```

## Before every push

Work happens on branches. Before pushing, make sure everything wanted is merged into `main`. Push only `main`: never another branch, `--all`, `--mirror` or tags.

Always run the privacy scan first, before every push without exception, however small. Scan everything that will be pushed (the tracked files, and the metadata of every commit being pushed) for personally identifiable information or anything else that shouldn't go to GitHub:

- personal names, usernames and email addresses; the commit author and committer must be the project identity;
- home directories and absolute local paths, machine and host names, IP addresses, device and Simulator identifiers, and Apple team IDs;
- keys, tokens, passwords, certificates and signing profiles; documented public keys, such as AntennaPod's Podcast Index pair, are the exception;
- time zones and locale: commit dates must be in UTC, and no local time-zone names, offsets or locale may appear in files, fixtures or logs;
- personal data: real podcast libraries, OPML, databases, listening history and screenshots;
- metadata hidden in binary files, such as EXIF or GPS in images and tags in audio;
- internal notes, mentions of AI tools or review process, logs, build outputs, caches and other artifacts.

Fix anything found before pushing.
