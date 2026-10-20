# NotTennaPod sync protocol and server behaviour

This document defines how NotTennaPod clients (iOS, Android and the web client) and NotTennaPod-Server keep one person's podcast library in sync, and what the server does. It is normative: a client or a server that follows it interoperates with the others. Where it says *must*, a client or server that does otherwise is wrong.

The executable definition is the contract in `contract/`: the shared vectors in `contract/vectors/` and the independent Python reference in `contract/reference/`. [CONTRACT.md](CONTRACT.md) documents the vector formats and the rules at vector level. When this document and the vectors disagree, the vectors and the code decide, and this document is corrected.

## Contents

1. Overview and guarantees
2. The AntennaPod reference release
3. Encodings and identity
4. Data model
5. Accounts, authentication and installations
6. The sync protocol
7. Operations and conflict rules
8. Playback, history and statistics
9. Discovery, refresh and parsing
10. Capabilities, errors and versioning
11. Backup, restore and import
12. The read API
13. The media relay
14. Service operation
15. Client obligations
16. Acceptance scenarios

---

## 1. Overview and guarantees

### 1.1 Modes

- **Standalone**: an app without an account, as AntennaPod is. Nothing in this document applies to it.
- **Server mode**: a device signed in to one account on one server. It uses a local library dedicated to that account, separate from its standalone library, and synchronizes it in the background. Switching back to standalone touches neither library.
- **Web client**: an online-only client the server serves, using the same protocol as the phones.

### 1.2 Architecture

- **Every phone is a full replica** of its account's library (catalog and state) and works on it locally. It records its changes as immutable **facts** in a durable local **outbox** and receives other devices' changes from the server's per-account **change log**.
- **The server** stores each account's accepted facts and its own **decisions**, orders them in the change log, derives the account's state from them with the rules of §7 and §8, refreshes feeds, and serves the web client.
- **Feeds are refreshed everywhere.** Phones keep refreshing as in standalone mode; the server refreshes too. Episode identity is derived the same way on every device (§3.3), so discoveries made independently agree.
- **Listening is recorded as sessions** (§8). Position, played state, last played, history and statistics are derived from them.

### 1.3 Guarantees

1. **Standalone is unchanged.** Without a server each app behaves as it does today; server mode is opt-in per device.
2. **No dependency on the server.** In server mode a phone still opens at once, plays, streams, downloads, refreshes, discovers new episodes and records every change, whether or not the server is reachable, for as long as it is unreachable. Sign-in problems, server errors, version mismatches and recovery pause sync only. Sync never seeks, pauses or replaces playing audio, and its work never starves audio or the interface.
3. **Nothing a person did is lost.** Every accepted fact is kept for the life of the account (version 1 compacts nothing). Unsent changes never expire. A restored or replaced server never empties or rolls back a phone. Recovery can restore only facts that survive in a backup or on at least one replica; no screen may claim full recovery when facts were destroyed.
4. **Devices converge.** Once devices have exchanged all changes they show the same subscriptions, episode states, queue, favorites, Inbox, history and statistics. Explicit actions always win over automatic filing.
5. **AntennaPod's meaning is preserved.** Inbox (NEW), played, queue, favorites, history and statistics behave as in the reference AntennaPod release (§2), except for the documented server-mode corrections:
   - auto-download is decoupled from the Inbox (§9.10);
   - a new subscription made while the server is unreachable stays a preview until the server is reachable (§15.2);
   - unsubscribing deletes the podcast's episode state on every device (§7.2);
   - statistics resets stay effective under concurrency (§8.8);
   - where play was pressed last owns the position (§8.1), and the queue merges as one device would have made it (§7.6).

### 1.4 What is synced

Synced: subscriptions and their lifecycle; four account-level podcast policies (the new-episodes action and its account default, keep updated, feed credentials, and the queue's enqueue location and keep-sorted order); listening sessions and the episode state derived from them (position, played, last played, measured duration); the Inbox; the queue; favorites; history; statistics; the catalog's canonical metadata.

Never synced: app and playback settings; every other per-podcast setting (custom title, tags and folders, speed, skip silence, volume adaptation, skip intro and ending, auto-download and its filter, auto-delete, notifications, episode-list filter and sort); downloads and storage; network rules; local-folder podcasts and everything about them; previews nobody listened to; which episode is loaded in the player; the standalone library.

### 1.5 Terms

| Term | Meaning |
| --- | --- |
| account | one person's private library on a server; nothing is shared or compared across accounts |
| installation | one app install's library for one account, or one browser profile; identified by a client-generated UUID |
| author | the installation (or derived branch, §6.10) that created a fact; immutable |
| fact | one immutable operation with a globally unique `op_id` (§6.1) |
| decision | an immutable, signed server choice: an identity allocation, an admission, a resolved queue effect, a fetch's result (§6.1) |
| node | a fact or decision in the account's causal graph |
| receipt | the server's signed record of a fact's first acceptance |
| commit | one entry of the change log, with sequence number `seq` |
| generation | one subscription lifecycle of a podcast; a re-subscription after removal opens a new one |
| record | the published value of one field group of one entity (§4) |
| cut | a history removal or clear, or a statistics reset: it suppresses exactly what it observed |
| capability | an optional feature area a client may skip (§10.1) |

---

## 2. The AntennaPod reference release

NotTennaPod follows the behaviour of **AntennaPod's latest tagged release**. The release the clients, the parser, the shared fixtures and the import are currently built around is **AntennaPod 3.12.1**; it moves forward as new releases come out. Version numbers in the code, tests and test data record what each part was built and checked against.

Where this document says "Android" it means that release's behaviour, as its source and its emulator show it. Parsing reproduces that release's parser exactly (§9.2); the database import reads that release's backup format (§11.5); queue commands, sort orders, completion and statistics reproduce its rules (§7, §8). Its defects are not reproduced where they lose or corrupt data; each such correction is named where it applies.

---

## 3. Encodings and identity

Everything here must give byte-identical results in every implementation (Go, Swift, Kotlin/Java, TypeScript and the Python reference). Vectors: `canonical_json`, `ids`, `guid`, `url`, `length_prefixed`, `content_hash`, `mac`, `digest_chain`, `cursor`, `parts`, `causal_order`, `time_normalization`, `queue_*`.

### 3.1 Canonical JSON

- **RFC 8785 (JCS)**: object members sorted by the UTF-16 code units of their names, no insignificant whitespace, minimal string escaping, UTF-8 output. Hash input is `UTF-8(JCS(value))`.
- **Numbers** are integers within `[-(2^53-1), 2^53-1]`, written without exponent or fraction. Anything that can exceed that range (128-bit position IDs, dyadic numerators, unbounded byte counts) is a string. Floating-point numbers never appear on the wire.
- **Absent optional fields are omitted**, never `null`, unless the schema gives `null` a meaning (for example a history time of `null` means "not in History").
- **Strict decoding** (the sync bodies, §6.13) rejects: invalid UTF-8, duplicate member names, lone surrogate escapes, unescaped control characters, trailing data, a leading byte-order mark, nesting deeper than 64 levels (the outermost container is level 1), and any number with a fraction (including `1.0`), an exponent, `-0`, leading zeros or a value out of range. Noncharacters and top-level scalars are accepted.
- A fact nests at most 32 levels, so commits and pages that embed it stay within 64.

### 3.2 Primitive encodings

| Kind | Encoding |
| --- | --- |
| UUID (account, namespace, installation, op, decision, session, position write, transaction, recovery, log, secret) | lowercase hyphenated 36 characters; other forms are rejected |
| podcast ID | `p1:` + 64 lowercase hex |
| generation ID | `g1:` + 64 lowercase hex |
| episode ID | `e1:` + 64 lowercase hex; or `e0:` + UUID for an item with neither GUID nor enclosure, or a persisted occurrence ID (§9.7) |
| derived author | `a1:` + 64 lowercase hex (§6.10) |
| time instant | integer milliseconds since the Unix epoch, UTC; sub-millisecond precision truncated toward negative infinity |
| duration, position, contribution | integer milliseconds ≥ 0 |
| bytes (hashes, digests, MACs) | lowercase hex |
| base64url (cursors, parts, page tokens) | RFC 4648 §5 alphabet, no padding; `=`, other characters and non-zero unused trailing bits are rejected |
| queue key | `q1:` + decimal `n` + `:` + lowercase hex `m` (§3.6) |
| enumerations | lowercase snake_case |

Display formatting is local; protocol data never uses RFC 3339 strings.

### 3.3 Identity derivations

`L(f1, …, fk)` is, for each field, its UTF-8 bytes preceded by their length as a 4-byte unsigned big-endian integer. Unicode is never normalized.

| Identity | Derivation |
| --- | --- |
| podcast | `P = "p1:" + hex(SHA-256(L("podcast-v1", A, creating_id)))` |
| generation | `G = "g1:" + hex(SHA-256(L("generation-v1", A, P, creating_id)))` |
| episode | `E = "e1:" + hex(SHA-256(L("episode-v1", A, P, K, V)))` |
| derived author | `D = "a1:" + hex(SHA-256(L("author-v1", A, root_op_id)))` |

- `A` is the **account namespace**: a random UUID assigned with the account, never changed, delivered to a device at first attachment, and used only in derivations.
- `creating_id` is the `op_id` of the `podcast.subscribe` or `podcast.register_preview` fact that created the podcast or opened the generation, or the ID of the `import.podcast` decision that did (§11.5). Because podcast IDs are a function of that fact, a restored server that replays the fact recreates exactly the same ID and every episode ID derived from it.
- **Episode `K` and `V`**, in this order (with the evidence of §9.7):
  1. **a lookup alias** the server has published for the item (by GUID if the item has one; by enclosure only if it has none): that episode's ID;
  2. `K = "rss-guid"` (or `"atom-id"` for Atom feeds), `V` = the GUID, if present and not disputed;
  3. for a **disputed** GUID, the release the item belongs to (§9.7); a new release takes `K = "rss-guid+enclosure"`, `V` = GUID + U+001F + normalized enclosure URL;
  4. `K = "enclosure"`, `V` = the normalized enclosure URL, if there is no GUID;
  5. otherwise (nothing to play) a random `e0:` UUID chosen once by the device; such items carry no listening state.
- Derivations are versioned and frozen; a change is a new version with a new prefix, and existing IDs stay valid forever.
- Queue entries need no ID: an episode is queued at most once. Session, proposal, secret and transition IDs are random UUIDs.

### 3.4 GUIDs

As Android's `GuidValidator` and Java's `String.trim()`: XML decoding happens in the parser; then leading and trailing code points ≤ U+0020 are removed (not other Unicode spaces). The GUID is **absent** if the result is empty or exactly `null`. Case and inner whitespace are kept.

### 3.5 URL normalization (version 1, frozen)

A string algorithm, not a URL parser, so that every language agrees:

1. Without a `scheme:` prefix matching `^[A-Za-z][A-Za-z0-9+.-]*:` the string is used unchanged.
2. Lowercase the scheme (ASCII).
3. If the rest starts with `//`, the authority runs to the first `/`, `?` or `#`. Userinfo (up to and including the **last** `@`) is kept unchanged. Of the host-and-port:
   - bracketed (`[`): the host runs to the first `]`; a following `:` starts the port; anything else after `]` makes the whole a host without a port; no `]` makes the whole the host;
   - not bracketed: exactly one `:` separates host and port; none, or more than one, makes the whole the host;
   - ASCII letters in the host are lowercased, except inside a percent triplet;
   - the port is removed with its `:` when empty, `80` with `http`, or `443` with `https`; any other port is kept as written.
4. Remove the fragment (from the first `#` after the authority, or the first `#` of the rest when there is no authority).
5. Keep everything else exactly: path, percent-escapes and their case, query and its order, trailing slashes, dot segments.

`http://` and `https://` differ. Tracking or token parameters are never stripped. The server stores the raw URL a client sent and compares normalized forms. Normalization is idempotent.

### 3.6 Queue position keys

A key is the reduced dyadic rational `m / 2^n` with `m` odd and `0 < m/2^n < 1`, written `q1:<n>:<m hex>`. Decoders reject even `m`, leading zeros, uppercase hex, values outside (0, 1) and `n` above 1,048,576. Keys compare numerically (`m1·2^n2` against `m2·2^n1`, arbitrary precision), never bytewise.

**Allocation** of a key strictly between `L` and `R` (keys, or the sentinels 0 and 1), for a persisted position-write ID `U` (the 128 bits of a random UUID, big-endian):

1. choose the smallest `n ≥ 1` with `(R − L)·2^n > 2^130`;
2. let `r = 2U + 1`; choose the smallest `m > L·2^n` with `m ≡ r (mod 2^129)`.

Then `m < R·2^n`, `m` is odd, and distinct `U` never give equal keys. A key must carry its position ID's residue; a retry reuses both. Keys are a materialization of the queue's order (§7.6), never an input to it.

### 3.7 Hashes, MACs, digests and cursors

- **Fact content hash**: `hex(SHA-256(UTF-8(JCS(fact))))` over the fact as received.
- **Receipt and decision MAC**: `hex(HMAC-SHA256(key[key_id], UTF-8(JCS(object without mac, plus member account_id))))`. `account_id` binds the MAC to the account and is not sent.
- **Commit digest chain**: `d(0)` = 32 zero bytes; `d(seq) = SHA-256(d(prev) ‖ UTF-8(JCS(commit)))`. The first commit of a new epoch chains from the restored head's digest.
- **Snapshot digest**: `s(0)` = 32 zero bytes; `s(i) = SHA-256(s(i−1) ‖ UTF-8(JCS(item_i)))` over whole items.
- **Cursor**: base64url of `UTF-8(JCS({log_id, epoch, seq, digest}))`, exactly those members (`seq` 0 is an empty log). Clients treat it as opaque and compare it only for equality.
- **Page tokens** are opaque to clients, which send them back unchanged. Pull and snapshot tokens are signed: base64url of canonical JSON (with the signing `key_id`), `.`, and the base64url HMAC-SHA256 under that receipt key of the domain `"ntp-page-token-v1\0"` followed by the JSON; they are useless in another account or after the lineage changes. Read-list tokens (§12) are not signed: they name a view of the server's in-memory registry and an offset, and are honoured only for that view's account and query, while its incarnation, lineage and generation are current: another account's or an expired view is `410 view_expired`, another query `400 invalid_request`.

---

## 4. Data model

Every synced entity has **field groups**: the unit of a record, of revision and of conflict resolution. The server publishes each changed field group as a **record** `{entity, id, field_group, value}` or `{entity, id, field_group, tombstone: true}` with its `provenance` (the sorted IDs of the nodes that determine it). A record exists once a contribution touches its group, with the folded value even when that equals the default. Closing a generation tombstones every episode record of that generation. Clients ignore groups they do not implement but keep their source facts.

### 4.1 Account

| Group | Value | Capability |
| --- | --- | --- |
| `discovery_policy` | `{new_episodes_action}`: `inbox`, `queue` or `nothing` (default `inbox`) | core |
| `queue_policy` | `{enqueue_location, keep_sorted, sort_order}` (defaults `back`, off, `date_new_old`) | queue |

History cuts and statistics cuts are facts, not account records; their effects appear in episode records.

### 4.2 Podcast

| Group | Value | Written by |
| --- | --- | --- |
| `identity` | `{aliases: [{url, normalized}], refresh_url}` | server |
| `identity_rules` | `{disputed_guids: [{guid, releases: [{episode_id, publication_time, enclosures}]}], disputed_enclosures, aliases: [{kind, value, episode_id}]}`: only what a device cannot derive (§9.7) | server |
| `subscription` | `{state, generation_id}`; `state` ∈ `not_subscribed`, `subscribed`, `archived`, `removed`, `merged` (with `merged_into`) | facts |
| `policy` | `{new_episodes_action, keep_updated}`; action ∈ `global`, `inbox`, `queue`, `nothing` | facts |
| `credentials` | `{secret_id}` (§5.8) | facts |
| `metadata` | `{revision, title, description, author, language, link, image_url, type, funding, paged, next_page_url, fetch_time}` | observations (§9.4) |
| `refresh_status` | `{last_attempt, last_success, error: {label, code, detail} or null, next_attempt}` | server |

### 4.3 Episode

| Group | Value | Capability |
| --- | --- | --- |
| `identity` | `{podcast_id, occurrence, merged_into?}`: the first evidence the episode was registered with | core |
| `metadata` | `{revision, title, description, link, pub_date, image_url, payment_link, media: {url, size, mime_type, duration}, chapters, chapter_url, transcript_url, transcript_type, social_interact_url, fetch_time}` | core |
| `admission` | `{generation_id, admission_id, proposal_id, source, action}` | core |
| `play_state` | `{generation_id, play_state}`; `new`, `unplayed` or `played` | core (`new` is shown only with `inbox`) |
| `position` | `{generation_id, position, claim_id}`: the claim that set it, so a change of owner is visible when the number is unchanged | core |
| `last_played` | `{generation_id, time}` | core |
| `measured_duration` | `{generation_id, duration_ms, installation_id}` | core |
| `favorite` | `{generation_id, on}` | favorites |
| `history` | `{generation_id, time}`; `null` when every entry is cut | history |
| `played_time` | `{generation_id, played_ms}` | statistics |
| `statistics_units` | imported rows that became one episode but keep their own statistics (§11.5) | statistics |
| `queue_membership` | `{generation_id, in_queue, reason?}` | queue |
| `queue_position` | `{generation_id, key, position_id}` | queue |

Defaults: play state `unplayed`, position 0 without a claim, favorite off. Catalog metadata is not per generation and stays after an unsubscribe; every other episode group is per generation.

An episode a device knows only from a fact (a listening placeholder) exists locally with the minimal metadata the fact carried until its catalog record arrives. Placeholders are never dropped.

### 4.4 Device-only state

Never sent: downloads, files and the download log; the discovery ledger (§9.10); notifications shown; local-folder podcasts (their IDs live in a device-local namespace); unregistered previews; device and per-podcast settings; the five-second save journal (§8.2) until exported; the loaded episode, the active session's pending-finalization reference, export watermarks and the physical-installation nonce (§5.3).

---

## 5. Accounts, authentication and installations

The server is for a household on a private network (Tailscale): sound security, sized for a home.

### 5.1 Accounts and administration

- The first account is created by **setup**: `POST /v1/setup {username, password, setup_code}` in one write transaction that checks no administrator exists (others get `409 setup_complete`). While no administrator exists the server generates a 10-character setup code, writes it to `setup-code` in the data directory (mode 0600) and prints it to the terminal (in the container image, the container's console, `docker logs`, counts as the terminal, not as a log); the file is deleted once setup completes. The code is required even from loopback, because a local HTTPS proxy makes every request look local.
- Administrators (an administrator's web session, `/v1/admin/accounts`) list, create, rename, disable and enable accounts, grant and revoke administration, reset passwords and delete accounts (the request repeats the username). The last active administrator cannot be disabled, demoted or deleted.
- **Disable**: sign-in is refused and every credential of the account answers `403 forbidden`; enabling restores them.
- **Password change** (`POST /v1/account/password {current, new}`) and **reset** revoke every device token and web session of the account, except the session making a self-change.
- **Delete** removes the account and all its data by cascade. Devices keep their local server library until the person removes it.
- Every query and record is scoped to the authenticated account; IDs in URLs or bodies are never trusted for authorization.

### 5.2 Secrets at rest

| Secret | Form | Stored as |
| --- | --- | --- |
| username | 1–64 of `A–Z a–z 0–9 . _ -`, unique case-insensitively | as entered, plus a lowercase key |
| password | 8–1024 bytes of UTF-8 | Argon2id PHC string (m = 64 MiB, t = 3, p = 4, 16-byte salt, 32-byte hash); parameters read from the string; at most two verifications at once |
| device token | `ntp1_` + base64url of 32 random bytes; `Authorization: Bearer` | SHA-256 of the token; never expires |
| web session | the same form, in a cookie of its own named `ntp_s_` + the first 16 hex digits of its SHA-256 (HTTP-only, `SameSite=Strict`, `Path=/`, `Secure` on an HTTPS origin); sessions issued by earlier servers are in `ntp_session` | SHA-256; expires after 30 days unused, extended (and its cookie re-sent under its own name) at most once an hour; an expired session is deleted when it is next presented, never extended |
| feed credentials | secrets (§5.8) | AES-256-GCM under `secrets.key` in the data directory |

Secrets never appear in logs, URLs, error messages, facts, commits or records. Every response is JSON with `Cache-Control: no-store`.

### 5.3 Device sign-in and installations

`POST /v1/auth/device {username, password, install_id, instance_nonce, device: {name, platform, app_version, capabilities}}` → `{token, account: {account_id, namespace, username}, install_id}`.

- The password is verified outside any transaction; the token is issued in one write transaction only if the login's credentials version is unchanged and the account active, so a sign-in that verified an old password never completes after a reset.
- **Instance nonce**: a random value the physical installation keeps where backups and migrations cannot copy it (iOS: a `ThisDeviceOnly` Keychain item; Android: no-backup storage; a browser: beside its installation ID). It tells a copied library from the device that created it.
- An unknown `install_id` is registered. A known one with the **same** nonce is **resumed**: revocation cleared, the new token replaces the installation's earlier tokens, unsent operations stay valid. A known one with a **different** nonce answers `409 installation_conflict` and changes nothing; the client takes a fresh installation ID and signs in with it in the same step.
- `capabilities` lists the optional capabilities (§10.1); unknown names are `400 invalid_request`. `PUT /v1/installations/self {name?, app_version?, capabilities?}` updates the caller (an app update declares a new capability here before requesting its snapshot, §6.8).
- **Successor installation** without a password: `POST /v1/installations/successor {install_id, instance_nonce, device}` with a device token or a bound web session, idempotent by the new `install_id` (the client persists it first). It issues a new credential of the caller's kind bound to the successor (for a web session, in a cookie of its own, §5.4). With the **same** nonce the old installation is marked superseded: its credentials may only repeat the request or sign out, other requests get `409 installation_superseded`, and they are revoked when the successor's credential is first used. With a **different** nonce (a copy) the original is untouched.
- `GET /v1/installations` lists the account's installations; `POST /v1/installations/{id}/revoke` revokes one (its row stays: the server never deletes data because a device was removed); `POST /v1/auth/device/sign-out` deletes the caller's token. `last_seen_at` is updated at most once a minute.
- A token that stops working only pauses sync ("Sign in again"); the library and outbox stay and are sent after signing in again.

### 5.4 Web sign-in and request protection

- `POST /v1/auth/web {username, password}` sets the session cookie. The browser profile then registers its installation with `POST /v1/installations {install_id, instance_nonce, name, platform: "web", app_version, capabilities}`, which binds the session (idempotent; same rules as §5.3; an installation superseded by a same-nonce successor that is not yet used, or has no web session left, may still be bound, as a superseded credential; in the second case the successor's next use revokes it again, as a first use does). `POST /v1/auth/web/sign-out` deletes the session in use and the web sessions of every installation on its same-nonce supersession path (its successors, used or not; never device tokens, never a copy), and clears every session cookie the request carries. `GET /v1/session` describes it.
- **A browser may hold several session cookies.** Each name only ever holds one session, so an answer that arrives late can bring back an old session but never replace a newer one. The server considers every session cookie the request carries: of the usable ones (only those of the account `X-NotTennaPod-Account` names when it is present; none of it but another usable: `409 account_changed`; a superseded session counts, §5.3), it drops each one whose installation has a successor or copy (following `created_from`) bound to another usable one, and uses the one issued last of the rest (latest issue time, then larger hash). A cookie is cleared only once its session is known gone for good: absent, or deleted in the same write transaction (an expired one is deleted there unless a renewal won). Web sign-in clears the session cookies the request carries. Known limitation: in a copied browser profile (§6.10), a renewal answer for the original's session that arrives after the copy signs out can bring that session back into the copy, whose conflict cleanup then signs the original out.
- Every POST, PUT and DELETE requires `Content-Type: application/json`. A request with an `Origin` header must match the browser-facing origin exactly. Web sign-in, setup and every cookie-authenticated state change must carry a matching `Origin`. Bearer requests carry none. GET never changes state.
- The browser-facing origin is `--public-url` when given; otherwise `https` for a TLS connection or a loopback peer sending `X-Forwarded-Proto: https`, else `http`, with the `Host` header's host and port.
- Every app request from the web client carries `X-NotTennaPod-Account`; a credential of another account answers `409 account_changed`.

### 5.5 Rate limits

Failed sign-ins (device, web and setup code) are counted per normalized username and, for non-loopback peers, per IP. After 5 failures within 15 minutes, attempts are refused for `2^(failures−5)` seconds, at most 15 minutes, with `429 rate_limited` and `retry_after`. Refused attempts are not verified and do not count. A success resets the counters. An unknown username costs the same Argon2id work as a known one and gets the same answer. Excess verifications get `503 server_busy`. Counters are in memory.

### 5.6 Host names

The server answers only these `Host` names, before routing or authentication: loopback names and addresses (any port); the address the connection arrived at and the listen host name; this machine's Tailscale addresses and MagicDNS name, from the verified `tailscale` command; `--public-url`'s host; and names given with `--allow-host` (repeatable). Ports are not compared. Any other host gets `421` with a message naming `--allow-host`.

### 5.7 Authorization

| Endpoint | Credential |
| --- | --- |
| `GET /v1/info`, `GET /healthz` | none |
| `POST /v1/setup` | setup code; matching `Origin` |
| `POST /v1/auth/device` | password |
| `POST /v1/auth/web` | password; matching `Origin` |
| `POST /v1/auth/device/sign-out`, `PUT /v1/installations/self` | device token |
| `POST /v1/installations` | web session (binds it once) |
| `POST /v1/installations/successor` | device token, or a bound web session; also a superseded installation's credential |
| `POST /v1/auth/web/sign-out`, `GET /v1/session` | web session |
| `GET /v1/installations`, `POST /v1/installations/{id}/revoke`, `POST /v1/account/password` | device token or web session |
| sync: `/v1/push`, `/v1/pull`, `/v1/snapshot`, `/v1/secrets/{id}`, `/v1/refresh`, `/v1/events`, `/v1/sync/recovery/*` | device token, or a web session bound to a web installation; the envelope's `install_id` must be the credential's |
| read API, media, import | web session bound to a web installation |
| `GET /v1/diagnostics` | web session, bound or not; `?account=` naming another account needs an administrator's |
| `/v1/admin/*` | an administrator's web session |

Missing, unknown, revoked or expired credentials answer `401 unauthorized`; a disabled account or a non-administrator on an admin endpoint `403 forbidden`. Any handler that issues or rebinds a credential, and every read that serves account data, re-validates the credential, its installation and the account inside its own transaction, so a revocation, reset, demotion or disable that lands during a request always wins.

### 5.8 Feed credentials as secrets

Feed credentials never appear in facts, commits or records. A push (or recovery upload) may carry `secrets: [{secret_id, username, password, approved_origins}]` beside its operations; `secret_id` is a client-chosen UUID. A request carries at most 500 secrets (`413 payload_too_large`); a username and a password are at most 4,096 UTF-8 bytes each, and `approved_origins` lists at most 16 origins (`400 invalid_request`). The server seals each secret (AES-256-GCM, additional data = account and secret ID) before admitting the operations, idempotently by ID (another value under a known ID is `invalid_request`). `podcast.subscribe` and `podcast.set_credentials` reference `secret_id`; a referenced secret the server does not hold makes the fact wait with `dependency_missing` naming it. Installations fetch values with `GET /v1/secrets/{secret_id}`.

`approved_origins` (normalized `scheme://host[:port]`) is part of the secret: credentials are sent only to those origins (§9.3). Any change of the set is a new secret written through the `credentials` register. Automatic moves never change it. Framed parts are sealed at rest too, so refused content never reaches the database in clear.

---

## 6. The sync protocol

Every request carries `NotTennaPod-Protocol: 1`. `GET /v1/info` answers `{server_id, version, protocol: {min, max}, setup_required, bounds, instance_id}`.

A JSON response of at least 1 KiB is gzip-compressed (`Content-Encoding: gzip`) when the request's `Accept-Encoding` allows gzip (RFC 9110: `gzip;q=0` refuses it, `*` covers it); otherwise, and without the header, it is sent as is. The API's JSON responses carry `Vary: Accept-Encoding`. The event stream and media are never compressed. Canonical bytes, digests and bounds apply to the decoded body, which is the same either way. Request bodies are sent uncompressed.

### 6.1 Facts, decisions and receipts

**Fact** (immutable; hashed):

| Field | Type | Notes |
| --- | --- | --- |
| `op_id` | UUID | account-scoped; a retry reuses it |
| `author_id` | UUID | immutable source installation |
| `author_seq` | integer ≥ 1 | strictly increasing per author |
| `previous_op_id` | UUID | the author's previous fact; omitted for its first |
| `dependencies` | node IDs | sorted, unique: the causal frontier the action observed; must include `previous_op_id` |
| `type`, `type_version` | string, integer ≥ 1 | the operation vocabulary only grows |
| `client_time` | integer ms | the action's time; never the causal order |
| `targets` | `[{entity, id, generation?}]` | sorted by `(entity, id, generation)`: every affected podcast and episode, with its generation |
| `payload` | object | schema per type and version; unknown members are rejected |

**Validation has two levels.** A fact that is not a strictly decodable object, or whose `op_id` is missing or not a UUID, is **unarchivable**: the request is `400 invalid_request` and nothing is consumed. Any other envelope or payload fault (missing or extra fields, wrong forms, `author_seq` or `type_version` below 1, `previous_op_id` as `null` or equal to `op_id`, unsorted or duplicate `dependencies` or `targets`, a self-dependency) is **rejected**: archived with a receipt and a reason, consuming its slot.

**No credentials in any string.** Before archiving, the server checks every string and object key of a received fact, whatever its validity, for a URL authority with userinfo: a string that is one URL (starts with `scheme://` and holds no whitespace or control characters) is refused if an `@` precedes its first `/`, `?` or `#`; in other text an embedded `scheme://` URL's authority also ends at whitespace, control characters, `<`, `>` and the quotation mark that opened it. A refused fact makes the request `invalid_request` (`invalid_part` for a framed fact), and nothing is archived.

**Receipt** (immutable; signed): `{op_id, content_hash, outcome, reason?, normalized_time, epoch, seq, index, frontier?, sort_values?, key_id, mac}`.

- `outcome` ∈ `applied`, `superseded`, `not_applied`, `rejected` is the outcome at first acceptance; `reason` (snake_case) accompanies `not_applied` and `rejected`. Later arrivals change projections, never receipts.
- `(epoch, seq, index)` is the commit revision and the position in the commit: a total order matching admission order.
- A **rejected** fact is archived but is not a causal node. Its receipt's `normalized_time` is the receive time and it carries `frontier`: the sorted causal-node IDs it observed, resolved through other rejected facts (empty for an envelope rejection). A dependency on a rejected fact stands for its frontier.
- `sort_values` are the canonical sort values (§7.6) the server recorded when it admitted a queue placement, a move or a sort command: `[{episode_id, generation_id, values: {title, podcast_id, podcast_title, publication_time, duration}}…]`, sorted by episode ID, then generation ID. A placement (add, restore, play bundle) or a move (move, run move) records its members that pass their checks, including those of an add that places nothing because they are already queued; an add, restore or bundle under keep-sorted at its place also records every member then in the queue; a sort command records the queue then. It is present only when it lists at least one member and never travels in a fact. Every client keeps a receipt exactly as received, members it does not use included: the MAC covers all of them, and a recovery upload sends the receipt back whole (§6.11).

**Decision** (immutable; signed; a causal node): `{decision_id, decision_type, decision_version, dependencies, normalized_time, epoch, seq, index, subject, inputs, result, key_id, mac}`.

- `decision_id` is a random UUID (or a UUIDv5 for import decisions); op IDs and decision IDs share one ID space.
- `normalized_time = max(server_now, max(normalized_time of dependencies))`, except for `podcast.resolution`, which takes its registration's normalized time (its only dependency): what a device recorded while its registration waited keeps the time the person acted (§6.9, §15.2). A decision's normalized time is stored with it and never recomputed.
- The **rank** is `(epoch, seq, decision_id)`. Decisions with equal `(decision_type, subject)` compete: the smallest rank is effective, the others stay as evidence (§6.11). `queue.layout` is the exception: layouts never compete, every one is effective and kept as signed evidence, and a member's published key is its latest key write (§7.6).

| Decision type | Subject | Decides |
| --- | --- | --- |
| `podcast.resolution` | the fact's `op_id` | the podcast and generation a subscribe or register-preview acts on (§7.2) |
| `podcast.alias` | `alias:` + normalized URL | which podcast owns a feed URL |
| `podcast.moved` | `moved:<G>:<refresh id>` | a permanent move of a generation's refresh URL (§9.6) |
| `podcast.merge` | the source podcast | a podcast merge (§9.7) |
| `episode.guid_change` | podcast and new GUID | a GUID-change match (§9.7) |
| `episode.merge` | the source episode | an episode merge (§9.7) |
| `episode.occurrence_mapping` | podcast, GUID and release | a reused GUID's release groups (§9.7) |
| `catalog.refresh` | `refresh:<P>:<refresh id>:<part>` | one server fetch's outcome and changed records (§9.6) |
| `discovery.admission` | `admission:<G>:<E>` | the one filing of an episode in a generation (§9.5) |
| `queue.resolved_add` | the start's `op_id` | a queue add the server resolved for a client without the queue capability (§7.6) |
| `queue.layout` | the account's queue | queue keys that make the published order the replay's (§7.6) |
| `import.manifest`, `import.podcast`, `import.episode` | the import | a library import (§11.5) |

### 6.2 Commits and the change log

One log per account. A commit is `{epoch, seq, accepted: [{fact, receipt}], decisions, slots, records}`:

- `accepted` sorted by `op_id`, `decisions` by `decision_id`, `slots` by `(install_id, install_seq)`, `records` by `(entity, id, field_group)`;
- `accepted` and `decisions` hold entries first committed here (a recovered entry is re-committed unchanged, with its original epoch and seq);
- `slots` lists every transport slot consumed: `{install_id, install_seq, op_id, content_hash, result}` with `result` ∈ `accepted`, `duplicate`, `op_id_conflict`;
- each record appears once per commit with its final value and the union of provenance.

Writes are serialized server-wide; a commit and all its effects are one transaction, so `seq` is contiguous and log order equals commit order. Commits are stored as canonical bytes and served byte for byte.

### 6.3 Lineage and epochs

A push, pull or capability snapshot names the client's `cursor` and transport position `{install_id, acked_install_seq}`. Before consuming or serving anything the server checks:

- an unknown `log_id` → `409 lineage_unknown` (the account's log is not the one the device knew);
- a different epoch, a digest that does not match the server's at that `seq`, a `seq` above the head, or `acked_install_seq` above the installation's consumed watermark → `409 lineage_mismatch` (the server lost acknowledged work: the client runs recovery, §6.11).

A restore moves every account to a new epoch (§11.3). A normal restart keeps the lineage.

### 6.4 Push

Request: `{cursor, install_id, acked_install_seq, operations: [{install_seq, fact} | {install_seq, part}], secrets?}`, with `install_seq` contiguous and ascending. Each envelope's `install_seq` is assigned when it is committed to the outbox. Response: `{head, install_watermark, results: [{install_seq, status, receipt?, missing?, parts_present?}]}`.

One push is one write transaction and at most one commit. The credential and installation are re-validated inside it (§5.7); then the lineage (§6.3); then each operation in order, with `next` = the installation's consumed watermark + 1:

| Situation | Result | Consumed |
| --- | --- | --- |
| `install_seq < next`, same content as recorded | the slot's original result (`duplicate` with the original receipt, or `op_id_conflict`) | no (already) |
| `install_seq < next`, different content | `installation_conflict`; stop | no |
| `install_seq > next` | `gap`; stop | no |
| a part, assembly incomplete | `part_stored` with `parts_present`; stop | no |
| a part of another framing transaction, or failed reassembly | `installation_conflict` / `invalid_part`; stop | no |
| known `op_id`, same content | `duplicate` with the original receipt | yes (a new slot) |
| known `op_id` (or a decision ID), different content | `op_id_conflict`, no receipt | yes |
| envelope fault | `accepted`, receipt `rejected` | yes |
| unknown type or version | `version_unsupported`; stop | no |
| a dependency, `previous_op_id` or referenced secret neither a node nor an archived rejected fact | `dependency_missing` with `missing`; stop | no |
| impossible author history, capability not declared, invalid payload | `accepted`, receipt `rejected` | yes |
| otherwise | `accepted`, applied by its type's handler | yes |

- Processing stops at the first result that consumes nothing; later slots wait. Local use and pulling continue.
- A transient failure rolls everything back and answers `503 server_busy`; the client resends the same slots.
- **Author history**: a fact whose `previous_op_id` is an admitted fact of another author or with a higher `author_seq` is rejected (`impossible_author_history`). A fact whose author position is already held is not refused: it roots a derived branch (§6.10).
- **Capabilities**: a newly authored fact outside the installation's current capabilities is rejected (`capability_not_declared`).
- `op_id_conflict` is a client fault shown in sync diagnostics; consuming it keeps one bad envelope from blocking the outbox.
- An operation is retired from the outbox only after its receipt and state are included in a locally committed pull (`install_watermark` at that pull's head).

### 6.5 Framed parts

A fact larger than the unframed bound is sent as parts of one slot: `part = {transaction_id, part_index, part_count, content_hash, data}`, where `data` is base64url of a non-empty slice of `UTF-8(JCS(fact))`, slices in index order concatenate to the whole, and `content_hash` is the fact's. Parts are stored durably (sealed) and are idempotent by index; the slot is consumed only when every part is present and the bytes are the canonical JSON of an object with that hash. A slot's assembly belongs to its first framing transaction until the slot is consumed. A library persists a fact's framing transaction ID with its outbox entry and never reframes a slot. Abandoned assemblies stay, within the bounds, until cleanup.

### 6.6 Pull

Request: `{cursor, install_id, acked_install_seq, page?, limit?}`. The first request (no `page`) runs the lineage checks and fixes the head `H`. Response page: `{head, from, has_more, next?, install_watermark, items}`, where items are `{commit}` or, for a commit larger than the page bound, `{commit_part: {seq, part_index, part_count, data}}`.

- Paging is position-based and retry-safe: sending the same `page` token returns the same logical page; the client advances only with the newly returned `next`. Commits up to `H` are immutable, so pull views keep no server state.
- `install_watermark` is the installation's consumed watermark as of `H`.
- The client stages every page and applies nothing until the last page arrives and every commit verifies against the digest chain from `from` to `head`.

### 6.7 Snapshot

Request: `{mode, cursor?, recovery_id?, field_groups?, install_id, acked_install_seq, page?, limit?}`:

- `attach`: first attachment; no cursor.
- `upgrade`: a capability upgrade on the same lineage, restricted to `field_groups`; the cursor is required and lineage-checked.
- `recovery`: after a restore (§6.11).

Response page: `{head, has_more, next?, install_watermark, items, manifest?}`. Items are `{accepted: {fact, receipt}}`, `{decision}`, `{record}`, or `{item_part: {index, part_index, part_count, data}}`. Order: admitted facts and decisions in causal order (§6.9), then rejected entries by their receipts' `(epoch, seq, op_id)`, then records by `(entity, id, field_group)`. The last page carries `manifest = {item_count, snapshot_digest}`, which the client verifies before switching.

A snapshot is a durable view materialized at `H` from one read transaction, outside the write lock. A view unused for 24 hours, or whose log or epoch is no longer current, is deleted; its page token then answers `410 view_expired`, and the client restarts the snapshot from the beginning, applying nothing staged.

### 6.8 Applying pulls and snapshots on a device

- Build the next replica in staging, in bounded chunks, without holding the live database's write lock or the audio thread, then switch the base and cursor atomically. Only bounded cutovers take the live write lock; if a cutover would exceed the local latency bound, apply smaller complete commits.
- Local facts made while staging stay in a durable overlay and are replayed over the new base at the switch. No snapshot replaces the live outbox.
- What a person sees is the accepted base plus unacknowledged local facts and pending registrations (masking). Applying records creates no facts.
- Fetch every secret referenced by staged records before activating a snapshot or advancing the cursor; a failed fetch blocks activation and is retried.
- Before applying a pull that would remove more than 75% of the queue's entries, keep a local copy of the queue for diagnostics for at least 30 days.
- A capability upgrade stages the missing field groups on the same lineage and keeps concurrent local edits.

### 6.9 Causal order and normalized time

**Topological order** of a set of nodes closed under dependencies: Kahn's algorithm with the ready set ordered by `(normalized_time, node ID bytewise)`; repeatedly emit the smallest ready node. A missing dependency is `dependency_missing`; a cycle is invalid. Facts and decisions are ordered together; ranks play no part. A pairwise comparator is never a substitute (it need not be transitive). The server maintains this order incrementally: a new node goes immediately before the first node after its last dependency whose priority is greater than its own.

**Normalized time** at first acceptance: `lower` = the maximum normalized time of the resolved dependencies (−∞ if none); `normalized = min(max(client_time, lower), receive_time)`, and `lower` if `receive_time < lower`. It is stored in the receipt and never recomputed. Causality always wins; among concurrent facts the earlier normalized time, then the smaller ID, comes first. A bad clock can distort dates and genuine concurrent tie-breaks, never beat a fact that observed it. A fact that depends on a registration's resolution is bounded below by the registration's time, not by when the server resolved it (§6.1).

### 6.10 Copied installations

A copy (a restored phone backup, a device migration) duplicates a library: its outbox, its author and transport identities, its archive and its player state. Three things can then diverge.

- **Transport.** A library that meets another's slot (`installation_conflict`, or an `installation_superseded` it did not cause) takes a fresh nonce and a new transport installation `T'`, renumbers its outstanding envelopes onto `T'` (fact IDs, authors, order, dependencies and framing unchanged), registers `T'` with a successor request (or signs in), and keeps its author.
- **Author history.** Every archived fact with well-formed coordinates holds a position in receipt order. A fact inherits the effective author of its `previous_op_id` when that is an archived fact of the same `author_id`, otherwise its `author_id`. The first fact at a position holds it; a later, different fact at that position roots a **derived branch** whose effective author is `D(X)` (§3.3); facts chained to it inherit it. Rejected facts hold their position. Nothing is refused or remapped, and recovery recomputes the same branches from the same receipts.
- **Local detection** (the normal case): at launch, before writing, a library whose stored nonce differs from the device's takes the device's nonce, a new transport `T'` and a new author `F = T'`; writes `installation.fork {author, head}` as `F`'s first fact (`head` is the library's own writing tip; no fork when it never wrote as `author`), depending on the copied frontier; exports any unexported save of the copied open session as a passive checkpoint authored by `F`; seals that session locally without a fact; and keeps the copied player state and pending finalization. New listening starts new sessions. Admission: `author` ≠ `F`, `F` has no earlier fact, and `head` is a fact of `author` among the dependencies; otherwise `rejected` / `invalid_payload`.
- **Sessions** follow the save tree (§8.5).

### 6.11 Recovery after a restore

After a restore every device answers `lineage_mismatch` once and recovers. Recovery is a union of what the restored server lacks, independent of upload order; uploads consume no transport slots.

| Step | Request | Notes |
| --- | --- | --- |
| start | persist a `recovery_id`; `POST /v1/installations/successor` with the same nonce, once | resumptions reuse both; the local library and player stay usable throughout |
| snapshot | `POST /v1/snapshot {mode: "recovery", recovery_id, install_id, acked_install_seq, cursor?}` | `{prefix: true, cursor, head, install_watermark}` when the restored log still holds the device's cursor commit (catch up by pulls; inventory only receipted pushes not yet pulled); otherwise a full paged snapshot |
| inventory | `POST /v1/sync/recovery/inventory {recovery_id, entries}` | entries `{kind: "fact", op_id, content_hash, position: [epoch, seq, index]}` or `{kind: "decision", decision_id, rank: [epoch, seq]}`, at most 2,000 per request and within `max_request_bytes`; each answered `have`, `need` or `conflict`. Earlier servers take inventory bodies of at most 64 KiB, so a client that may meet one (a phone) keeps each inventory request within 64 KiB and sends smaller batches after a `413`. The web client is served by the server it syncs with, so it meets no earlier server and is exempt |
| upload | `POST /v1/sync/recovery/upload {recovery_id, entries, secrets?}` | entries `{fact, receipt}`, `{decision}` or `{part, receipt}`, in causal order, a fact's decisions right after it, at most 500 per request (`413 payload_too_large`) and within `max_request_bytes`; each answered `joined`, `known`, `replaced`, `dependency_missing`, `invalid_receipt`, `invalid`, `conflict` or `part_stored` |
| catch up | pull through every commit from the snapshot head; switch the base; push the remaining outbox under the successor | an outbox operation that already holds an old-lineage receipt is uploaded, not pushed |
| finish | `POST /v1/sync/recovery/finish {recovery_id, complete}` | `complete: false` when anything was `invalid_receipt` or `conflict`, or facts are known lost |

Rules:

- Every receipt and decision MAC is verified; a failure is `invalid_receipt`, never joined, and listed.
- A joined entry is archived at its original position and its effects derived there. Normalized times are never reclamped. A recovered removal closes its generation from its position on; removed generations stay removed.
- **Original receipts replace provisional ones.** A fact re-pushed after the restore got a later receipt; when its verified earlier original arrives, it replaces the provisional receipt (kept as superseded, so a later upload of it is `have`), the fact is re-ranked and re-applied, and it is re-committed so that clients replace the receipt they hold.
- **Competing decisions.** For resolution decisions (`queue.resolved_add`, `discovery.admission`, `catalog.refresh`, `podcast.moved`) only the effective one (smallest rank) contributes. Identity decisions compete by the evidence they decide (the normalized URL, the fact, the source podcast, the source episode, podcast and GUID, a release) and never split anything: when a recovered earlier decision differs from a provisional one, the earlier becomes effective and the provisional one's creations are merged into it by a new merge decision with `reason: "recovered"`.
- Recovered facts create no decisions at their own acceptance. At the end of each upload request the server resolves any subscribe still without an effective resolution, and publishes the queue (§7.6). A lost admission is not re-decided; the server's own discovery files what is still eligible.
- **Lost-timeline evidence.** A verified receipt or decision at an epoch at or above the account's current one, for an entry the archive does not hold, proves an undetected rollback: in the same transaction the server runs the restore transition for the whole server (§11.3) before joining it.
- Every result is idempotent by ID; recovery and pushes answer `server_busy` with `retry_after` while an account rebuild runs.
- An upload's derivation may outlast its request: the server continues it and answers `server_busy` with `retry_after`, to the uploader too. Repeating the request evaluates it afresh: entries already joined answer `known`, and conditional answers are re-evaluated. A request that joins nothing (such a repeat, or a framed fact's part that does not complete it) is answered at once, without deriving the account. While a derivation continues, an identical request waits for it and any other upload of the account answers `server_busy`. It publishes only with the authority its request would have at publication: after a lost-timeline transition the upload ran itself, that authority holds only on the timeline the transition made, and any later transition voids it. A server restart resumes it.
- A browser is a recovery source only for the receipts its journal retained.

### 6.12 Events

`GET /v1/events` is a server-sent-event stream for one account: `event: changed` with the account's head seq, and a comment heartbeat every 25 s. Each event re-checks the credential; the stream closes when it stops being valid. Events are hints: a client pulls (or re-reads its views) on a hint, on reconnect and on focus, and missed hints lose nothing.

### 6.13 Bounds

Advertised in `GET /v1/info`'s `bounds`:

| Bound | Value |
| --- | --- |
| `max_request_bytes` | 4 MiB |
| `max_operations` per push | 500 |
| `max_unframed_fact_bytes` | 256 KiB |
| `max_part_bytes` | 256 KiB |
| `max_part_count` per fact | 64 |
| `max_framed_fact_bytes` | 8 MiB |
| `max_fact_depth` | 32 |
| `page_bytes` (pull, snapshot) | 1 MiB |
| `recovery_inventory` | 2,000 entries |

Push, the recovery inventory and the recovery upload take bodies up to `max_request_bytes`, and an import chunk its encoded chunk (§11.5); other endpoints' bodies are limited to 64 KiB. The sync bodies (push, pull, snapshot, `POST /v1/refresh` and the three recovery calls) are decoded strictly (§3.1). The other endpoints' bodies are typed JSON objects that refuse unknown members and trailing data, without §3.1's further checks (duplicate members, invalid UTF-8, lone surrogates); `POST /v1/admin/keys` also refuses invalid UTF-8 and a lone surrogate escape in its body as `400 passphrase_invalid` (§11.1). An oversized unframed request is refused before anything is consumed (`413 payload_too_large`); the client reframes it. Operation designs keep every fact well below 8 MiB; large logical batches (catalog observations) are several facts.

### 6.14 Scheduling

Push soon after local actions and at the checkpoint cadence (§8.2); pull at launch and foreground, after a push, on network return, on an event hint, and at permitted background intervals. Retries use exponential backoff with jitter, capped at a few minutes, and resume indefinitely.

---

## 7. Operations and conflict rules

### 7.1 How state is computed

The accepted facts and decisions are the source; records are caches. For any entity, the server folds every contribution in the deterministic causal order (§6.9), scoped to its exact generation.

- A handler records **contributions** `(scope, node, effect)`, where the scope is one episode within one generation, one podcast, or the account. A fact affecting several entities contributes to each explicitly (bulk members, an unsubscribe's fan-out to its generation, a cut's fan-out to the episodes it covers).
- **The fold unit is the whole scope**: after a push, every scope a new node contributed to is refolded from all its contributions through one state machine that produces every field group together, so conditional effects and bundles are re-evaluated with everything they depend on. Changed groups are published as records.
- **The default register rule**: explicit contributions beat automatic ones (admissions, imports); among explicit ones the last in order wins. Field-specific rules below override it.
- **Outcome** at acceptance compares the fold with and without the fact: `applied` if it determines a value it writes (or its conditional effect fired), `superseded` if a later-ordered fact decides every value it wrote, `not_applied` with a reason if a rule made it ineffective at its place. A bulk fact is `not_applied` only if every member fails, with the first member's reason.
- **Effects that depend on a device's settings or state** (enqueue location, "Enqueue downloaded", "Delete removes from queue", "Keep skipped episodes", the smart mark-as-played threshold, completion, the current episode) are decided on the device where the action happened and sent explicitly. Receivers and the server never re-derive them.
- **Explicit intent beats automatic filing.** A **barrier** prevents automatic Inbox filing from undoing an interaction: an explicit play-state command, a dismissal, a queue add or restore, a session start, and listening activity. Favorites and metadata reads are not barriers. Automatic queue membership never overrides an explicit membership decision. Undo is an explicit compensating fact and can restore NEW.

**Episode references.** Every episode a fact acts on is an `EpisodeRef`: `{episode_id, podcast_id, generation_id, occurrence: {k, v, guid?, enclosure_url?, title?, publication_time?}}`, where `k`/`v` are the identity kind and value the device derived the ID from (`k: "none"`, `v: ""` for an `e0:` episode) and the rest is the raw evidence of the release acted on. It also serves as the placeholder metadata. The first accepted fact naming an episode registers it with its podcast and occurrence; later facts never change that. Acceptance checks, in order: `unknown_podcast`, `unknown_generation`, `episode_podcast_mismatch`, `generation_closed`. A fact naming a merged identity is routed to the survivor (§9.7) before any state check.

### 7.2 Generations and subscriptions

A generation is opened by a subscribe or register-preview fact (or an import decision) and identified by `G` (§3.3). Which podcast and generation such a fact acts on is decided once at acceptance and persisted as its `podcast.resolution` decision `{podcast_id, generation_id, mode}`:

| Mode | When | Effect |
| --- | --- | --- |
| `created` | the URL is not an alias in the account | the fact creates the podcast and its first generation; the alias is claimed by a `podcast.alias` decision |
| `joined` | P has a live generation L and the fact's `observed` is absent, names L, or names L's removed predecessor | the fact acts on L |
| `opened` | every generation of P is closed and `observed` names the latest as `removed` | the fact opens a new generation |
| `stale` | anything else | `not_applied` / `stale_subscribe`: a request that did not observe a removal cannot revive the podcast; the device explains and offers an explicit retry with a current observation |

Concurrent subscribes are serialized by acceptance: the first opens a generation, the next joins it. A device that assumed its own `P`/`G` remaps when the resolution says otherwise, before sending anything that depends on it.

| Operation | Payload | Effect |
| --- | --- | --- |
| `podcast.subscribe` | `{feed_url, observed?: {podcast_id, generation_id, state}, policy?: {new_episodes_action, keep_updated}, credentials_secret?}` | as the table below; a created or opened generation takes the given policy or the defaults (`global`, keep updated on); subscribing from a preview turns keep updated on and keeps the action |
| `podcast.register_preview` | `{feed_url, observed?}` | creates a `not_subscribed` generation (keep updated off) if none is live; never downgrades |
| `podcast.archive`, `podcast.restore` | `{}` | state `archived` / `subscribed`; nothing else changes (archived NEW episodes are hidden, as in Android) |
| `podcast.unsubscribe` | `{}` | closes the generation: state `removed`; every episode state of the generation hidden; identities and aliases stay |
| `podcast.set_policy` | `{new_episodes_action?, keep_updated?}` (at least one) | each present field is an independent explicit register |
| `podcast.set_credentials` | `{secret_id}` or `{clear: true}` | the `credentials` register |
| `podcast.change_url` | `{url}` | claims the normalized URL as an alias and makes it the generation's refresh URL; owned by another podcast → `not_applied` / `alias_conflict` (both identities and URLs stay; no merge) |

| Fact | No live generation | Live `not_subscribed` | Live `subscribed` | Live `archived` | Named generation closed |
| --- | --- | --- | --- | --- | --- |
| subscribe | created, `subscribed` | joined, `subscribed` | joined, no change | joined, `subscribed` | opened, or stale |
| register_preview | created, `not_subscribed` | no change | no change | no change | as subscribe, `not_subscribed` |
| archive / restore | `not_applied` | `not_applied` / `not_subscribed` | `archived` / no change | no change / `subscribed` | `generation_closed` |
| unsubscribe | `not_applied` | close | close | close | no change |

- **Removal is final for its generation**: it hides all of the generation's state, whatever the order of other writes to it. A fact naming a generation already closed when it is accepted is `not_applied` / `generation_closed`; old facts remain only as evidence. Re-subscribing starts fresh state and defaults.
- A remote unsubscribe hides the podcast at once but pins any playing local media until that playback ends; cleanup of files runs after. It never stops audio.
- The refresh URL belongs to a generation: its opening fact's `feed_url`, replaced by later `change_url` and move writes of that generation. Aliases are permanent and keep the raw spelling of their first claim. Alias claims are serialized: of concurrent edits claiming one URL, only the first wins.
- Device clean-up of previews never propagates. Previews nobody listened to are never registered.

### 7.3 Episode state, Inbox and favorites

| Operation | Payload | Effect |
| --- | --- | --- |
| `episode.mark_played` | `{episode, reset_position}` | play state `played` (explicit); position 0 if `reset_position` (a claim, §8.1). Menu and multi-select send `reset_position: true`; a swipe toggle on an unplayed episode `false`. Queue, history and statistics unchanged |
| `episode.mark_unplayed` | `{episode}` | `unplayed` (explicit) |
| `episode.reset_position` | `{episode}` | `unplayed`, position 0 (a claim) |
| `episode.restore_state` | `{episode, play_state, position}` | Undo: both set explicitly (may restore `new`); a claim |
| `inbox.dismiss` | `{episodes: [EpisodeRef…]}` (sorted) | per member, at its place: `new` → `unplayed`, otherwise nothing; always a barrier |
| `inbox.dismiss_all` | as `inbox.dismiss` | exactly the NEW episodes the device saw (hidden archived and preview ones included); episodes filed concurrently elsewhere stay NEW |
| `favorite.set` | `{episode, on}` | the `favorite` register |
| `account.set_discovery_policy` | `{new_episodes_action}` (target the account) | the account register |

`new` is produced only by an admitted filing into the Inbox (automatic) or by `episode.restore_state` (explicit). An automatic `new` applies only if the scope holds no barrier contribution at all, whatever their order. Inbox dismissals come from: remove from Inbox (menu, multi-select, swipe, per podcast), a swipe toggle on a NEW episode, a download request with "Enqueue downloaded", and a completed download.

### 7.4 History

| Operation | Capability | Payload | Effect |
| --- | --- | --- | --- |
| listening activity (§8) | core | | a history entry at the fact's normalized time |
| `history.remove` | history | `{episode}` | a cut suppressing exactly this episode's covered entries |
| `history.restore` | history | `{episode, removal, time}` | cancels exactly the named removal's cut (`time` is evidence); only the first restore of a removal takes effect (`not_applied` / `already_restored` otherwise); an unknown removal waits (`dependency_missing`) |
| `history.clear` | history | `{}` (no targets) | a cut suppressing every covered entry, fanned out to every episode holding one |

**A cut observes its causal past**: an entry is covered if it is an ancestor of the cut. Before creating a cut a device exports its unexported saves (§8.2), and the cut's dependencies are the device's whole known frontier (every known fact no other known fact depends on). Unobserved concurrent activity survives, whatever the wall clock says. History time is the greatest unsuppressed entry's time, else `null`. Cuts never change last played, position or played time.

### 7.5 Account settings

`account.set_discovery_policy` and `account.set_queue_policy` (`{enqueue_location?, sorting?: {keep_sorted, sort_order}}`, at least one; `enqueue_location` and `sorting` are independent registers). Changes apply to later operations only; nothing is re-filed or re-sorted retroactively, except that the queue replay applies the policy in force at each step (§7.6).

- `enqueue_location` ∈ `back`, `front`, `after_current`, `random`.
- `sort_order` is one of Android's queue sort orders: `episode_title_a_z`, `episode_title_z_a`, `feed_title_a_z`, `feed_title_z_a`, `duration_short_long`, `duration_long_short`, `date_old_new`, `date_new_old`, `random`, `smart_shuffle_old_new`, `smart_shuffle_new_old`. `{keep_sorted: true, sort_order: random}` is `rejected` / `invalid_payload`.

### 7.6 The queue

The queue is a set of unique episodes (membership) plus an order. **The order is what one device would show if every queue command had been made on it, one after another, in the deterministic order (§6.9), each executed as Android executes it.** Keys are how every device stores and displays that order; the server chooses them and publishes them as records. Devices apply records; they never replay.

**Membership** uses the register rule: explicit decisions (adds, restores, removals, clears, completion and skip removals, a resolved add for a person's play) — the last in order wins. An automatic add (a filing) applies only if the scope has no explicit membership decision at all (otherwise `not_applied` / `explicit_state`). A clear names only what it observed, so a concurrent add of another episode stays; a removal beats a concurrent move (a move never adds). Archived and preview podcasts' episodes stay queued; a closed generation hides its entries.

**Placement intent.** Every placement carries `place`, the gap the device actually resolved against its queue: `{kind: "start"}`, `{kind: "after", after: EpisodeRef}` or `{kind: "end"}`. A placement without `place` is `rejected` / `invalid_payload`. An `after` anchor is context, never a target: an anchor that is removed, closed or merged never makes a command fail.

| Command | Android | `place` |
| --- | --- | --- |
| add, back | append | `end` |
| add, front | index 0, after the consecutive run of episodes downloading at the head | `start`, or `after` the last of that run |
| add, after current | after the current episode, then after the downloading run that follows it; if the current episode is not queued, the front scan | `after` the last episode passed, or `start` |
| add, random | a random index i, immutable on retry | i = 0 → `start`; otherwise `after` the episode at i−1 |
| Undo of a removal | `addQueueItemAt(saved index)` at Undo time | the index resolved against the queue at Undo time, clamped to [0, length], then as random's rule |
| drag, Move to top, Move to bottom, multi-select move | the run's resulting position | `start`, `after` the episode now before the run, or `end` |
| play bundle, filing, resolved add | as an add at the account's location | as above |

Local-folder entries never sync: a gap next to them is projected onto the nearest synced episode before the inserted run (or `start`); the device keeps its local entries' places with device-only anchors, rewritten after every local insertion or move.

| Operation | Payload |
| --- | --- |
| `queue.add` | `{members: [placement…], place}`: members in their requested order; each placement `{episode, key, position_id}` is the device's key prediction |
| `queue.restore` | `{members: [placement…], place}` (Undo of a removal); never sorts |
| `queue.remove` | `{episodes: [EpisodeRef…], reason}` (sorted); `reason` ∈ `user`, `download_deleted` |
| `queue.move` | `{episode, key, position_id, place}` |
| `queue.reorder`, multi-item move | `{run: [EpisodeRef…], place, members?}`: the complete selected run in its resulting order; `members` are optional key predictions |
| `queue.reorder`, sort dialog | `{sort: {sort_order, permutation?}, observed: [EpisodeRef…]}`; sent even when it changes nothing locally; `permutation` is the device's resulting order for `random` |
| `queue.clear` | `{observed: [EpisodeRef…]}` (sorted) |
| `session.start.queue_bundle` | `{key, position_id, place}` |

Keys must parse, lie strictly between the sentinels and carry their position ID's residue; an episode appears once per fact. Completion, skip and clear removals come from their own facts (reasons `completed`, `skipped`, `cleared`). Clients apply commands as the replay does: insert at `place`, then stable-sort if keep-sorted is on.

**The replay.** State: a structure of entries (one per episode identity and generation class, including entries out of the queue and ghosts), membership at the current step, the sorting policy at the current step, and the final identity mapping. Steps, in the deterministic order:

| Step | Effect |
| --- | --- |
| add, restore, play bundle, filing, resolved add | place only the members not in the queue immediately before this step, at `place`, in requested order; if any was placed and keep-sorted is on here, sort (a restore never sorts). A command that places nothing has no placement effect, but its membership effects (explicit provenance, the barrier) still apply |
| move, multi-item move | take the run out and insert it at `place`; acts on out entries too |
| removal, clear, completion or skip removal (if effective at its place) | membership out; the entry keeps its place in the structure |
| sort dialog | stable sort of the in-members only, permuted among the slots they occupy; out entries and ghosts keep their slots; `random` replays its recorded permutation |
| keep-sorted trigger | as a sort with the account's order |
| policy change | update the sorting policy |
| a filing that does not apply, a placement on a closed generation | a **ghost** at `place` if the episode has no entry: never a member, never sorted, never moves an entry; it exists so that anchors resolve |

`after P` resolves right after P's entry, ghosts included; if P has no entry at all the command goes to `end` (logged in diagnostics); if P is in the moved run, after its nearest predecessor outside the run. Merges apply from the start of history (the replay reruns from the earliest command naming either identity); a run naming both aliases keeps the first. Entries are keyed by generation class, so an anchor recorded against a closed generation never follows a new subscription's placement. An Android database import is one step at its manifest's place: its episodes at the end in the backup's global `Queue` order, once every imported podcast's decisions are present (§11.5).

**Sort values.** A sort uses canonical values (title, podcast ID, podcast title, publication time and duration) the server records at admission, in the receipt's `sort_values` (§6.1): a placement (add, restore, play bundle) or a move (move, run move) records those of its members that pass their checks, including members of an add that places nothing because they are already queued; an add, restore or bundle under keep-sorted at its place also records every member then queued; a sort command records every member then queued. A sort step uses its trigger's recorded values, and for a member not covered there, the values recorded with that member's latest placement at or before the step. A metadata change alone never re-sorts. Comparators are Android's (`FeedItemPermutors`), stable over the current order; missing values default to `""`, 0 and 0; titles compare by Unicode simple lowercase, then code point; smart shuffle reproduces Android's algorithm, breaking ties between equally large groups by podcast ID. The one documented exception to arrival independence: a metadata correction racing a sort can change which values the sort used.

**Keys and `queue.layout`.** Publication rule, a transaction invariant: **no commit publishes keys that differ from the replay's order.** Every commit whose inputs can change queue membership or order (queue commands, listening facts that change a completion or skip removal, generation closes, identity decisions, admissions, imports, receipt replacement, recovery requests) derives the replay's order of in-members, and if the published keys do not give it, records one `queue.layout` decision and publishes its keys.

- An in-member's **published key** is its latest key write in rank order `(epoch, seq, index, ID)` (in one commit: the fact, then its decisions, then the layout). A placement writes keys only for the members it actually places; a move or run move writes for every member carrying a key, out members included; an add or restore member already queued writes nothing; failing members and rejected facts write nothing.
- A layout's `result` lists only the members whose key changes: the in-members along the first longest increasing run of published keys keep theirs, the others get fresh keys left to right. `inputs: {frontier, mapping_version, policy}`; its dependencies are the queue frontier and every causal input the replay read.
- `queue_position` records are republished only when they change. Layouts are archived and recovered but never applied as order; a stale layout can change keys, never the order. Different recovery request partitions may give different keys, never a different order.

**Server-resolved adds.** A client without the queue capability sends `session.start` without a bundle (a bundle from it is `rejected` / `capability_not_declared`). If the episode is not queued at the start's place, with the start's own effects counted, the server resolves the add once at the account's location (`after_current` becomes the back, `front` the front, `random` drawn once) as a `queue.resolved_add` decision whose effect is the start's own, at the start's place. Server filings into the queue resolve the same way. Retries, recovery and replay reuse the decisions.

### 7.7 Merges

Only the server merges or splits identities (§9.7); clients never do, and in server mode phones do not run AntennaPod's duplicate repair.

- An **episode merge** moves the source's contributions in every scope (and its listening indexes, catalog item and aliases) to the root survivor, which is refolded from the union: sessions keep their IDs; membership and position stay registers, so one queue item survives; explicit removals and dismissals are kept; of two admissions, the earlier one's filing counts. The source keeps its `identity` record with `merged_into` and its state records are tombstoned. A merge never touches playback.
- When a merge or mapping puts two effects of one node in one scope, they become a compound applied in a fixed order: membership effects before position-only effects, then by original episode ID.
- A **podcast merge** maps every source episode (re-derived under the survivor, through the survivor's evidence) and every generation: the source's live generation joins the survivor's live class; closed source generations stay closed and contribute nothing, even where episode IDs match. Subscription properties are the survivor's, unless the survivor's latest generation is removed and the source's is live. Later facts are routed by their original identity and generation; an unsubscribe that observed the merge closes the joined class; a pre-merge removal closes only the class it named.

### 7.8 Reason codes

| Reason | Outcome | Meaning |
| --- | --- | --- |
| `unknown_field`, `missing_field`, `invalid_author_id`, `invalid_author_seq`, `invalid_previous_op_id`, `invalid_dependencies`, `unsorted_dependencies`, `invalid_type`, `invalid_type_version`, `invalid_client_time`, `invalid_targets`, `unsorted_targets`, `invalid_payload` | `rejected` | envelope or payload fault |
| `impossible_author_history`, `capability_not_declared` | `rejected` | author chain or capability fault |
| `unknown_podcast`, `unknown_generation`, `episode_podcast_mismatch`, `generation_closed` | `not_applied` | acceptance checks |
| `stale_subscribe`, `alias_conflict`, `not_subscribed`, `explicit_state` | `not_applied` | subscription, URL and queue rules |
| `foreign_session`, `session_ordinal_conflict`, `session_sealed`, `session_finalized`, `already_restored` | `not_applied` | listening and history rules (§8) |
| `not_playable`, `ineligible`, `already_admitted` | `not_applied` | discovery (§9.5) |

---

## 8. Playback, history and statistics

The originating player reports what happened; the server never infers completion from a remote duration or position. Device playback never waits for the server.

### 8.1 Sessions and ownership claims

A **session** is one continuous period of listening to one episode on one author: a UUID, an episode and generation, a starting position, numbered checkpoints and at most one end. It corresponds to Android's played-duration baseline, taken when media becomes current in the running player. Pause and resume keep the session; reloading the player after process death takes a new accounting baseline; the next intentional playback takes a new session, as Android does.

**Position ownership claims**, folded in the deterministic order, decide which listening owns the position. The latest claim owns it. Claims are:

- an intentional `session.start` (a play, a continuous-play transition, an intentional seek that takes over from a known newer claim);
- an intentional resume of an open session: a `session.checkpoint` with `claim: true`;
- `episode.mark_played` with reset, `episode.reset_position`, and a position-restoring `episode.restore_state`.

So **where play was pressed last owns the position.** A claim is recorded when playback actually starts after the person's command (READY), with that moment's time, never backdated. Only the person's play command creates a claim: in-app play buttons, lock screen, Control Center, headphone and remote Play, CarPlay and Android Auto, Siri and Shortcuts, widgets. Buffering, interruption-end and focus resumption, un-ducking, reconnection and Cast restoration never do. A play that fails or is cancelled records nothing. Automatic restoration of a paused player is not a claim and emits no fact at all: it initializes only a local accounting baseline and cannot write position, enqueue, dismiss NEW or create history.

**Claims belong to a writing path.** Each checkpoint, end, finalization and leave resolves against the latest claim on its own path (its author's continuation and the shared prefix it inherited), so a copy's resume cannot be taken back by the original's ordinary checkpoint. Only checkpoints on the owning claim's path, after it, move the position. An explicit reset bars later checkpoints of older sessions unless a new claim is made on that path. A new session that follows a legitimate completion does not invalidate it.

### 8.2 Recording and exporting

- Every five-second save, and the saves at start, resume, pause, stop and end, durably records position, time and contribution in a local journal. Each save has a random ID, the session's next save index, and its baseline (a UUID begun whenever the player loads the episode).
- **Contribution** (Android's `played_duration` increment): 0 at the start; on a save beyond the session's start position it is `position − start_position`, otherwise it keeps its previous value (the below-baseline rewind rule). Forward seeks count, backward seeks can lower it, speed does not scale it.
- **Wire checkpoints** are created from the journal at most once a minute while playing, at every intentional resume (a claiming checkpoint), and at pause, stop, end, switch and any explicit action needing the latest checkpoint (history clear, statistics reset, position reset). The export watermark and the outbox insert are one transaction. Numbered facts are never thinned or rewritten.
- A process restart exports the latest unexported save before anything else, as a passive export. A recorded claim is owed durably until its outbox insert succeeds, and is flushed before any later fact of the device; later saves never replace it.

| Operation | Capability | Payload |
| --- | --- | --- |
| `session.start` | core | `{episode, session_id, start_position, activity, baseline?, observed_claim?, duration?, queue_bundle?}` |
| `session.checkpoint` | core | `{episode, session_id, ordinal ≥ 1, position, contribution ≥ 0, activity, claim?, observed_claim?, save?, duration?}` |
| `session.end` | core | `{episode, session_id, ordinal ≥ 1, position, contribution ≥ 0, activity, reason, completed, threshold_ms, remove_from_queue, history, save?, duration?}` |
| `session.finalize` | core | `{episode, session_id, ordinal ≥ 1, claim_id, completed, threshold_ms, remove_from_queue, history}` |
| `episode.leave` | core | `{episode, transition_id, reason, position, observed_claim?, completed, threshold_ms, remove_from_queue, history}` |

- `ordinal`: a session's checkpoints and end share one sequence from 1, strictly increasing, assigned by the device and never renumbered.
- `activity`: the save is a listening position save (it sets last played and history and dismisses NEW). An end that only seals (for example `superseded`, sealing a paused session at its pre-adoption position) has `activity: false`.
- `claim` (strict boolean, default false) and `observed_claim` (a UUID, only with `claim`): a claiming checkpoint must have `activity: true` and cover a new, non-passive save. Malformed claim fields are `rejected` / `invalid_payload`.
- `save: {id, index, first?, baseline, passive}`: the exported save's identity; `first` is the first save it covers (absent when it covers no new save); `passive` marks a restart or tail export. Phones always send it.
- `reason` ∈ `ended`, `skipped`, `switched`, `removed`, `suspended`, `superseded`. `completed` and `threshold_ms` are the device's decision; `remove_from_queue` is the device's queue decision for a skip with "Keep skipped episodes" off; `history` is the end, skip, switch or finalization history mark Android writes (`playback_completion_date`).
- `session.finalize` is the lazy completion of a paused previous episode: it names the session, the latest ordinal the device holds, and `claim_id`, the claim whose position the device evaluated (the path's latest claim). It adds no contribution and does not reopen the session.
- `duration` is the device's measured duration, a register (the last in order wins), with the reporting author as `installation_id`.

### 8.3 The episode fold

| Contribution | Effect at its place |
| --- | --- |
| `session.start` | a claim: position := `start_position`; `new` → `unplayed` (never `played` → `unplayed`); a barrier; a listening activity if `activity` |
| `session.checkpoint` | if its path's claim is current: position := `position`; if `activity`: `new` → `unplayed`, a barrier, a listening activity; the session's contribution at this ordinal. With `claim`: first a claim at this node |
| `session.end` | as a checkpoint, then the session is sealed; if `completed` and its path's claim is still current, the **completion bundle**; otherwise, if `remove_from_queue`, queue out with reason `skipped`; if `history`, a history mark |
| `session.finalize` | if `completed` and `claim_id` is still the current claim, the completion bundle under it; else if `remove_from_queue`, queue out (`skipped`); if `history`, a history mark |
| `episode.leave` | if `history`, a history mark; if no earlier leave of the same `transition_id` was effective: if `completed` and the current claim equals `observed_claim` (both absent counts as equal), the completion bundle under that claim; else if `remove_from_queue`, queue out (`skipped`) |

A checkpoint or end writes the position (and an end's bundle fires) only if its ordinal exceeds every ordinal of its session ranked before it.

**Admission checks** for session facts, after §7.1's: a checkpoint, end or finalize naming a session without an accepted start, or an ordinal the session does not have, is `rejected` / `invalid_payload`; a different episode or generation than the start is `rejected`; a fact from outside the session's author lineage is `not_applied` / `foreign_session`; a different fact at an existing or lower ordinal is `session_ordinal_conflict` (reported before `session_sealed`); anything after the end is `session_sealed`; a second finalization is `session_finalized`. Starts, checkpoints and ends are `applied` whenever admitted (they record progress even when another session owns the position); a finalization or leave whose completion or removal did not fire is `superseded`.

### 8.4 Completion

- The originating device evaluates its own measured duration and smart threshold. A natural end completes. A switch, skip, lazy finalization or leave completes only within the threshold. A skip outside it never marks played and removes from the queue only if "Keep skipped episodes" is off. Pausing, suspension and supersession never complete.
- The **completion bundle** is one conditional effect: play state `played`, position 0 under the validated claim (which stays current), queue membership out with reason `completed`. Its history mark is separate. It is evaluated **at its place in the deterministic order**: a takeover or explicit reset ordered before it suppresses it, even if it arrived later; recomputation then restores the prior state. A replay of a played episode stays played unless marked unplayed, as in Android.
- `episode.leave` records Android's switch-away effect for an episode that was loaded but not played in that load (continuous playback off, a paused load, Cast adoption): its history mark and its threshold completion. It is not a claim, not a barrier, does not dismiss NEW and adds no contribution. Only the first effective leave of a pending transition (`transition_id`, which a copy of the library keeps) completes or removes. The device records it only from that exact pending transition.
- The device stores the pending previous-episode transition with its current-episode preference. Loading a different episode consumes it and records the finalization (or leave) before the new episode starts. Resuming the same episode consumes it without completion and starts a new session if the previous one was sealed. An open session abandoned by app removal stands at its latest checkpoint.
- Auto-delete and choosing the next item are decided once, on the originating device; it selects the next queue item before removing the completed one. Loading the next item paused fabricates no listening. An incoming completion never makes another device delete media.

### 8.5 Copies: a session's saves form a tree

A session's **family** is every continuation of it: its author's, and those of derived branches or local-detection authors whose lineage descends from it. The exported saves of a family form a tree: each continuation is a path (its exports, preceded by the exports it shares with its ancestors); two paths branch after their last common export (at the smaller-index export when both next cover the same first save).

- **Played time** of a family: the sum over the tree's edges of child minus parent contribution, plus the root's, at least zero. The shared prefix counts once; each continuation counts its own change, including rewinds; the result does not depend on which library kept the author or on arrival order.
- **Claims past a divergence**: a continuation's export claims if it is not passive, covers a new save, and is its first export under a baseline that neither the start nor any export at or before its branch point uses. A session with a single continuation keeps its start's claim through reloads. Without a claim of its own, an export writes under the latest claim on its path.
- A passive export of a save that another export of the family descends from writes no position.
- **Copied finalizations**: a descendant author may finalize a session it holds the pending transition of. In a family of several continuations, a finalization is eligible only if no export of another continuation descends from the save it names; only the first eligible finalization in order takes effect; ineligible ones keep only their history mark.
- A statistics reset covers every exported save in its causal past and, along each path, every node before a covered one.

### 8.6 Applying a remote position

- Sync never seeks, pauses or replaces playing audio. An active old session does not become newer because a pull arrived; its later checkpoints cannot take ownership back.
- A loaded but paused player adopts a newer position (including an explicit reset to zero): it seals its local session at the pre-adoption position and contribution without listening activity, and changes the resume position. The remote jump is not counted as local listening. An intentional resume then claims at the adopted position.
- Adoption waits for a view that includes the device's own latest local claim (acknowledged and covered by the view's head, or still pending, in which case the overlay wins). A position whose `claim_id` is one of the device's own continuation's facts is its own.
- Starting playback uses the newest locally available position, including unsent local changes; it never waits for a pull.

### 8.7 Last played and history

- `last_played` is the greatest normalized time of actual listening position saves, including start or resume at READY. Selecting an episode paused and adopting a remote position are not listening activity.
- History entries are those saves plus the end, skip, switch, finalization and leave marks. A lazy finalization days later can update history without changing the statistics month.
- Both use the source facts' times; receiving or recovery time is never listening time.

### 8.8 Statistics

- **Played time** of an episode: Σ over its sessions (families, §8.5) of `max(0, C_latest − C_cut)`, plus its uncovered imported baselines. `C_latest` is the contribution at the session's greatest ordinal; `C_cut` is the contribution at its **effective cut**, the greatest ordinal any reset covered (0 without one). A session whose end was covered contributes 0 thereafter. Sessions that never owned the position still count. Removed generations never count.
- **`statistics.reset`** (`{}`, no targets, capability statistics) is a cut over its causal past: per session the greatest covered ordinal (only admitted checkpoints and ends give ordinals), fanned out to every episode holding such a session. Concurrent resets union by ordinal, never by numeric contribution, so a rewind cannot resurrect cleared listening, and listening before a reset on another device stays cleared. This corrects Android, whose stale in-memory baseline can undo a reset. Example: reset at contribution 20, rewind to 10, reset again, advance to 15: 5 counts.
- **Projections** (the read API and clients compute them from records, as Android does):
  - per podcast: subscribed and archived podcasts' episodes, each episode's whole played time filtered by `last_played` in `[from, to)`;
  - per month: each episode's whole residual total in the UTC month of its `last_played`, preview episodes included; never split across months;
  - episodes started: history time present and played time > 0 (with "include marked as played": also played or position > 0); not date-filtered;
  - include marked as played: played time if nonzero, else the duration if played; all-time range; total duration sums durations;
  - duration: the measured duration register, else the feed duration.
- Imported rows that became one episode but keep their own statistics are `statistics_units` (§11.5), each counted as Android counts a row.

---

## 9. Discovery, refresh and parsing

### 9.1 Who refreshes

- **Phones** refresh as in standalone mode, directly from publishers with the account's feed credentials: subscribed podcasts with keep updated on, on their schedule and on pull-to-refresh; a manual refresh of one podcast even with keep updated off. They never wait for the server.
- **The server** refreshes every account's subscribed, keep-updated podcasts on a persistent schedule and on request (`POST /v1/refresh`, §9.6).
- **The web client** cannot fetch feeds; it relies on the server.
- Archived, preview and removed podcasts are not refreshed, except by an explicit refresh of one podcast.

### 9.2 Parsing

The server's parser (`internal/feed`, parser version `ntp-go/1`) is a faithful port of the reference release's SAX pipeline, and every client parses with the same rules and declares a parser version. Expected outputs come from running the reference release's unchanged parser classes on its emulator; they are the shared feed vectors (`contract/vectors/feeds`, `feed_values.json`, `feed_date_symbols.json`) every implementation passes.

- **Decoding**: commons-io `XmlStreamReader` rules (BOM, 4-byte guess, prolog encoding); malformed UTF-8 becomes one U+FFFD per maximal subpart; an unknown charset is a parser error.
- **XML**: Expat's well-formedness and limits; internal DTD entities expanded at use, external entities never fetched; namespaces registered as Android's SAX prefix mapping does (a namespace registers when first declared with its canonical prefix; the Podcasting 2.0 URI under any prefix; unprefixed elements fall to the default namespace; an unbound prefix is malformed).
- **Handler state** exactly as Android's `SyndHandler` and `HandlerState`; namespace handlers for RSS 2.0, Atom, iTunes, content, Dublin Core, Media RSS, Podlove simple chapters and Podcasting 2.0, rule for rule (description-if-longer by UTF-16 length, the 16 KiB enclosure-size floor, Android's API 36 MIME map, `media:content` `isDefault`, `itunes:new-feed-url` only when it starts with `http`).
- **Values**: Android's `DateUtils` (with Android 16's ICU zone names and tzdata history; `parseOrNullIfFuture` against the fetch time), `DurationParser`, chapter time strings, and `HtmlCompat.fromHtml(…).toString()` for titles, descriptions and author.
- **After parsing**: a feed without a title, or an item without one, is a parser error. A feed without an image gets the cover marker `antennapod_generative_cover:<download URL>`. An HTML page gives Android's "unsupported type (html)" error with its title.
- **Server budgets** (Android has none; hitting one is a parser error and keeps the previous catalog): Expat's entity amplification rule and 16 MiB of expanded entity text; 64 MiB of retained decoded text; 100,000 items; 256 levels of nesting. Parsing checks cancellation throughout, so the fetch deadline also stops it.

### 9.3 Fetching

- GET with the server's user agent and `Accept-Encoding: gzip`; only `http` and `https`; URL userinfo and fragments are never sent.
- **Conditional requests** use the last successful response's `ETag` and `Last-Modified`, only for the same representation key (the URL fetched after redirects and the credential revision). A `304` without a matching stored response is an error. A forced fetch (a single podcast's refresh request, §9.6), the first fetch after a failure, a generation's first population, and a fetch after a URL or credential change send no validators; a refresh of every podcast is an ordinary fetch and sends them.
- **Unreliable ETags**: a host whose ETag fails to match an unchanged body while its `Last-Modified` is unchanged (a request with `If-None-Match` got a `200` whose body and non-empty `Last-Modified` equal the stored response's) is sent only `If-Modified-Since` from then on, and is fetched without validators at its first scheduled fetch 24 hours or more after its last full response (the stored response's evaluation time), so a change its validators hide shows within 24 hours plus one interval with its jitter, later only by a failure's backoff or a `Retry-After`. The mark is kept with the stored response, so it survives a restart, and is forgotten when the URL, the representation key or the credentials change. A host whose `Last-Modified` changes is never marked, and an unmarked host gets no such daily fetch.
- **Redirects**: at most 20, manual, `http`/`https` only; `Authorization` is dropped on a cross-origin hop. A **permanent move** is Android's: the first hop is 301 or 308, or the second URL is the first upgraded from `http` to `https`. Temporary redirects never add aliases.
- **Credentials** are decrypted only inside the fetch and sent as Android does (after a `401`: ISO-8859-1 Basic, then once UTF-8), and only to the secret's approved origins.
- **Bounds**: 30 s to first byte, 120 s total, 64 MiB decoded body, 16 KiB per URL; a `Content-Length` that disagrees with the body is an error; nothing partial is kept.
- **Destinations**: loopback and unspecified addresses are refused after resolution, on every hop; LAN addresses are allowed.
- **Errors** use Android's download-error labels (`unauthorized`, `forbidden`, `not found`, `http status`, `I/O`, `too many redirects`, `size limit`, `time limit`, `parser exception`, `unsupported type`, `unsupported type html`) with a sanitized detail (no credentials, userinfo or query strings).
- **Stored response**: the last successful body per podcast, gzip-compressed and sealed, with its representation key, validators, source URL, evaluation time, parser version and unreliable-ETag mark, so a parser upgrade can re-parse without refetching.

### 9.4 Catalog observations and canonical metadata

**`catalog.observe`** (core): `{podcast_id, batch_id, parser_version, fetch_time, feed?: {base, fields}, items: [{episode: EpisodeRef, base, fields}]}`, one fact per chunk within the fact bounds. Targets are the podcast and every episode, without generations: observations are catalog evidence, accepted whatever the subscription state.

- A device sends only records that are new or differ from the canonical record it holds; unchanged refreshes upload nothing. Each record is whole (every field, `null` where the feed lacks it). `base` names the canonical revision the device last received for that record (omitted or `null` without one). `items` may be empty only when `feed` is sent. URLs carry no userinfo.
- **Replacement rule**, per record in rank order: an observation replaces the current record if there is none, if it names the current revision as its `base`, or if it is concurrent (another base) and its fetch time is later. A server fetch names the revision current when the fetch started; a device's fetch time is clamped to its fact's normalized time. So a phone that reaches a publisher the server cannot still repairs metadata for every device, and a newer parser never overrides a newer observation by itself.
- An episode missing from a later fetch keeps its catalog item and metadata: episodes are never deleted because they left the feed. A failed fetch writes no observation. Metadata changes never file or sort.
- Devices keep showing their own parse until the canonical record arrives. A canonical update never replaces a media URL that is working for the playing device, and never interrupts playback.
- A podcast's catalog items record which episodes the catalog knows from feeds, with each item's effective publication date (its date, or the first observation's time when it has none, as Android persists a null date as "now"). A listening placeholder never makes an episode known to the catalog.

### 9.5 Discovery proposals and admission

**`discovery.propose`** (core), one fact per eligible playable discovery, after its batch's observations: `{podcast_id, generation_id, episode, batch_id, pub_date, newest_before_batch, gained_media?: {catalog_only_observation}, action, policy_node, queue_bundle?}`. `action` is the new-episodes action as the device knew it, with `global` resolved through the account default; `policy_node` is the policy write it read.

**Eligibility** (Android's rule): the live generation is subscribed; the batch is not the generation's first population for its source (first subscription, preview registration, imports never file); the item is playable (has an enclosure); and either its publication date is missing or not before the newest effective date before the batch, or it is a gained-media transition (the catalog knew it as catalog-only, the proposal names that observation, and it now has an enclosure). A gained-media item never changes an episode with explicit state and never turns a played episode back to new. Devices send proposals only for eligible discoveries.

**Admission**: the first eligible proposal accepted for `(G, E)` is admitted by a `discovery.admission` decision (`result: {episode_id, generation_id, action, proposal_id, source, key?, position_id?}`); later ones are `not_applied` / `already_admitted` and never re-file. Other outcomes: the acceptance checks of §7.1, `not_subscribed`, `not_playable`, `ineligible`. The filing acts at the proposal's place, with the proposal as its origin:

- `inbox`: an automatic NEW, not applied wherever any barrier exists;
- `queue`: an automatic add with the proposal's bundle (or the server's resolution for a device without the queue capability), not applied if the scope has an explicit membership decision; under keep-sorted it is a sort trigger;
- `nothing`: no effect; the filing is used up.

The resolved action stands even if the policy changed since. A device converges on the admission record: a losing proposal's local filing is replaced by the admitted one, except where its person already acted. A proposal naming a closed generation is `generation_closed`; late observations still update the catalog but never file, download or notify for a closed generation.

**The server's own discoveries** follow the same rules against its own fetch history: on a successful fetch, each item new to the server's fetch history (or known there as catalog-only and now playable) is admitted if the generation the fetch started under is still live and subscribed, its first population is recorded, and the date rule holds against the newest effective date the server's fetches had seen before. Each such admission is its own filing place and sort trigger, in the fetch's item order. First population is recorded per generation by the first successful fetch's `catalog.refresh` part 0, which admits nothing; a retry after a failed first fetch is still first population; a feed that later becomes empty is not.

### 9.6 Server refresh

- **Which**: every podcast whose live generation is subscribed with keep updated on; one podcast through the refresh API whatever its state (a removed podcast's explicit refresh updates the catalog but files nothing); a newly subscribed or registered generation at once.
- **Schedule**: every `--refresh-interval` (default one hour) per podcast with ±10% jitter, persisted per account and podcast, so a restart or wake runs what is overdue, spread out. At most 4 fetches at a time and 1 per host; while another account has refreshes waiting, an account uses at most half; accounts take turns, the one with fewest running first, then the one served longest ago.
- **Quiet podcasts** (the server's own schedule only; phones refresh on theirs, §9.1): a podcast whose catalog the server's successful fetches have found unchanged for 30 days is fetched every 6 hours, and after 90 days every 24 hours, or every `--refresh-interval` if that is longer; jitter applies as above, so a nominal 24 hours can be 26.4.
  - The quiet time is counted from the start of the last successful fetch that found catalog activity: a new item, whatever its date; a changed item record; or an item new to the server's fetch history (or newly playable there). An item a phone observed first is such activity when the server's own next fetch applies it.
  - A change to the podcast's own metadata alone, a `304`, a failure and a re-parse are not activity, and keep the quiet time. Publication dates are never used.
  - The quiet time starts at a podcast's first successful fetch, so a podcast without a recorded quiet start (a new subscription, an import, a first attachment) is fetched every `--refresh-interval` for at least 30 days. A URL a person sets, new credentials or a new generation start it again, from the first successful fetch that uses them, as does a clock that went back; a move the server's own fetch applies (a permanent redirect or a fetched `itunes:new-feed-url`, Moves below) keeps it.
  - A fetch that finds activity makes the next interval `--refresh-interval` at once. A deadline recomputed after a clock jump uses the podcast's own interval (§14).
- **Failures** back off from 15 minutes, doubling up to 24 hours, with jitter. `Retry-After` on 429 or 503 is a lower bound on the next attempt, capped at 30 days, and kept across restarts.
- **Coalescing**: one fetch per account and podcast at a time; a manual request during a fetch joins it, except that a single podcast's request joining an ordinary (not forced) fetch is answered by a forced fetch after it. One fetch context per account and podcast, so credentials and validators are never shared between accounts.
- **Moves**: a successful fetch that reports a permanent move, or a fetched `itunes:new-feed-url`, writes `podcast.moved`: the generation's refresh URL becomes the new URL if the generation and its refresh-URL write are still the ones captured when the fetch started (a person's newer edit wins); each 301 or 308 target and the fetched new feed URL become aliases if unclaimed (a claimed one triggers a podcast merge, §9.7). Approved credential origins never change.
- **Outcome**: a failed fetch keeps the catalog and updates `refresh_status` with Android's label and a sanitized detail; a successful one applies, in one server commit, the changed records as `catalog.refresh` parts of at most 200 items, the admissions, the move and the status. `refresh_status` is published when its outcome class or error changes, and at most once a day otherwise. Its `next_attempt` is the attempt planned when the status was published: a request, and a fetch that finds catalog activity, can bring the real one forward.
- **Refresh API**: `POST /v1/refresh {podcast_id}` refreshes that podcast whatever its state or policy (a forced fetch); with no body it refreshes every podcast whose live generation is subscribed with keep updated on (ordinary fetches, with validators). It answers `202` at once with each podcast's live status. Phones never wait for it. Refreshing every podcast does not bring one forward past its `Retry-After`: it is fetched when that ends, which its status shows; refreshing one podcast fetches it at once regardless.
  - **Requests** are kept with the podcast's schedule, so a restart loses none: each gets the podcast's next `request_seq`, which its answer carries (`null` for a podcast a refresh of every podcast left to its `Retry-After`, which records no request). The attempt that answers a request is the next one to end, whatever its outcome (success, `304`, failure); a request that joins a running fetch is answered by it, except when a single podcast's request joins an ordinary (not forced) fetch: that fetch then answers only the requests made before it started, and a forced fetch after it answers the rest. A request is answered only with an outcome a person can see: an outcome that cannot be written is recorded as an `I/O` failure instead (the catalog untouched), and an attempt that could neither read its context nor write an outcome or that failure answers nothing: its requests stay pending, and the podcast runs again after a minute, whether or not it is kept updated, each such wait in a row twice the last, up to 15 minutes. `done_seq` is the newest request answered. A request for a podcast merged before it ran is answered without a fetch; one a later `Retry-After` holds back during a refresh of every podcast is answered and left to its schedule.
  - **Status**: `GET /v1/refresh?podcast_id=a,b,…` (the parameter comma-separated or repeated, at most 500 podcasts, at least one) answers `200` with the named podcasts' statuses in that order, each once, and schedules nothing; a podcast the account does not have is left out. Each podcast is read by its keys, never the library. A request is answered once `done_seq` reaches its `request_seq`, or the podcast is merged (`merged_into`). An unchanged feed commits nothing and sends no event, so this read is how a client learns that its refresh ended.
  - **Items**, in both answers: `{podcast_id, last_attempt, last_success, error, next_attempt, in_progress, request_seq, done_seq, running, merged_into}`; times in ms or `null`; `in_progress` is a fetch running or a request waiting for one; `running` is a fetch running now; in a status read `request_seq` is the newest request's (`null`: none yet).
- **Re-parse**: when the parser version changes, each stored response is re-parsed with its own source and evaluation time; changed records are `catalog.refresh` parts with outcome `re-parse`, naming as base the revision their original fetch wrote and keeping the original fetch time. A re-parse never proposes, admits, moves or changes `refresh_status`. After 3 timeouts for one parser version the response counts as unparseable.

### 9.7 Identity reconciliation

Only the server decides identity, by decisions that recovery restores before dependent facts.

- **Evidence**: every observation records occurrences `(GUID, enclosure, publication date) → episode`. A device observation counts as evidence for an episode only if that episode is the plain derivation of its occurrence or a published rule's result. **Lookup aliases** exist only by server decision (a GUID-change match or a merge).
- **Disputed enclosure**: an enclosure observed under two different GUIDs is disputed and never used for lookup (a bulletin feed reusing `/latest.mp3`). Different GUIDs never share identity through an enclosure.
- **Reused GUID**: a GUID observed with different, non-empty enclosures and two non-null publication dates more than 7 days apart becomes disputed. Duration differences and missing dates are never dispute evidence (signed URLs and dynamic ads vary for one release). For a disputed GUID the server keeps **release groups**: occurrences within 7 days of each other are one release, whatever their enclosures; the earliest release keeps the original `rss-guid` ID; a later release takes `rss-guid+enclosure` of its first enclosure, or, when an earlier release already uses that pair (rotating enclosures), a persisted `e0:` occurrence ID. Each grouping is an `episode.occurrence_mapping` decision, published in `identity_rules`. Every device and the server resolve an item with a disputed GUID to the release whose publication date lies within 7 days of its own; without a date, to the release listing its enclosure if exactly one does, otherwise the original. A fact's contributions follow the release its occurrence resolves to; facts without occurrence evidence, and ambiguous ones, stay with the original and are listed in diagnostics. Nothing is copied to both.
- **GUID change**: on a complete successful server fetch (never a device observation or a re-parse), the old candidates are the episode roots the server's earlier fetches saw that no item of this fetch resolves to, and the new candidates are this fetch's items that resolve to an episode new to the server's history. A pair matches when both have identical non-empty titles and identical non-null publication dates to the second; only a one-to-one match across both whole sets counts. A match records the new GUID as a lookup alias of the old root (`episode.guid_change`) and merges the new item's episode into it if one exists. A rolling feed whose new item has the same title but another date stays separate.
- **Episode merges** (`episode.merge {from, into}`) always name the root survivor, the episode that appeared first; a later merge of a survivor rewrites existing redirects so every redirect is one step and no cycle forms. Effects in §7.7.
- **Podcast merges** happen only when a server fetch's permanent move or fetched new feed URL lands on an alias of another podcast: the same feed answers at both URLs. The survivor is the podcast created first. Titles, `podcast:guid`, artwork or overlapping episodes never merge podcasts; different feed URLs (a public feed and a subscriber feed) stay separate podcasts. Effects in §7.7; the source's subscription record becomes `{state: merged, merged_into}`.
- **Clients** derive with `identity_rules` (§3.3), redirect `merged_into` identities (show and act on the survivor, move local downloads and positions without interrupting playback), and never merge or split.

### 9.8 Feed discovery from a web page

Adding a podcast by URL (the web client's search and add) follows Android's flow: Java trim; strip `feed://`, `pcast://`, `pcast:`, `itpc://` and `antennapod-subscribe://`; unwrap `antennapod.org/deeplink/subscribe?url=` and `subscribeonandroid.com/`; prepend `http://` without a scheme. A feed is returned as is. An HTML page yields its head `<link>` elements whose `rel` is `alternate` or `feed` and `type` `application/rss+xml` or `application/atom+xml` with a non-empty `href`, titled by the `title` attribute (else the `href`), in Android's order (by the URL's `String.hashCode`); relative URLs resolve per RFC 3986 against the page URL, a link back to the page itself is dropped, and at most 1,000 links (4 MiB of resolved URLs) are kept. A `401` asks for credentials for the origin that answered it; confirming approves exactly that origin plus any the person adds. Userinfo in the address is the person's credentials for that origin and never stays in a stored URL. Addresses on which Android crashes are reported as unusable. `POST /v1/read/preview` runs this for the web client.

### 9.9 Device follow-ups: the discovery ledger

Auto-download, new-episode notifications and similar device effects run from a durable local **ledger** of new releases on each device:

- Entries come only from playable, eligible discoveries, from the device's own refresh or delivered by sync as an admission. Backfill, first population, catalog-only items, initial snapshots, metadata updates and imports never add entries. Entries are deduplicated by generation and playable occurrence and follow merges, so a release discovered twice is handled once.
- Recovery and catch-up compare admission IDs with the device's durable seen set: handled admissions never repeat; a genuinely unseen eligible release delivered in a recovery snapshot is considered. First attachment and import mark existing admissions as seen.
- Auto-download evaluates entries under the device's own settings; in server mode it does not require the episode to be in the Inbox.
- Download effects follow Android: a download request with "Enqueue downloaded" adds to the queue at the device-resolved place (which also dismisses NEW); a completed download dismisses NEW; cancelling a download removes from the queue only if that download's request added it; deleting a download with "Delete removes from queue" removes it. Android's "auto-download queued episodes" path stays.
- Notifications follow the device's per-podcast setting, once per entry.

### 9.10 Auto-download and the Inbox

In server mode the shared Inbox follows only the new-episodes action. With auto-download on and the action set to queue or nothing, new episodes do not land in the Inbox (Android forces them there when auto-download is on).

---

## 10. Capabilities, errors and versioning

### 10.1 Capabilities

| Capability | Contents |
| --- | --- |
| core (required) | accounts and sign-in; subscriptions and their policies and credentials; the catalog; played and unplayed; position; listening sessions; last played; discovery proposals |
| `inbox` | showing `new`; `inbox.dismiss`, `inbox.dismiss_all` |
| `queue` | the queue and its operations; the queue policy |
| `favorites` | `favorite.set` |
| `history` | the History list; `history.remove`, `history.restore`, `history.clear` |
| `statistics` | the statistics screens; `statistics.reset` |

- A client declares its capabilities at sign-in or installation registration; the server records them per installation and rejects newly authored writes outside them. Recovery may retransmit accepted facts of any capability.
- Every operation writes only its own field groups, so a client never overwrites data it does not understand. A core-only client still sends session contributions.
- Core effects on optional data are applied by the server whatever the client declared: starting playback dismisses NEW and queues the episode (resolved by the server, §7.6); completion removes it from the queue and sets history; unsubscribing deletes queue entries, favorites, history and statistics.
- The server sends every record; a client ignores groups it does not show but keeps their source facts. A client without `inbox` shows `new` as unplayed. A client that gains a capability requests an `upgrade` snapshot of its groups.
- The phones implement every capability; the web client implements all of them too.

### 10.2 Versions

- **Protocol version**: an integer. `GET /v1/info` gives the supported range; the client uses the highest common version on every request. Without one, sync pauses with "This server needs updating" or "This app needs updating"; the app keeps working and recording.
- **Operation types** carry `type` and `type_version`; the vocabulary only grows, and the server accepts every type and version any released client produced. An unknown one is `version_unsupported` and not consumed: the client pauses pushing (pulling continues) until the server is upgraded.
- **Records** evolve additively; clients ignore what they do not know. A semantic change to an existing core field needs a negotiated version and a same-lineage projection snapshot for compatible clients.
- **Identity derivations** are frozen per version (§3.3).

### 10.3 Errors

Every response is JSON with a correct HTTP status. Errors are `{"error": code, "message": text, "retry_after"?: seconds, "missing"?: [IDs]}`. A non-JSON response (an HTML error page, a proxy login page, a redirect) is a transient failure, never success.

| Code | HTTP | Client behaviour |
| --- | --- | --- |
| `unauthorized` | 401 | pause sync; ask to sign in again; keep everything |
| `forbidden` | 403 | as above (account disabled, or not an administrator) |
| `lineage_unknown` | 409 | pause; explain in Synchronization settings; keep everything (§11.6) |
| `lineage_mismatch` | 409 | run recovery (§6.11) |
| `dependency_missing` | 409 | send the named facts or secrets first; nothing was consumed |
| `installation_conflict`, `installation_superseded` | 409 | take a new transport installation (§6.10) |
| `account_changed` | 409 | the web tab reloads |
| `setup_complete`, `last_admin`, `username_taken`, `import_exists`, `account_not_empty`, `backup_running` | 409 | explain |
| `view_expired`, `cursor_expired` | 410 | restart the snapshot or view from the beginning; apply nothing staged |
| `payload_too_large` | 413 | reframe (§6.5); never discard a logical action |
| `unsupported_media_type` | 415 | client bug |
| `version_unsupported` | 426 | pause the affected direction; say which side needs updating |
| `rate_limited` | 429 | retry after `retry_after` |
| `server_busy` | 429, 503 | retry after `retry_after` (an import or rebuild holds the account, or capacity is full) |
| `backup_failed` | 503 | explain: an admin backup that ran out of time; nothing was published |
| `insufficient_space` | 507 | explain: the server's disk lacks the free space an import or a backup needs; nothing was written |
| `invalid_request` | 400 | a client bug: log it, retry later, never drop the operation |
| `password_incorrect` | 400 | a wrong current password when changing it or saving the recovery key |
| `passphrase_invalid`, `passphrase_weak` | 400 | a recovery key's passphrase that is not valid text, or too weak (§11.1) |
| `not_found` | 404 | |
| `method_not_allowed` | 405 | |

### 10.4 Push outcomes

| Outcome | Meaning |
| --- | --- |
| `applied` | applied; its effect is at its commit |
| `superseded` | recorded, but a concurrent fact won by the rules |
| `not_applied` | recorded, not applied by rule, with a reason |
| `rejected` | invalid; recorded with a reason; its slot still advanced |
| `duplicate` | a transport status carrying the original, unchanged receipt; never a receipt outcome |
| `op_id_conflict` | a slot consumed by a known `op_id` with different content; no receipt |

---

## 11. Backup, restore and import

### 11.1 Keys and the key bundle

The server's receipt keys live in `receipt.keys` and its sealing key in `secrets.key`, both in the data directory (mode 0600), never in the database or a backup. Key ID `k1`; version 1 never rotates keys, so one key export restores every backup.

`nottennapod-server keys export --data-dir D --out FILE [--passphrase-file F]` writes the **key bundle** `{format: "nottennapod-keys", version: 1, server_id, created_at, kdf: {alg: "argon2id", m, t, p, salt}, nonce, ciphertext}`: the canonical JSON `{server_id, receipt_keys: [{key_id, key}], secrets_key}` sealed with AES-256-GCM under the passphrase's Argon2id key, the canonical header as additional data. `keys install` puts lost keys back from a bundle that belongs to this server and verifies its database. The service page warns until an export exists; a restore records the bundle it opened as one.

**Save recovery key** (the web client's name for the bundle) works while the server runs, for an administrator's web session:

- `POST /v1/admin/keys {password, passphrase}` → `{export_id, file_name: "nottennapod-recovery-key-<UTC yyyyMMdd>.json", bundle, created_at}`, where `bundle` is the bundle file's text, as `keys export` writes it. In order: a body that is not valid UTF-8 or escapes an unpaired UTF-16 surrogate is `400 passphrase_invalid`, so decoding never changes the passphrase (valid escapes, surrogate pairs included, are kept; nothing is trimmed or normalized); a passphrase of fewer than 12 or more than 1024 code points, only whitespace, or equal to the password is `400 passphrase_weak`; at most 10 sealings an hour for the whole server (`429 rate_limited` with `retry_after`, before any Argon2 work); one at a time (`503 server_busy`); the password of the caller's own login (by account ID), checked with the sign-in limiter (§5.5; a wrong one is `400 password_incorrect` and counts as a failed sign-in; a refused password does not use a sealing); sealing outside any transaction; and a final transaction that re-validates the session and the login's credentials version before the bundle is released (`401`/`403`, or `password_incorrect` after a password change).
- Sealing is not saving: nothing is recorded. The export waits in memory for an hour (a restart forgets it). `POST /v1/admin/keys/{export_id}/saved {}`, sent once the person confirms the file is saved, records the export at its `created_at` (never moving the record backwards) and answers `{keys_exported_at}`; a repeat answers the same. An unknown, expired or another account's export is `404 not_found`.
- No bundle, password or passphrase is stored or written to the server's logs; failures are logged with a fixed message.

### 11.2 Backups

- A backup is one consistent read transaction of the whole database: `VACUUM INTO` a private temporary file outside the write lock; the backup marker written into the copy; the copy reopened read-only and checked (`integrity_check`, `foreign_key_check`, the exact schema of its recorded migrations, no object of its own); then compressed and signed, read back as a restore reads it (the MAC first, then the decoder, which must return the checked copy's exact bytes), fsync, rename to `nottennapod-<UTC yyyyMMdd-HHmmss>-<n>.db.gz`, fsync the directory. A crash leaves only temporaries (`.nottennapod-backup-*`), which the next backup into the folder removes before it checks the free space.
- **Compressed and signed (version 2, `.db.gz`)**: the file is two gzip members. The first is the database (deflate, an empty header: no name, comment, extra field or time). The second is empty, and its comment is the trailer: `1f 8b 08 10 00000000 00 ff <JSON> <8 lowercase hex: the JSON's length> "NTPBMAC2" 00 03 00 00000000 00000000`. The JSON (2 to 4096 bytes of printable ASCII, nothing after it, no other field) is `{format: "nottennapod-backup-mac", version: 2, server_id, key_id, size, compression: "gzip", database_size, database_sha256, mac}`: `size` is the first member's bytes (the file's size less the trailer), `database_size` (1 to 2⁴²) and `database_sha256` (lowercase hex) the database's. The backup MAC key of both versions is `HMAC-SHA256(receipt_key, UTF-8("nottennapod backup MAC key"))`, `receipt_key` being the receipt key `key_id` names, and `mac` is its 32-byte MAC in padded standard base64 (RFC 4648 §4), an exception to §3.2's lowercase hex, which sync receipts and decisions keep. The MAC is HMAC-SHA256 under that key over `"nottennapod-backup-mac\0" "2\0" server_id "\0" key_id "\0" size "\0" "gzip\0" database_size "\0" database_sha256 "\0"` followed by the first member's bytes, so every field a reader uses is authenticated. `gunzip -c` returns exactly the database. A reader uses nothing but the fixed framing before the MAC verifies, and decodes only after: the member must end exactly at `size`, its CRC must hold, and its output must be exactly `database_size` bytes with that SHA-256, all before SQLite opens anything. Servers before this version refuse `.db.gz` backups as unsigned.
- **Version 1 (`.db`, written before; still restored)**: the database followed by `<JSON> <JSON length, 4 bytes big-endian> "NTPBMAC1"`, the JSON `{format: "nottennapod-backup-mac", version: 1, server_id, key_id, size, mac}` and `mac` the HMAC-SHA256 under the same backup MAC key over `"nottennapod-backup-mac\0" "1\0" server_id "\0" key_id "\0" size "\0"` followed by the database's `size` bytes, in the same base64. SQLite still opens the file; changing it breaks the MAC. Unsigned files are not restorable.
- **Scheduled**: daily (with jitter); missed days coalesce into one catch-up backup after wake or start; failed attempts back off and do not count. The last **30** verified daily backups are kept, ordered by a durable publication counter, whichever their format (`nottennapod-daily-<counter>-<UTC>.db.gz`, or `.db` before); a daily backup published just before a crash is checked at start with the server's keys (MAC first, then the decoder, on a copy beside it) before it is recorded. On-demand backups (`POST /v1/admin/backup`, or `nottennapod-server backup --data-dir D --out DIR` while stopped) are kept apart and never pruned. A second concurrent backup is `409 backup_running`.
- **Disk safety**, for daily, admin and command-line backups alike: the leftovers of an interrupted backup are removed first; the backup's disk must then have room for a copy of the database plus the headroom (the larger of 1 GiB and twice the database with its write-ahead log), and the data folder's disk the headroom; both disks are checked every second while the backup runs, and each 32 MiB the backup writes while it compresses must leave the headroom; unknown free space refuses the backup. No compression ratio is assumed: incompressible content needs about twice the database, and stops cleanly when it is not there. A backup runs at most 30 minutes, and the compression and MAC passes stop when it is stopped; a stopped or failed backup publishes nothing and never deletes a good one. Too little space for an admin backup is `507 insufficient_space`. While the copy's read is open, everything written meanwhile stays in the write-ahead log, which is cut back afterwards (§14).
- Backups are not encrypted in version 1: they hold listening data and feed URLs, so they belong on a private or encrypted disk. Only the backup folder may be synced elsewhere, never the data directory, and never with the key bundle's passphrase.

### 11.3 Restore metadata and detection

`restore-state.json` in the data directory (never in a backup, never rolled back) holds the server ID, the epoch high-water mark, the incarnation, the database's file identity, the auth revision, an in-progress restore step and the last key export.

At every start, after the lock, `quick_check`, the exact-schema check and migrations, the database counts as **restored** if its incarnation is not the metadata's, it carries a backup marker, its file identity differs, its auth revision is below the metadata's, or the metadata is missing although the database had it. Another server's database (a different server ID) is refused. Every change that removes access (revocation, sign-out, password change or reset, disable, demotion, deletion, a superseded installation's revocation) raises the auth revision in two phases, so a rollback across it is always detected.

The **transition** (for a detected restore, a restore procedure, or lost-timeline evidence in recovery): compute `E = max(high-water mark, highest epoch) + 1` and persist it first; then in one transaction move every account to epoch `E` (head seq and digest unchanged), delete every device token and web session, delete snapshot views and transport scratch state, fail unfinished imports (`interrupted_by_restore`) and release their reservations, clear the backup marker and write a new incarnation. Devices then sign in again (same installation and nonce resume it) and recover once.

### 11.4 Restore procedure

`nottennapod-server restore --data-dir D --from BACKUP --keys BUNDLE` with the server stopped. Each step is recorded before its effects, so a crash resumes it. A refusal in steps 1–2 changes nothing (beyond removing an earlier run's unrecorded staged files, below); a new data directory (missing or empty) is left as it was until they pass, so it is still new afterwards (a compressed backup is checked in temporaries there, `.nottennapod-restore-*`, removed on every path, and at the next restore or start after a crash). Before anything is recorded, the staged copy (`nottennapod.db.restoring.src`) is made from the very file verified and checked against the verified bytes: a backup changed or replaced since is refused the same way, and the run does not read it again (a resumed run copies it again and refuses it if it changed or is missing):

1. verify the backup's MAC with the bundle, then its integrity, schema and server ID (a compressed backup: in an exclusive copy whose digest is the one taken first, authenticated before it is decoded into a second temporary);
2. verify that every sealed secret opens and every receipt, superseded receipt and decision MAC verifies, that facts and decisions are stored with the positions they were signed with, and that every log is a contiguous canonical chain;
3. stage the copy (a compressed one authenticated again with the data directory's keys, then decoded; a version 1 one renamed and its MAC checked and cut off), check its sealed values and signed objects again, and migrate it;
4. run the transition, carrying each account's login (username, password hash, raised credentials version, administrator flag, disabled state) from the pre-restore database when it is readable and belongs to this server; accounts deleted after the backup come back disabled (a username clash gets `-restored`);
5. keep the old database as `nottennapod.db.pre-restore-<UTC>` and swap the staged file in;
6. report the epoch, the accounts restored, those created after the backup (not restorable from it), those returned disabled, and the logins carried.

**Free space**: a restore's own writes never take the data folder's last 1 GiB. Before it verifies anything, it removes the staged files of an earlier run that stopped before recording itself (as a start does); then it is refused, changing nothing, unless the whole restore fits, counted from the backup's own authenticated sizes (not today's database): its copy, its database, and what SQLite writes while it updates the database. A staged database at the current schema needs only room for the transition (64 MiB); an older one is migrated, and a migration that rebuilds a table (0024 rebuilds every stored feed response) writes the new pages into the write-ahead log and again into the file at the checkpoint while the old pages stay allocated, so it is allowed two and a half times the database plus 64 MiB. Every copy and decoding asks before each 32 MiB (never for more than the bytes still to come), and each SQLite step is checked before it starts. Another program taking disk space meanwhile cannot be prevented, by a restore or by any other writer (pushes included): the restore detects it late, by a watch every 50 ms while SQLite runs, at its next copy or decoding check, and by a check after SQLite's checkpoint, and by then SQLite may have written into the reserve. It then stops, removes what it wrote, keeps the database it would replace (and its record, once the restore is recorded), and finishes when run again, or at the next start, once space is freed. The peak is the backup's copy plus its database, or its database plus the update's writes, whichever is larger.

### 11.5 Library import

An existing library reaches the server only by the web client importing one file into an **empty** account: an Android database backup in the reference release's format (NotTennaPod-iOS exports the same) or an OPML file. Phones never upload their library. An empty account has no facts and no reserved or published import (logins and installations do not count).

| Request | Effect |
| --- | --- |
| `POST /v1/import {kind: "android_db" \| "opml", fingerprint, size, file_name}` | reserve in one transaction (`size` 1 byte to 2 GiB, `file_name` 1 to 1,024 bytes); the same fingerprint returns the existing import; otherwise `409 import_exists` or `409 account_not_empty` |
| `PUT /v1/import/{id}/chunks/{n}` | JSON `{data: base64}`, at most 4 MiB decoded, `n` from 0 to 2047; durable and idempotent (a chunk sent again replaces it) |
| `POST /v1/import/{id}/finish` | checks size and SHA-256 fingerprint, then `processing` |
| `GET /v1/import` → `{import: <import> \| null}` (the account's import), `GET /v1/import/{id}` → `<import>` | the import `{import_id, kind, fingerprint, size, file_name, status, reason?, created_at, updated_at, result?}`: status (`uploading`, `processing`, `published`, `failed`), the failure's reason, and the result `{imported: {podcasts, episodes, queue, favorites, history, inbox}, skipped: [{reason, title}], ambiguous, merged, not_carried?}` (`not_carried`, Android backups only: below) |
| `GET /v1/import/{id}/progress` | the only progress: the import without its result, for polling, with while `uploading`, `received: {chunks: [[n, length]…], bytes}` (the chunks held, by index, read with the status under the import's lock); while `processing`, `processing: {phase: reading \| importing \| finishing, done, total}` (podcasts while importing) or `null` when not known (waiting, or resumed after a restart; never zero); once `published`, `refresh: {status, total, attempted, succeeded, failed, until}` (below; counts only with `running`, `done` and `expired`) |
| `POST /v1/import/{id}/cancel` | an `uploading` import becomes `failed` with reason `cancelled`, its chunks are deleted and the account is released (the same file can be reserved again, as a new import); a `failed` one is answered as it is; `processing` or `published`: `409 import_not_uploading` |

- **Decisions**: `import.manifest` (one per import), `import.podcast` (one per podcast: it creates the podcast and generation with the subscription, policy and credentials) and `import.episode` (episode state and baselines). Their IDs are UUIDv5s of the import ID and source IDs, so a resumed import reproduces them. They are signed and recoverable like any decision; a recovered manifest restores the import's row.
- **Accepted files**: an SQLite header and `integrity_check`; `user_version` 1 to 3110000 (newer is refused as made by a newer AntennaPod); columns read by name; older versions upgraded with a port of Android's `DBUpgrader` on a private copy.
- **Mapping**: feed URL → podcast (feeds whose URLs normalize the same become one podcast, the lowest source ID creating it); `Feeds.state` 0, 1, 2 → subscribed, preview, archived (a preview is imported only if one of its rows holds user state, else skipped as `unused_preview`); `keep_updated`; `new_episodes_action` GLOBAL, INBOX, NOTHING, QUEUE → `global`, `inbox`, `nothing`, `queue`; `username`/`password` → a sealed secret approving the feed URL's origin; feed and item metadata → catalog records with no base and the import time as fetch time (the first server fetch replaces them, whole: an episode the feed still lists takes the feed's chapters, or none); an item's stored chapters (`SimpleChapters`, when its `has_simple_chapters` is set: in `id` order, as Android loads them; no rows, or an unset flag, is none) → its metadata's chapters (the backup's rows are applied in publication-date order, undated last, then by source item ID; rows that are one episode take the first row's metadata, with the chapters of the first row that has stored chapters, so a stored list is never dropped for a row without one; between two different stored lists the earlier row's wins); `FeedItems.read` 1, 0, −1 → played, unplayed, unplayed in the Inbox (an import's NEW is state, not a release: no ledger entries or notifications; the first explicit interaction takes it out); `position`; `duration` → measured-duration evidence; `played_duration` → a statistics baseline per media row; `last_played_time` → the baseline's last played; `playback_completion_date` > 0 → a history entry; `Queue` → membership in global queue order (one replay step, §7.6); `Favorites`. Device-only columns and downloads are not imported (§1.4); local-folder feeds (`antennapod_local:`) are skipped and listed. The result's `not_carried` names what a person would miss: `podcasts: [{title, url, source_feed_id, settings: [name…]}]`, per imported feed in feed-ID order (`url` is its address without userinfo, telling same-titled feeds apart), the per-podcast settings that differ from Android's defaults as its cursors read them (`custom_title`, `tags`, `playback_speed`, `skip_silence`, `skip_intro`, `skip_ending`, `volume_adaptation`, `episode_notification`, `auto_delete`, `auto_download`, `auto_download_filter`, `episode_list_filter`, `sort_order`), and `episode_auto_download_off`, the imported episodes not played with a row whose `FeedItems.auto_download` is not set. It is part of the manifest's result. Global policies the backup does not hold start at defaults. Source dates are kept exactly.
- **Episode identity** under the new podcast ID (§3.3); reused GUIDs within a feed follow §9.7 with the rows as evidence; a row with neither GUID nor enclosure gets a persisted `e0:` ID and is listed as ambiguous. Several rows mapping to one episode become one episode: state from the most recently played row; favorites and queue membership the union; queue position the earliest row's; the state row is the episode's baseline, and every other row stays a separate **statistics unit** with its own played time, last played, history and duration, as Android counts it.
- **Baselines** belong to their import decision, so a later reset or history cut covers them exactly; a retry creates nothing new.
- **First population**: an imported generation with items counts as populated, so the first server fetch files only releases newer than the newest imported date; generations without items and every OPML generation stay unpopulated (their first fetch files nothing).
- **OPML**: each `outline` with an `xmlUrl` (nested outlines followed, duplicates once) becomes a subscribed podcast with no episodes.
- **Atomicity**: the import is staged in a private staging database chained from the account's head, outside the main writer; readers and refresh workers never see it. Publication is one transaction that re-checks the account is still empty and reserved, copies an explicit list of library tables, and advances the head. While an import is reserved, sync writes into the account answer `server_busy`. A crash at any step resumes; no partial library is ever visible and no baseline is duplicated. Processing has 30 minutes a try and at most 3 tries, counted across crashes and failures (a server's orderly stop does not count one); a third failed or interrupted try fails the import, and the account is left empty and released, as after any failure. An unreadable file fails the import and releases the reservation; the uploaded file (which may hold credentials) is deleted at publication or failure. Import IDs and fingerprints are kept permanently.
- **Progress**: a poll costs the same whatever the library or the report holds; the report is read once from `GET /v1/import/{id}`. A client continues an upload only if every held chunk has the length its own chunking gives that index; otherwise it cancels and reserves again (chunk boundaries are the client's). `refresh` counts exactly the podcasts the server's schedule fetches at publication (scheduled or awaiting first population), listed from the staged library the publication copies, before it commits; later changes (a first population, keep-updated turned off, an unsubscription) never change the set. `attempted` once one fetch of a podcast ended after publication, `succeeded` once one succeeded, `failed` = attempted − succeeded. `status` is `running`, `done` (every podcast attempted), `expired` (`until`, an hour after publication, passed first: what was not attempted is left to the schedule, never reported as refreshed) or `unavailable` (no counts are given: the counts are kept in memory and not rebuilt after a server restart, or the podcasts could not be listed). Attempts are counted until `until`, an hour after publication; the counts, and with them `refresh` (`done` or `expired` in the second hour), are kept until they are forgotten two hours after publication (at the next poll, publication or periodic sweep). `unavailable` is reported only until `until`; then `refresh` is absent.
- **Order**: export the phone's library, import it on the web, then sign the phones in (first attachment adopts the imported library, §15.4). Changes made on a phone after its export stay in its standalone library.

### 11.6 A server lost without a backup

If an account's log is gone and no backup exists, phones receive `lineage_unknown`: sync pauses with an explanation; nothing local changes. Restoring the server's own backup is the recovery path. Otherwise a person may start a **new lineage** by exporting one phone's server library as a database backup and importing it into the empty account; this is not recovery, and nothing from the old lineage is replayed. Every phone keeps its old server library and unsent changes (inactive, never discarded) and attaches to the new lineage as a first attachment, reusing its downloads; changes missing from the chosen export do not reach it, and no screen claims full recovery.

---

## 12. The read API

The web client keeps no replica: it reads server-computed views and writes facts like any client. Every read endpoint takes a web session bound to its installation, except `GET /v1/diagnostics`, which takes any web session (§5.7); device tokens are refused. Every view response (all of the endpoints below but `frontier`, `preview` and `diagnostics`) carries `head`: `{cursor, log_id, epoch, seq}`, where `cursor` is the view's digest-bearing cursor (§3.7), which a push names, and the other members are its parts. Responses carry no frontier. A fact the browser writes depends on the `provenance` of the records it acts on (§4) and on its author's previous fact (§15.2); a cut depends on the whole frontier at the head of the view it was taken on, read with `GET /v1/read/frontier?cursor=<the view's cursor>`, so its cuts and "observed" lists are exact.

| Endpoint | View |
| --- | --- |
| `GET /v1/read/summary` | navigation counters (queue, Inbox), account policies, the account's installations, import status |
| `GET /v1/read/subscriptions` | subscribed and archived podcasts in Android's order, with counters and `refresh_status` |
| `GET /v1/read/podcasts/{id}` | a podcast's records (credentials shown only as set, with the username) and its statistics card |
| `GET /v1/read/podcasts/{id}/episodes?filter=&sort=` | its episodes with Android's feed filters and sort orders |
| `GET /v1/read/episodes?filter=&sort=` | Android's Episodes screen |
| `GET /v1/read/episodes/{id}` | one episode: metadata (raw show notes), every state record, its podcast |
| `GET /v1/read/queue`, `/inbox`, `/favorites`, `/history` | those lists, in Android's order |
| `GET /v1/read/statistics?from=&to=&include_marked=` | per podcast and per month (§8.8) |
| `GET /v1/read/home?seed=&inbox_sort=&include_marked=` | Android Home's sections (continue listening, new, a seeded random sample, classics) |
| `GET /v1/read/frontier?cursor=` | `{cursor, frontier}`: the sorted causal-node IDs at that cursor's head, for a cut; a cursor of another lineage or epoch, or ahead of the account's head, is `410 view_expired` |
| `POST /v1/read/preview` | feed discovery and a preview of a podcast by URL (§9.8); bounded per account. The preview lists at most 500 of the feed's items, with `item_count` the feed's whole count. `podcast_id` names the account's podcast at the address; when that podcast is removed, `observed` (`{podcast_id, generation_id, state: "removed"}`, its subscription record) is the observation an explicit re-subscribe sends to open a fresh generation (§7.2) |
| `GET /v1/diagnostics` | the account's devices, recent rejections, recoveries, imports and refresh errors (an administrator may name another account); credentials and userinfo redacted; URLs cut to `scheme://host[:port]/…` |

- Filters, sort orders and Home sections follow the reference release exactly. Views read published records only.
- A list view is fixed at one head on its first request and paged with a token bound to that view and its query; a token may name any offset of the view. A view expires (`410 view_expired`) after 10 minutes idle, when its account has more than 64 views (the least recently read first), at once when the account's log lineage or library generation changes (a restore, an import's publication, a rebuild), once its version of the account's library is no longer kept (the server keeps the newest version, the one before it, and the versions of the two most recently read other views read in the last 10 minutes, so a list held open survives the commits a playing phone makes; reading a view's page renews its lease, so a client that holds a view open, such as a selection, reads it at least once within every 10 minutes, and about once a minute keeps it ahead of other windows' views), or when the server drops the account's library after finding it wrong, or no longer holds the account's library: every page is checked against the account's current lineage and generation, and a page made while they changed is not served. The client then loads the list again. A screen's view includes the counters it shows, so one screen is one head. A client applies a response only if its head is at or after the one the screen shows.
- A list answers `{head, view, total, items, next_page_token}`: `total` rows in the view, `items` the page, `view` the list's own fields, `next_page_token` `null` on the last page. `limit` is 1 to 500 rows (default 100); `page_token` reads the view's next page (or any page a token names) with the same query.
- A list's first request may name a window instead of the first page (the episode lists, the podcasts' lists and the queue): `anchor` (an episode ID), `anchor_index` (an index, used when the anchor is no longer a stored episode) and `before` (0 to 200 rows before the anchor). The window starts `before` rows ahead of the place the anchor has in the list (where it would be, if the list does not show it), or ahead of `anchor_index` clamped to the list's length when the anchor has no place: an episode no longer stored, or, in the queue, one not queued. Its response adds `offset` (the index of its first item), `prev_page_token` (the page before it, `null` at the start) and `start_page_token`.
- Home's Surprise: for each subscribed podcast, the eligible episode (new or unplayed, with media, published in the last 712 days, never played or played in the last hour) with the highest score (u − v) mod 2⁶⁴, where u is SplitMix64 of the episode ID's FNV-1a hash and v is SplitMix64 of the seed, ties to the lowest catalog rank; then the first 8 of those by (u₂ − v₂) mod 2⁶⁴ descending, u₂ hashing "surprise order", a 0xff byte and the ID the same way and v₂ = SplitMix64(seed XOR 0x5eed5eed5eed5eed). A seed always gives the same sample of a library; another seed reshuffles it.
- Every read re-checks the caller inside its read transaction.

**Serving the app.** `/` and the app's static paths serve the embedded build of NotTennaPod-Web (`Cache-Control: no-cache` for `index.html`, immutable caching for hashed assets; text files of at least 1 KiB gzip-compressed as JSON responses are); an unknown app path serves `index.html`; unknown `/v1/` paths are JSON 404. Headers: `Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' https: data:; media-src 'self' https: blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'`, `Referrer-Policy: no-referrer`, `X-Content-Type-Options: nosniff`.

**Web client rules** (also §15.6): one transport installation per browser profile and one author per tab load; `install_seq` and `author_seq` allocated inside the IndexedDB transaction that appends the pending-write journal entry; an action is done only once its journal entry is committed, and retired only when a view of the same lineage at or past its receipt's commit has been shown; player ownership by a fenced lease in IndexedDB (renewed every 2 s, lapsing after 10 s), while every tab's saves are journaled and exported whatever the lease; before a cut, every unexported save of the account's journal is exported and the cut depends on them; show notes sanitized with an allowlist (DOMPurify: text formatting, `https`/`mailto` links, `https` images; no scripts, styles, forms, frames, SVG or MathML, same-origin or credential URLs); a completion threshold of 30 s.

---

## 13. The media relay

The browser plays an episode directly when its enclosure is `https:`, its podcast has no credentials and the browser decodes its format. Otherwise, and after a direct-playback error, it plays through the server's **relay**: account- and episode-scoped, never an arbitrary-URL proxy. Phones always fetch directly.

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/media/types` | the capability strings the player asks `canPlayType` about, and the output profiles |
| `POST /v1/media/{episode_id}/play` | the descriptor: `{capabilities, start_ms, direct_failed?, force?}` → `{mode, url, token, type, profile, offset_ms, duration_ms, seekable, representation, reason, message, error}`. `capabilities` maps each MIME type the player asked about to its `canPlayType` answer (`""`, `"maybe"` or `"probably"`; at most 128); `start_ms` is ≥ 0; `force` is absent or `"compat"` |
| `GET`, `HEAD /v1/media/s/{token}` | the media (the session cookie authenticates it); a compatibility token takes `?start_ms=` |
| `GET /v1/media/s/{token}/status` | the out-of-band outcome: `{state: streaming \| complete \| failed, stream_id, error}` |
| `GET /v1/media/{episode_id}/image`, `/chapters`, `/transcript`; `GET /v1/media/podcasts/{id}/image` | artwork, chapters and transcripts under the same discipline |
| `GET /v1/admin/media`, `POST /v1/admin/media/verify` | the conversion check (administrators) |

- **Selection**: `direct` when the browser plays the source type, the URL is `https:`, the podcast has no credentials, neither `direct_failed` nor `force` is set, and direct playback of the source has not failed for the account in the last 24 hours; else, without `force`, `relay` when the browser plays the type; else `compat` (a remux or transcode); else `unsupported` (`no_ffmpeg`, `unknown_format`, `no_profile`, `too_large`, with `reason` and `message`). `force: "compat"` skips both direct playback and the relay (a seek beyond what a non-seekable relay buffered). A direct candidate whose declared type (the enclosure's type, else its extension's) the browser plays is answered `direct` at once, without fetching anything. Otherwise the server pins the representation, and the source type is the probe's (`ffprobe` on the first bytes), else the response's `Content-Type`, else the declared type; with none, the browser's answer is not asked and the type is given as `audio/mpeg`.
- **Failures**: a publisher's or the network's failure while the representation is pinned is still `200`, with `{mode: "failed", error: {error, label, message, code?, retry_after?}}` (`error` is one of the error codes below, `label` Android's download-error label, `message` its wording, `code` the publisher's HTTP status for the `http_status` label, `retry_after` in seconds); only a busy server is an HTTP error (`server_busy`). A `direct_failed` request is remembered for the account's source for 24 hours (in memory, at most 1,000 sources); the web client remembers the episodes whose direct playback failed for as long, and asks for them with `direct_failed`, so a server restart does not make it try them again.
- **Scope**: the episode must be in a subscribed, archived or kept podcast of the account; anything else is `404 not_found`, as an unknown episode. The enclosure is the canonical `metadata.media.url`, else the first occurrence's enclosure.
- **Fetching** follows §9.3: destination rules on every hop, at most 20 redirects, credentials only to approved origins, never the NotTennaPod cookie or token.
- **Ranges are exact and never mixed**: a strong representation (strong `ETag`) from a host with ranges gets exact `206`, `416` and `HEAD`; a representation is bound to the resource its redirects lead to (without volatile signature, expiry and tracking parameters); a change of representation answers `412 media_changed`, never spliced bytes; weak or range-less representations are served one whole upstream response per request with `Accept-Ranges: none`. A representation that needs backward access (no index within the probe budget on a range-less host, a content coding, an unknown length) is spooled to a private file (at most 1 GiB, 2 per server) and served with real ranges.
- **Compatibility** (`compat`): ffmpeg and ffprobe (6 or later, with `aac` and `libopus`) are an optional runtime dependency. A conversion is a stream from a requested media time; the player maps displayed position = `offset_ms` + element time, so positions, saves and contributions stay on the original timeline. ffmpeg reads only from the server's own loopback reader (a per-conversion capability path, never on a command line), never a publisher URL. Profiles: `aac`, `mp3`, `mp4-aac`, `webm-opus`. A run is complete only when ffmpeg exits cleanly with no decode error and reaches the original duration within 2 s.
- **Responses** are only `audio/*` or `video/*` (images `image/*`, never SVG; chapters JSON; transcripts JSON or text), with `X-Content-Type-Options: nosniff`, `Content-Disposition: inline`, `Content-Security-Policy: sandbox; default-src 'none'`, `Cross-Origin-Resource-Policy: same-origin`, and only safe upstream headers.
- **Limits**: 4 streams per account (`429 server_busy`); 2 conversions per server (`503 server_busy`); an account's new conversion replaces its previous one; first byte within 30 s; streaming stops when the browser disconnects. Artwork is at most 10 MiB, chapters and transcripts 5 MiB.
- **Media tokens** are held in memory and bound to the account, the session that asked, the episode, its source and the pinned representation: another session's token is unknown. A token ends after 12 idle hours, when the account holds more than 32 (the least recently used goes), and at a server restart; the player then asks for a new descriptor.
- **Errors** say whose fault they are: `media_unauthorized`, `media_not_found`, `media_refused`, `media_unavailable`, `media_changed`, `server_busy`, each with Android's download-error wording; the status call (which waits up to 2 s for a finishing stream) reports them to the player.

---

## 14. Service operation

- **Storage**: one SQLite database (`nottennapod.db`, WAL, `synchronous=FULL`, `fullfsync`, `foreign_keys`, `trusted_schema=OFF`) per data directory, created 0600; one writer connection (`BEGIN IMMEDIATE`) and a reader pool; an exclusive lock (`server.lock`) so only one process uses a data directory. Every start syncs the data directory and its ancestors. Migrations are numbered, checksummed and applied at start in one transaction; a failure leaves the previous schema and the server refuses to start; a database newer than the binary is refused. Every account-owned table keys on `account_id` with composite foreign keys, so the database refuses cross-account references. Logical text is never truncated; long values are indexed by hash. Operation, session, proposal and import IDs are indexed permanently.
- **Write-ahead log**: SQLite reuses the log once a checkpoint has copied it into the database; whenever it restarts, the file is cut back to 64 MiB (`journal_size_limit`). After an import's publication, a rebuild and a backup's copy, a passive checkpoint on a connection of its own copies what it can of the log into the database. It runs only once the account is no longer busy, so a push meanwhile is never answered `server_busy` because of it; it never takes the writer's lock and never waits for a reader or another checkpoint, so it competes with pushes only for disk I/O; a failure only logs. The peak is not capped: the log still grows to the largest transaction (an import's publication writes about the whole library) or to everything written while a long read (a backup, a phone's snapshot) is open, and is cut back at the first write after the log is all in the database and no read still uses it.
- **Shutdown** at any instant never corrupts data: anything not committed was not acknowledged and is resent.
- **Time**: the server clock is used only for normalization and scheduling. Schedulers check what is due against the wall clock every minute, so work missed during sleep runs soon after waking, spread with jitter; persisted deadlines further ahead than one interval (a quiet podcast's own, §9.6) plus jitter are recomputed after a clock jump.
- **Listening**: plain HTTP only on loopback or a verified Tailscale address (`--allow-insecure-listen` otherwise); HTTPS with `--tls-cert`/`--tls-key` (reloaded on `SIGHUP`; the key must be private to the server's user); the documented default is loopback behind `tailscale serve` with `--public-url`. Host names as §5.6.
- **The service** is installed by an explicit command (`nottennapod-server service install | update | start | stop | restart | status | uninstall`): a launchd agent on macOS, a systemd user unit on Linux, restarted after any failure. `service update FILE` stages and checks the new binary, keeps the previous one and a database copy, swaps, and confirms readiness through `/v1/info` (the new version and this start's `instance_id` from `server.instance`); a version that is not ready and accepted nothing is rolled back, and one that accepted changes is kept. Each step of an update is recorded first and resumable.
- **Files at rest** belong to the server's user, are private (0700/0600, ACL entries removed), are real files and folders on a trusted path, and are never followed through planted symlinks or hard links. Logs are private, rotate at 10 MiB with 3 kept, and never hold credentials or the setup code. The container image's console (`docker logs`) counts as the terminal, not as a log: the server prints the setup code there at each start until setup completes.
- **The service page** (`GET /v1/admin/service`, an administrator's web session) shows the version, uptime, data directory, database size and schema version, the last and next backup and whether the keys were exported (`keys_exported_at`; until then the warning `keys_not_exported`, and the web client offers Save recovery key), the copies never deleted automatically, also with daily backups off (`manual_backups` and `pre_restore`, each `{count, bytes}`: on-demand backups in the backup folder, and the databases restores kept in the data folder with their log files; `backups.used_bytes` is the daily ones), the refresh scheduler, the listen address and HTTPS, and the operating instructions. Everything shown is sanitized. The web client takes an on-demand backup from it ("Back up now").
- **The read model** builds each account's library once and shares it between requests: after a commit, the next read applies only what changed. The libraries of the accounts with an installation seen in the last 30 days are built in the background after a start, after those accounts' queue replays (§7.6), and an account's library, with its list views, is dropped after 10 minutes without reads or to make room: at most 8 accounts hold a library at once, counting an account whose library was dropped while one of its responses is still being made (a read of a ninth account waits for one to end). Memory: an account holds one library in steady state (about 180 MB for 100 podcasts and 50,000 episodes, about 360 MB at 100,000), its later versions sharing what did not change. What it holds beyond that depends on how much changed between the versions it keeps. After small changes (playback, a few episodes) its worst state (two leased lists, two responses being made on older versions, the version before the published one, and a verified build whose comparison is a full build) measured about 2 libraries. After broad changes (a whole library marked played, then back) versions share little: two leased lists alone hold about 4 libraries, and the worst state about 8, its structural bound (the published, previous and two leased versions, two responses' versions, a build and its comparison). For one power user's account of 50,000 episodes that is about 180 MB steady, about 360 MB at a peak after small changes, about 720 MB with two lists held across broad changes, and at most about 1.4 GB. A household's worst case is that times the accounts holding a library (at most 8): eight such accounts at once, at most about 11.5 GB. The first change applied to each account's library after a start is checked against a full build; `POST /v1/admin/read-model/verify {}` (an administrator's web session) checks every library held now the same way and answers `{accounts: {<account_id>: "ok" | "mismatch"}}`. A library found wrong is dropped and built again in full; its list views expire, and its next changes are each checked again. A read that finds no library to serve after several attempts (the account's library dropped or failing again and again), or that is not admitted in time (an account's reads made at once are limited, which bounds the memory they hold), answers `503 server_busy`.
- **Witness**: `make acceptance-witness DATA=<dir>` checks a copy of a stopped server's data directory (integrity, archive and sealed values authenticated with its own keys, every account's digest chain and the rebuild invariant) without writing to it.

Operating instructions are in `docs/OPERATIONS.md`.

---

## 15. Client obligations

### 15.1 Contract

- Pass every case in `contract/vectors/`, including the rule vectors for the projections the client computes locally.
- Integer milliseconds for every time; canonical JSON for hashes; strict base64url.

### 15.2 Recording

- Every change and its outbox fact are one local transaction, through one place in the data layer that emits facts; changes applied from received records go through a path that emits nothing.
- **Every fact depends on its author's previous fact**, so the known frontier stays about one fact per author.
- Effects that depend on device settings are resolved on the device and sent explicitly (§7.1). Undo is a compensating fact with explicit prior values.
- **No credentials in any string**: strip userinfo from every URL and text value before creating a fact; credentials travel only as secrets (§5.8) with `approved_origins` (the feed URL's origin, any origin a discovery reported for a `401`, and those the person approves).
- Every episode reference is a full `EpisodeRef` with occurrence evidence. A subscribe carries `observed` unless the podcast is unknown to the device.
- Cuts export unexported saves first and depend on the whole known frontier; a cut made while a snapshot stages observes the live base plus the overlay. A history restore depends on the removal it undoes.
- `invalid_request` and `dependency_missing` never drop a fact; facts too large for one request are framed.
- Local-folder podcasts never produce facts. A preview produces facts only once registered; a subscription made while the server is unreachable is a durable pending request that completes when the server is reachable, the podcast behaving as a preview meanwhile; registration waits never block unrelated facts or local preview playback, and dependent facts are materialized once the resolution is known, each with the time the person acted and depending on the resolution. A waiting fact recorded in the same action as facts that are not waiting (one queue command naming episodes of both) also depends on all of them, so the action replays as the device executed it.

### 15.3 Playback

- Sessions, claims, checkpoints, ends, finalizations and leaves as §8; save identities on every export; the restart export; claims owed durably; the pending transition stored with the current-episode preference.
- Never seek, pause or replace the playing episode because of sync; adopt remote positions only as §8.6.

### 15.4 Accounts, libraries and copies

- Server mode uses a separate local library per account; switching never touches either library. A phone has one server account: signing in to another account or lineage keeps the earlier server library inactive with its unsent operations until the person removes it. Each library has its own installation ID, outbox and cursor.
- **First attachment** stages a new library from an `attach` snapshot without touching the current one, adopts the account's library whatever it holds (never uploading its own), states that plainly, links verified downloaded files from any retained library (reference-counted, with player pins), and activates only when nothing is playing from the current library.
- Disconnecting returns to the standalone library and keeps the server library and its outbox; reconnecting the same account resumes it. "Export database" in server mode exports the active server library.
- Keep the instance nonce out of backups; run local copy detection at launch (§6.10); handle transport conflicts; persist framing transaction IDs; register a new transport installation before its first push.
- After a restore: `401` means "Sign in again" (keep everything); `lineage_mismatch` runs recovery (§6.11); a later commit carrying a fact you hold with a different receipt replaces yours; keep every decision with its MAC (they are recovery evidence).

### 15.5 Receiving and displaying

- Stage pulls and snapshots, verify them, fetch referenced secrets, and switch atomically (§6.8); keep placeholders; apply merges and mappings without interrupting playback.
- Derive episode IDs with `identity_rules`; keep distinct observed occurrences of a reused GUID locally; never merge or split identities.
- Parse with the reference release's rules and pass the shared feed vectors; send observations and proposals as §9.4–§9.5; file locally at once and converge on the admission; run auto-download and notifications from the ledger (§9.9).
- Remote unsubscriptions delete the podcast's episode state and downloaded files after the current playback of that podcast ends; nothing else is deleted because of received records.
- Pause sync quietly on every error class of §10.3, with a status in Synchronization settings; no dialogs or nags during normal use. Keep a bounded, shareable local sync log without passwords or tokens.
- Sync work runs in bounded chunks at low priority and never starves audio or the interface.

### 15.6 Web client

- Online only: if the server is unreachable it says so and does nothing else.
- Every change is a fact pushed through the durable IndexedDB journal; a browser close before acknowledgement never reports success; a reload loses nothing and pushes the journal first.
- Facts depend on the provenance of the records they act on, and cuts on the frontier at their view's head (§12); `X-NotTennaPod-Account` on every request; responses applied only if their account and head still match.
- After a restore, entries holding an old-lineage receipt are uploaded with it, others pushed under the successor; an entry waiting on a dependency the browser lacks waits with an explanation and is never dropped.
- The rules of §12 for tabs, the lease, cuts and show notes.

---

## 16. Acceptance scenarios

Every implementation must pass these end-to-end scenarios; each checks something a person would notice. Shared conformance data (identity, parser, statistics and rule vectors) is in `contract/vectors/`.

| # | Scenario |
| --- | --- |
| 1 | With the server stopped for three days, a phone plays, streams, refreshes, files, auto-downloads, marks, reorders and favorites; after it returns every device converges and nothing is lost |
| 1a | Two weeks offline, two phones each offline at times (refresh, discover, listen, mark, favorite, reset a position, queue edits, a statistics reset; one episode started on P, played further on C, resumed on P): after both reconnect in either order and are paused, every device shows one state: P's position, the queue in the replay's order, each new episode filed once, downloads and notifications once per eligible phone, statistics equal to the listening less the reset |
| 2 | The server restarts mid-push; the phone retries; nothing is applied twice or lost |
| 3 | A phone launched with the server unreachable and its sign-in expired opens at once and everything local works |
| 4 | A subscription made during an outage completes when the server returns; its preview listening carries over |
| 4a | A phone subscribes during an outage, listens to an episode and queues it; meanwhile another device subscribes to the same feed, resets that episode and removes it from the queue. Once both have exchanged, every device shows the reset position and the episode out of the queue; when the phone's actions were the later ones, the phone's stand |
| 5 | Two phones offline listen to one episode; once both reconnect and are paused, the position is the one where play was pressed last; statistics count both |
| 5a | A starts Monday and pauses; B plays Tuesday to 60%; A resumes Wednesday to 30%: every device resumes at 30%; statistics count one session each |
| 6 | A browser tab left on an old session keeps sending checkpoints; the phone's newer listening is never rewound |
| 6a | A browser's answer that renewed its session or registered its successor arrives late, after a newer session cookie (a handoff, another tab's repeated registration, signing out and in) or after signing out (also when a successor's first use overlaps the sign-out): the browser is signed in with the newer session, or stays signed out. Copied browser profiles are excluded (§5.4) |
| 7 | An episode paused 10 s from the end on A is replayed from the start on B; A then starts another episode; it is not marked played |
| 8 | A removes an episode from the queue while offline B moves it: it is out |
| 8a | A pauses at minute 10; B continues to 30; A adopts 30 and resumes to 45: every device resumes at 45; statistics count 10→30 on B and 30→45 on A only |
| 8b | B takes over from A's paused session before A's lazy finalization in the deterministic order, without observing it, in both delivery orders: unplayed at B's position and queued |
| 8c | Two offline phones each reset statistics after listening: neither's listening before its own reset counts |
| 8d | Start, rewind before the session's start, switch: the same played time as Android |
| 9 | A clears the queue while offline B adds an episode: B's episode remains |
| 9a | Adds, moves and sorts on two phones that cannot see each other give, in every arrival order and request split, the order one device would have made: X back Monday and Y back Tuesday give …, X, Y (front: Y, X, …); an add of an already queued episode is ignored |
| 10 | A unsubscribes while offline B listens to one of its episodes: the podcast is gone everywhere; B's listening is recorded, not applied |
| 11 | "Remove all from Inbox" on A does not dismiss an episode filed concurrently by B |
| 12 | A favorite added on A and removed on offline B converge to the later action |
| 13 | Undo of a mark played restores the previous state and position everywhere |
| 14 | Two phones and the server discover one new episode while disconnected: filed once, as the first admission says; explicit actions survive |
| 15 | A podcast whose action is "queue" files new episodes at the account's enqueue location, once |
| 16 | Subscribing never files existing episodes; an older episode added later (backfill) is not filed |
| 17 | A bulletin feed reusing `/latest.mp3` with new GUIDs and the same title makes a new episode each time; a hosting move changing every GUID but keeping titles and dates keeps listening state |
| 17a | "Enqueue downloaded" off never queues; backfill never auto-downloads or notifies; an item that gains audio later is filed once |
| 17b | A phone that reaches a feed the server cannot corrects a title for every device |
| 18 | A publisher changes all GUIDs: episodes merge only where the rule holds; state is kept |
| 19 | A feed moves with a permanent redirect: same podcast ID, aliases and state; no duplicate |
| 20 | The server is restored from a week-old backup while three devices hold newer changes, one only received by another: after recovery everything converges and nothing is lost |
| 21 | The server is replaced by an empty one with no backup: no phone is emptied; sync pauses with an explanation; importing phone A's export starts a new lineage; A attaches without re-downloading; B keeps its old library and attaches as a first attachment; nothing counts twice; no screen claims full recovery |
| 22 | A phone backup restored onto a new phone: both run; the copy takes a new installation; nothing is lost or applied twice |
| 23 | First attachment to a non-empty account adopts the server's library, reuses downloads and leaves the standalone library unchanged; disconnecting returns to it |
| 24 | Importing an Android backup, and the same library exported by NotTennaPod-iOS, into an empty account reproduces subscriptions, policies, credentials, play states, positions, queue, favorites, Inbox, history and statistics (Android's numbers); the same file again returns the same import; a different import is refused; OPML imports subscriptions only |
| 25 | A client without the queue capability plays an episode: it is queued and removed on completion |
| 26 | A newer app sends an operation the server does not know: sync pauses it without losing it; it applies after the server is upgraded |
| 27 | Two accounts on one server never see each other's data, also through shared feed URLs |
| 28 | For each action of AntennaPod's workflow (play, pause near the end, skip with and without "Keep skipped", menu and swipe mark played, mark unplayed, reset position, remove from Inbox, download with "Enqueue downloaded", archive, unsubscribe, clear history, reset statistics) every other device shows what Android shows on the device where it happened |
| 29 | Catch up a many-week backlog while playing, seeking, downloading and editing the queue; bounded staging; crash between every step; cursor and overlay stay consistent |
| 30 | Restore before a manual played mark later hidden by a stale completion; recover from a phone that only received it; the mark survives |
| 31 | Finish normally, then replay without marking unplayed: played remains; a takeover or explicit reset ordered before a lazy completion suppresses it in both network orders |
| 32 | Restore before remove and Undo, a multi-field mark, a first admission, a podcast creation, an occurrence correction and a merge; recover from two replicas in both orders with crashes between frames: every effect and barrier survives exactly once |
| 33 | Copy an actively listening library after checkpoint 5; both continue past checkpoint 6 at different positions: the copy forks before writing (and with the server-side fallback); the shared prefix counts once and both continuations count |
| 34 | Two offline phones meet two reused-GUID releases before the dispute is known: each release keeps its own state and files; a removed survivor merged with a live source rejects late closed-generation facts without suppressing the live source |
| 35 | Concurrent sorted adds of B and C between A and Z converge A, B, C, Z (also for a server discovery and a queue-less client); metadata alone never reorders; thousands of nested keys stay strictly inside their gaps and distinct |
| 36 | Reset at contribution 20, rewind to 10, reset again, advance to 15: 5; independent cuts union in every order; imported baselines reset and never come back on retry |
| 37 | Listen 0→10, restart, rewind to 5 and save: total 10; pause near the end, relaunch, start another episode: lazy finalization completes once; adopting a remote position adds no listening or history |
| 38 | A 200-episode first population and old backfill make no notifications or downloads; a proven no-media→media transition qualifies once; an unseen release in a recovery snapshot drives the ledger once, a first-attachment snapshot does not; queuing an old episode still triggers queue auto-download |
| 39 | Import a large backup with a crash before and after each part and publication: no partial library, no duplicated baseline, local-folder feeds listed, concurrent imports cannot combine |
| 40 | Serve an HTTP-only and an authenticated enclosure through the HTTPS web app; seek by range; reconnect after a partial response; no credentials reach another origin or account; unsupported formats use the compatibility path on the same timeline; malicious show notes cannot run scripts or send credentialed requests |
| 41 | A remote unsubscribe during playback: audio continues from its pinned file; state hides at once; cleanup after release; a library switch during playback keeps checkpoints in the originating library |
| 42 | Missing dependencies, unknown versions, an oversize bulk command, a rejected operation and a copied transport counter never drop unrelated facts; framing and recovery resume without gaps or repeated effects |
| 43 | A exits paused at 10; B reaches 30; A relaunches with and without pulling first: passive restoration emits nothing; the shared position stays 30; A's deliberate resume adopts 30 |
| 44 | A phone signs in while its standalone player plays 10→40 and completes: the server library activates only after playback stops; the session stays standalone; deleting one library's reused download keeps the other's file |
| 45 | P's URL changed to Q's alias, and P and Q racing for one unowned URL: an explicit, logged conflict; both identities survive; no merge and no playback interruption |
| 46 | A hosting move `(G1, U1)`→`(G2, U2)` with matching title and exact date keeps identity; competing candidates or missing titles or dates do not merge; one GUID and date at two signed URLs with 20- and 23-minute durations stays one episode; different enclosures with dates more than 7 days apart are distinct releases; missing dates or duration-only differences never split |

Run reducers after each prefix as well as after the whole exchange; vary arrival order, duplicate delivery, transport identity, crash points and restore points; keep the original decision receipts when comparing permutations (a different first admission may legitimately choose a different filing).
