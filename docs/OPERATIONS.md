# Operations

How to run NotTennaPod-Server for a household: as a service, reached over Tailscale, with daily backups. The rules behind each step are in [PROTOCOL.md](PROTOCOL.md) §11 and §14.

## First start

The browser app comes from NotTennaPod-Web; without it the server shows only a placeholder page. With NotTennaPod-Web cloned next to this checkout and Node 24 or later installed ([README](../README.md#development)):

```sh
(cd ../NotTennaPod-Web && npm ci && npm run build && npm run install-into-server)
make build
.work/bin/nottennapod-server --data-dir <data-dir>
```

On first start the server logs a setup code and writes it to `setup-code` in its data directory. Open the server in a browser, enter the code and create the first administrator. The server then deletes the file.

Without `--data-dir` the server uses `~/Library/Application Support/NotTennaPod-Server` on macOS and `$XDG_DATA_HOME/nottennapod-server` (or `~/.local/share/nottennapod-server`) on Linux. Only one server may use a data directory at a time.

## Reaching the server

The server listens on `127.0.0.1:8080` by default. Plain HTTP is allowed only on loopback or on this machine's verified Tailscale address; anything else needs HTTPS or `--allow-insecure-listen`.

The recommended setup is loopback behind Tailscale's HTTPS:

```sh
tailscale serve --bg http://127.0.0.1:8080
nottennapod-server --public-url https://<machine>.<tailnet>.ts.net …
```

`tailscale serve status` shows the setup and `tailscale serve --https=443 off` removes it. Alternatively serve HTTPS directly with `--tls-cert` and `--tls-key` (the key must be readable only by the server's user; send `SIGHUP` to reload a renewed certificate).

The server answers only known host names: loopback, the listen address, this machine's Tailscale names, `--public-url`'s host, and names added with `--allow-host NAME` (repeatable), for example a LAN name.

## Running as a service

```sh
nottennapod-server service install --data-dir <data-dir> --public-url https://<machine>.<tailnet>.ts.net
nottennapod-server service status
```

`service install` copies the binary to a stable place (macOS `~/Library/Application Support/NotTennaPod/bin/`, Linux `~/.local/lib/nottennapod/`), resolves every path, and installs a launchd agent (macOS) or a systemd user unit (Linux) that starts at login and restarts after any failure. It accepts only a data directory that holds a server database; `--new-server` starts a new one in an empty directory. `service start`, `stop`, `restart` and `uninstall` manage it; `uninstall` leaves the data, backups and keys in place. Logs go to a private, rotated log directory.

## Keys

The server's receipt keys and sealing key live in the data directory, never in a backup. Export them once, and keep the file and its passphrase somewhere other than the backups:

```sh
nottennapod-server service stop
nottennapod-server keys export --data-dir <data-dir> --out <keys-file>
nottennapod-server service start
```

Version 1 never rotates keys, so one export serves every backup. A backup cannot be restored without it. `keys install` puts lost keys back from the bundle. The service page warns until the keys have been exported.

## Backups

- The server backs itself up daily (`--backup-time`, default 03:30 local time, or `off`) into `<backup-dir>/daily` and keeps the last 30 verified backups. A day missed while the machine slept is caught up once after waking.
- `--backup-dir` defaults to `<data-dir>/backups`. Point it at a folder another program copies off the machine (an external disk, a synced folder) to survive losing the machine; never sync the data directory itself, which holds the keys.
- An administrator can take a backup at any time from the web client's service page; the command `nottennapod-server backup --data-dir D --out DIR` does the same while the server is stopped.
- Backups are not encrypted: they hold listening data and feed URLs, so keep them on a private or encrypted disk.

## Restoring

```sh
nottennapod-server service stop
nottennapod-server restore --data-dir <data-dir> --from <backup-file> --keys <keys-file>
nottennapod-server service start
```

The restore verifies the backup's signature and every key first and changes nothing if anything fails. It keeps the previous database as `nottennapod.db.pre-restore-<time>`, carries current logins forward, and reports what it restored. Afterwards every device is asked to sign in again and then recovers what the backup lacked; nothing on the phones is lost. Accounts created after the backup are not in it.

## Updating

```sh
nottennapod-server service update <new binary>
```

The update checks the new binary, keeps the previous binary and a copy of the database, swaps, restarts and waits for the new version to answer. If it does not, and it accepted no changes, the previous version is put back. `service update` without a file finishes an interrupted update. The installed binary is never overwritten in place.

## Browser media

The web client plays most episodes directly. Episodes in formats a browser cannot play (chiefly Ogg in Safari) are converted with ffmpeg and ffprobe 6 or later, with the `aac` and `libopus` encoders: `brew install ffmpeg` on macOS, the distribution's `ffmpeg` package on Linux. The server finds them on `PATH` or through `--ffmpeg` and `--ffprobe`, and checks every conversion at start (`GET /v1/admin/media`). Without them, such episodes are reported as unsupported.

## Checking a data directory

`make acceptance-witness DATA=<dir>` checks a copy of a stopped server's data directory without writing to it: database integrity, the archive and sealed values authenticated with the directory's own keys, every account's log digest chain and the rebuild invariant.
