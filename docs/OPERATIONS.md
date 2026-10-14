# Operations

How to run NotTennaPod-Server for a household: as a service, reached over Tailscale, with daily backups. The rules behind each step are in [PROTOCOL.md](PROTOCOL.md) §11 and §14. To run it in a container instead, see [Running in Docker](#running-in-docker).

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

FFmpeg 8.0.x (for example Ubuntu 26.04's 8.0.1) logs a spurious Opus parser error at the end of every WebM Opus file ([FFmpeg #20954](https://code.ffmpeg.org/ffmpeg/ffmpeg/issues/20954), fixed in 8.1), so the check leaves WebM Opus out and reports conversion as degraded. Check that the service page shows the media check as `ready`; if it does not, install FFmpeg 8.1 or later. The container image's build fails unless its ffmpeg is `ready`.

## Checking a data directory

`make acceptance-witness DATA=<dir>` checks a copy of a stopped server's data directory without writing to it: database integrity, the archive and sealed values authenticated with the directory's own keys, every account's log digest chain and the rebuild invariant.

## Running in Docker

The container image runs the same server under [s6-overlay](https://github.com/just-containers/s6-overlay) on LinuxServer.io's Alpine base, with ffmpeg included, for `linux/amd64` and `linux/arm64`. `make docker` builds it ([README](../README.md#development)). Everything that changes lives in `/config`; the container itself is disposable.

**Building.** `make docker` runs BuildKit in a container of its own, the `ntp-utc` builder (Docker's `docker-container` driver with `TZ=UTC`, created on first use and kept for its cache; `DOCKER_BUILDER=` names another). Every timestamp in the image, its configuration, history and build provenance, is therefore in UTC whatever the build machine's time zone; Docker's default builder and settings are not changed. Publish only images built this way. `docker buildx rm ntp-utc` removes the builder and its cache.

### Quick start: the host's Tailscale

The default deployment keeps the server on the host's loopback, with host networking, and lets the host's own `tailscale serve` provide HTTPS, as in a native install. It needs a Linux host with Tailscale, and MagicDNS and HTTPS certificates enabled in the tailnet. Edit [`docker-compose.yml`](../docker-compose.yml) (`PUID` and `PGID` of the user who owns `./config`, `TZ`, `PUBLIC_URL`), then:

```sh
docker compose up -d
sudo tailscale serve --bg http://127.0.0.1:8080
docker logs nottennapod        # the setup code
```

Open `https://<machine>.<tailnet>.ts.net` and enter the setup code. The server needs nothing from Tailscale inside the container: loopback is always allowed, and `PUBLIC_URL` names the host browsers use.

- If HTTPS port 443 is taken on the host, use another: `sudo tailscale serve --bg --https=<port> http://127.0.0.1:8080` and `PUBLIC_URL=https://<machine>.<tailnet>.ts.net:<port>`.
- If local port 8080 is taken, set `LISTEN_ADDRESS=127.0.0.1:<port>` and point Serve at that port.
- `tailscale serve status` shows the setup and `sudo tailscale serve --https=<port> off` removes it. Changing Serve needs root or the host's Tailscale operator user.

With Docker's bridge network instead, the default `LISTEN_ADDRESS=127.0.0.1:8080` is unreachable from outside the container, and `-p 8080:8080` serves nothing; the start log says so. Use host networking, or one of the [other setups](#other-networking).

### The setup code

On a new `/config` the server prints the setup code to the container's console, which `docker logs nottennapod` shows; the console counts as the terminal, not as a log ([PROTOCOL](PROTOCOL.md) §5.1). Until setup completes the code is also in `/config/data/setup-code`:

```sh
docker exec nottennapod cat /config/data/setup-code
```

Export the keys right after setup ([Commands](#commands)); the service page warns until you do.

### `/config`

| Path | Contents |
| --- | --- |
| `/config/data` | the server's data directory: the database, the keys, imports, the media spool and snapshot views; private (0700/0600) |
| `/config/backups` | daily backups (`daily/`, the last 30) and backups taken from the service page |
| `/config/export` | key bundles from `keys export`, offline backups, and the files you bring in for `restore` |
| `/config/tls` | optional: `cert.pem` and `key.pem` (0600) for the server's own HTTPS |

- `/config` must be a local folder, not NFS or SMB: the server relies on file locks and SQLite's WAL.
- At every start the container gives `/config/data`, `/config/backups` and `/config/export` to `PUID`:`PGID`, takes group and other write access away from `/config`, makes the server's folders 0700, and logs what it changed. If it cannot, for example on a read-only mount, the container stops with the reason.
- **Backing up the host:** copy `/config/backups` and `/config/export`. Never copy `/config/data` while the container runs: it holds a live database and the raw keys. The supported backup is `backups/` together with the exported key bundle, which you keep somewhere else.
- `BACKUP_DIR` can point at another volume, for example `-v /mnt/usb/nottennapod:/backups` with `BACKUP_DIR=/backups`. The container fixes its ownership in the same way.
- Logs go to the console (`docker logs`); there is no log folder.

### Parameters

| Parameter | Default | Function |
| --- | --- | --- |
| `-e PUID=1000` `-e PGID=1000` | 911 | the user and group that own `/config` and run the server |
| `-e TZ=Etc/UTC` | UTC | local time for `BACKUP_TIME` and the logs |
| `-e UMASK=022` | 022 | only helper files; the server's own files are always private |
| `-e PUBLIC_URL=https://…` | — | the origin browsers use (`--public-url`); required behind `tailscale serve` or a proxy |
| `-e ALLOW_HOSTS=a,b` | — | more host names to answer (`--allow-host` each), such as a LAN name |
| `-e LISTEN_ADDRESS=` | `127.0.0.1:8080` | the address inside the container (`--listen`) |
| `-e ALLOW_INSECURE_LISTEN=true` | false | allow plain HTTP on an address other than loopback (`--allow-insecure-listen`) |
| `-e TLS_CERT=` `-e TLS_KEY=` | — | serve HTTPS directly (`--tls-cert`, `--tls-key`) |
| `-e BACKUP_TIME=` | `03:30` | the daily backup's local time (±15 min), or `off` |
| `-e BACKUP_DIR=` | `/config/backups` | backups on another volume |
| `-e REFRESH_INTERVAL=` | `1h` | how often each podcast is refreshed |
| `-e DOCKER_MODS=` `-e FILE__<VAR>=` | — | LinuxServer.io [docker mods](https://docs.linuxserver.io/general/container-customization/) and variables read from files |
| `--network host` | — | the default setup: the server on the host's loopback, behind the host's `tailscale serve` |
| `-p 8080` | — | only with bridge networking and a non-loopback `LISTEN_ADDRESS` |
| `-v /config` | — | everything that changes |

The data directory is always `/config/data`. The log folder, the development options, Tailscale detection and the ffmpeg paths are not exposed: ffmpeg is on `PATH`.

### Commands

`nottennapod-server` inside the container runs the server's commands. For `backup`, `keys export`, `keys install` and `restore` it takes a maintenance lock (a second such command is refused at once), stops the server, runs the command as the server's user with `--data-dir /config/data`, and then starts the server again, whatever the command's outcome. A server an administrator had stopped (`s6-svc -d`) stays stopped, and the command says so.

```sh
docker logs nottennapod                                    # the version, the setup code, warnings
docker exec nottennapod cat /config/data/setup-code       # the code again, until setup completes
# Key export: once, then copy the file off the machine and delete it here
docker exec -it nottennapod nottennapod-server keys export --out /config/export/<keys-file>
# Backup: from the web client's service page (no downtime), or offline:
docker exec nottennapod nottennapod-server backup --out /config/export
# Restore in place (the server is stopped and started again):
docker exec -it nottennapod nottennapod-server restore --from /config/backups/daily/<backup> --keys /config/export/<keys-file>
docker exec -it nottennapod nottennapod-server keys install --keys /config/export/<keys-file>
```

- The passphrase is typed at the prompt, so use `-it`. Scripts can give `--passphrase-file /config/export/<file>` instead: make it mode 0600 and delete it afterwards.
- Files copied into `export/` from the host must be readable by `PUID`.
- `nottennapod-server service …` is not used in Docker: the container runs and restarts the server, and an update replaces the image.
- A renewed certificate is reloaded with `docker exec nottennapod s6-svc -h /run/service/svc-nottennapod`.

### Restoring onto an empty `/config`

A new `/config` that has been started once holds a new server with its own keys, and a backup of another server can no longer be restored there. On a new machine, restore **before the first start**, with a one-off container:

```sh
docker run --rm -it --user <PUID>:<PGID> --entrypoint nottennapod-server -v <host-config-dir>:/config \
  nottennapod-server:<tag> restore --from /config/export/<backup> --keys /config/export/<keys-file>
docker compose up -d
```

Put the backup and the key bundle in `<host-config-dir>/export` first, owned by `PUID`. The server then starts with the restored library and no setup code.

### Moving a native install into Docker

No restore is needed:

1. Stop and uninstall the service (`nottennapod-server service uninstall` keeps the data).
2. Copy the stopped data directory to `<host-config-dir>/data` (`cp -a`, no links).
3. Start the container; it fixes the ownership.

### Updating and rolling back

The new image runs the migrations at start; an older image refuses a migrated database.

1. Before updating, take a backup from the service page and note the current tag.
2. Pull or build the new image, then `docker compose up -d`. The service page shows the new version.

To go back, stop the deployment first (`docker compose down`), run the old tag in a one-off container with `restore --from <that backup> --keys <keys-file>`, as above, and start the old tag. Keep the key bundle and its passphrase at hand. Devices sign in again and send what the backup lacked.

### Stopping

The server finishes its work before it exits: HTTP requests, feed fetches (up to 2 minutes), imports and backups. Give it time: the compose example sets `stop_grace_period: 2m`; with `docker run`, use `--stop-timeout 120`. A forced stop does not corrupt anything, but it cuts work short.

### Other networking

The server allows plain HTTP only on loopback. Each setup below sets `PUBLIC_URL`, which gives the browser origin, Secure cookies and an allowed host name.

**A. A Tailscale sidecar**, for a host without Tailscale or a separate tailnet machine. The server shares the sidecar's network and stays on loopback; the sidecar's Serve configuration, [`docker/tailscale/serve.json`](../docker/tailscale/serve.json), gives it HTTPS:

```yaml
services:
  tailscale:
    image: tailscale/tailscale:latest
    hostname: nottennapod
    environment:
      - TS_AUTHKEY=${TS_AUTHKEY}          # from .env
      - TS_STATE_DIR=/var/lib/tailscale
      - TS_SERVE_CONFIG=/serve/serve.json
    volumes:
      - ./tailscale:/var/lib/tailscale
      - ./docker/tailscale:/serve:ro     # the folder, not the file
    restart: unless-stopped
  nottennapod:
    image: nottennapod-server:local
    network_mode: service:tailscale
    environment:
      - PUID=1000
      - PGID=1000
      - PUBLIC_URL=https://nottennapod.<tailnet>.ts.net
    volumes:
      - ./config:/config
    stop_grace_period: 2m
    restart: unless-stopped
```

It uses Tailscale's userspace networking. If Serve does not work in that mode, use kernel mode: `TS_USERSPACE=false`, `devices: [/dev/net/tun]` and `cap_add: [net_admin]`.

**C. The server's own HTTPS**, with a certificate from `tailscale cert` or a LAN certificate authority:

```sh
docker run -d --name nottennapod -p 8443:8443 -v <host-config-dir>:/config \
  -e PUID=1000 -e PGID=1000 -e LISTEN_ADDRESS=0.0.0.0:8443 \
  -e TLS_CERT=/config/tls/cert.pem -e TLS_KEY=/config/tls/key.pem \
  -e PUBLIC_URL=https://<name>:8443 --stop-timeout 120 nottennapod-server:<tag>
```

The key must belong to `PUID` with mode 0600. After renewing the certificate, reload it with `s6-svc -h` ([Commands](#commands)). Other names clients use go in `ALLOW_HOSTS`.

**D. A reverse proxy (opt-in, plain HTTP inside Docker).** Caddy, Traefik or SWAG on a user-defined network forwards to the server with `LISTEN_ADDRESS=0.0.0.0:8080`, `ALLOW_INSECURE_LISTEN=true` and `PUBLIC_URL=https://<name>`, with no `-p` (or `-p 127.0.0.1:8080:8080` for a proxy on the host). The server logs a warning and shows it on the service page: any container on that network can read the traffic, passwords included. The server ignores `X-Forwarded-For`, so per-address sign-in limits apply to the proxy's address (per-username limits still apply). As a last resort, the same settings without a proxy and with `ALLOW_HOSTS=<lan name>` serve plain HTTP to the LAN, with the same warning.

### Advanced: non-root and read-only containers

Both are supported, as in LinuxServer.io images ([non-root](https://docs.linuxserver.io/misc/non-root/), [read-only](https://docs.linuxserver.io/misc/read-only/)): `user: <uid>:<gid>` in the compose file, or `read_only: true` with `tmpfs: [/run:exec]`. The container then skips its `PUID`/`PGID` handling and ownership fixes, so `/config` must already belong to the user the server runs as (`<uid>`, or 911 in read-only mode). The server refuses anything else and says why.
