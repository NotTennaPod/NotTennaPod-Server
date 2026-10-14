# syntax=docker/dockerfile:1
# NotTennaPod Server: the container image (docs/OPERATIONS.md, "Running in
# Docker"). Build it with `make docker`; README "Development" explains the
# options.
ARG ALPINE_VERSION=3.24
# The NotTennaPod-Web commit embedded in the image: bumped deliberately, in a
# Server commit, after the pairing is tested.
ARG WEB_COMMIT=9c8f0787a0c49cff04113af18cecb6ef3538fa34

# Web source: the pinned commit. While the repository is private, BuildKit
# fetches it with the pre-defined GIT_AUTH_TOKEN build secret (`make docker`
# passes it when GIT_AUTH_TOKEN is exported); the token never reaches an ARG,
# an ENV, the URL, a file or a layer.
# Local override: --build-context web-src=<NotTennaPod-Web checkout>
FROM scratch AS web-src
ARG WEB_COMMIT
ADD --keep-git-dir=false https://github.com/NotTennaPod/NotTennaPod-Web.git#${WEB_COMMIT} /

# Web client: architecture-independent, built on the build machine's own platform.
FROM --platform=$BUILDPLATFORM node:24.21-trixie-slim AS web
WORKDIR /web
COPY --from=web-src package.json package-lock.json tsconfig.json ./
RUN npm ci --no-audit --no-fund
COPY --from=web-src scripts ./scripts
COPY --from=web-src src ./src
# dist/: what `npm run install-into-server` would copy.
RUN npm run build

# Server: cross-compiled per target, the web client embedded, the version
# stamped as `make build` does.
FROM --platform=$BUILDPLATFORM golang:1.27.1-alpine${ALPINE_VERSION} AS server
ARG TARGETOS TARGETARCH SERVER_VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY --from=web /web/dist/ ./internal/webui/dist/
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH
RUN go build -trimpath -ldflags "-X github.com/NotTennaPod/NotTennaPod-Server/internal/version.Version=${SERVER_VERSION}" \
      -o /out/nottennapod-server ./cmd/nottennapod-server \
 && go test -c -o /out/media.test ./internal/media

# Runtime: LinuxServer.io's Alpine base (s6-overlay, the abc user, PUID/PGID,
# docker-mods).
FROM ghcr.io/linuxserver/baseimage-alpine:${ALPINE_VERSION} AS runtime
ARG ALPINE_VERSION BUILD_DATE SERVER_VERSION=dev SERVER_REVISION WEB_COMMIT
# The base's own labels (its maintainer, revision, date and links) are replaced.
LABEL build_version="NotTennaPod Server ${SERVER_VERSION} (web ${WEB_COMMIT}) Build-date: ${BUILD_DATE}" \
      maintainer="NotTennaPod" \
      org.opencontainers.image.title="NotTennaPod Server" \
      org.opencontainers.image.description="NotTennaPod's self-hosted sync server and browser client" \
      org.opencontainers.image.authors="NotTennaPod" \
      org.opencontainers.image.vendor="NotTennaPod" \
      org.opencontainers.image.url="https://github.com/NotTennaPod/NotTennaPod-Server" \
      org.opencontainers.image.documentation="https://github.com/NotTennaPod/NotTennaPod-Server/blob/main/docs/OPERATIONS.md#running-in-docker" \
      org.opencontainers.image.source="https://github.com/NotTennaPod/NotTennaPod-Server" \
      org.opencontainers.image.version="${SERVER_VERSION}" \
      org.opencontainers.image.revision="${SERVER_REVISION}" \
      org.opencontainers.image.ref.name="${SERVER_VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.base.name="ghcr.io/linuxserver/baseimage-alpine:${ALPINE_VERSION}" \
      org.opencontainers.image.licenses="GPL-3.0-or-later" \
      nottennapod.web.revision="${WEB_COMMIT}"
# Stop the container, rather than start the server, when init fails.
ENV S6_BEHAVIOUR_IF_STAGE2_FAILS=2
RUN apk add --no-cache ffmpeg mailcap \
 && printf 'NotTennaPod Server %s (web %.7s)\nBuild-date: %s\n' "$SERVER_VERSION" "$WEB_COMMIT" "$BUILD_DATE" > /build_version
COPY --from=server /out/nottennapod-server /app/nottennapod-server/nottennapod-server
COPY LICENSE NOTICE /app/nottennapod-server/
COPY LICENSES /app/nottennapod-server/LICENSES
COPY docker/root/ /
EXPOSE 8080
VOLUME /config

# Gate: this image's ffmpeg must pass the server's conversion check, on every
# architecture (13 sources x 4 profiles ready); otherwise the build fails.
FROM runtime AS media-check
COPY --from=server /out/media.test /tmp/media.test
RUN NTP_REQUIRE_FFMPEG=1 /tmp/media.test -test.count=1 -test.v \
      -test.run '^(TestVerifyMatrix|TestVerifyOnANewDataDirectory)$' && touch /media-verified

FROM runtime
COPY --from=media-check /media-verified /app/nottennapod-server/.media-verified
