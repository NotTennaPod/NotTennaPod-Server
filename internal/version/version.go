// Package version names this build of the server. Make sets it at link time
// (-X github.com/NotTennaPod/NotTennaPod-Server/internal/version.Version=…); `go build` and tests
// leave it "dev".
package version

// Version is the build's version, reported by --version and /v1/info.
var Version = "dev"
