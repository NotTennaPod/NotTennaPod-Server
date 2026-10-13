// Package listen decides where the server may listen and serves HTTPS when
// given a certificate (PROTOCOL §14). Plain HTTP is allowed only on loopback,
// or on an address this machine's Tailscale reports as its own; anything else
// needs TLS or an explicit
// --allow-insecure-listen.
package listen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/private"
)

// Rules are what the address is checked against.
type Rules struct {
	TLS           bool
	AllowInsecure bool
	// Tailscale is the tailscale command to ask: an absolute path that
	// service install verified and recorded (FindTailscale). Empty finds and
	// verifies it now.
	Tailscale string
	// TailscaleAddrs lists this machine's Tailscale addresses; nil asks the
	// tailscale command. An error means they cannot be verified.
	TailscaleAddrs func(ctx context.Context) ([]netip.Addr, error)
}

// tailscaleRanges are the addresses Tailscale assigns: 100.64.0.0/10 and
// fd7a:115c:a1e0::/48. An address outside them is never a Tailscale address,
// whatever a command prints.
var tailscaleRanges = []netip.Prefix{netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("fd7a:115c:a1e0::/48")}

func inTailscaleRange(a netip.Addr) bool {
	a = a.Unmap()
	for _, p := range tailscaleRanges {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// Check returns nil when the server may listen on address, and a warning
// when it may only because of --allow-insecure-listen.
func Check(ctx context.Context, address string, r Rules) (warning string, err error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("listen: %q is not host:port: %w", address, err)
	}
	if r.TLS {
		return "", nil
	}
	if host == "localhost" {
		return "", nil
	}
	ip, perr := netip.ParseAddr(strings.Trim(host, "[]"))
	if perr == nil && ip.IsLoopback() {
		return "", nil
	}
	reason := ""
	switch {
	case host == "" || (perr == nil && ip.IsUnspecified()):
		reason = "every network interface (a wildcard address)"
	case perr != nil:
		reason = fmt.Sprintf("the host name %q", host)
	case !inTailscaleRange(ip):
		reason = fmt.Sprintf("%s, which is not a Tailscale address", ip)
	default:
		tailnet, terr := tailscaleOwns(ctx, ip, r)
		if tailnet {
			return "", nil
		}
		reason = fmt.Sprintf("%s, which this machine's Tailscale does not report as its own", ip)
		if terr != nil {
			reason = fmt.Sprintf("%s, which cannot be checked against Tailscale (%v)", ip, terr)
		}
	}
	if r.AllowInsecure {
		return fmt.Sprintf("serving plain HTTP on %s: anyone who can reach it can read passwords and listening data", reason), nil
	}
	return "", fmt.Errorf("listen: plain HTTP is allowed only on this machine (127.0.0.1) or its Tailscale address, not on %s; "+
		"serve through `tailscale serve`, give --tls-cert and --tls-key, or accept the risk with --allow-insecure-listen", reason)
}

func tailscaleOwns(ctx context.Context, ip netip.Addr, r Rules) (bool, error) {
	list := r.TailscaleAddrs
	if list == nil {
		list = func(ctx context.Context) ([]netip.Addr, error) {
			bin, err := r.Tailscale, error(nil)
			if bin == "" {
				bin, err = FindTailscale()
			} else {
				bin, err = ResolveTool(bin)
			}
			if err != nil {
				return nil, err
			}
			return TailscaleIP(ctx, bin)
		}
	}
	addrs, err := list(ctx)
	if err != nil {
		return false, err
	}
	for _, a := range addrs {
		if inTailscaleRange(a) && a.Unmap() == ip.Unmap() {
			return true, nil
		}
	}
	return false, nil
}

// cliPaths are where the tailscale command lives: on PATH, or inside the Mac app.
var cliPaths = []string{"tailscale", "/Applications/Tailscale.app/Contents/MacOS/Tailscale"}

// FindTailscale finds the tailscale command (on PATH, or inside the Mac app)
// and returns its absolute path, symlinks resolved, once VerifyTool accepts
// it. service install records the path, and the service uses only that one.
func FindTailscale() (string, error) {
	var firstErr error
	for _, p := range cliPaths {
		bin, err := exec.LookPath(p)
		if err != nil {
			continue
		}
		if bin, err = filepath.Abs(bin); err == nil {
			bin, err = ResolveTool(bin)
		}
		if err == nil {
			return bin, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return "", firstErr
	}
	return "", errors.New("the tailscale command is not installed")
}

// VerifyTool checks a command the server runs with authority over where it
// listens: an absolute path to a regular file that no user but root and the
// server's own can replace, nor anything on the way to it, symlinks and
// their targets included (private.TrustedExecutable).
func VerifyTool(bin string) error {
	_, err := ResolveTool(bin)
	return err
}

// ResolveTool is VerifyTool returning the verified file's resolved path: the
// command runs from it, never from the spelling given.
func ResolveTool(bin string) (string, error) {
	if !filepath.IsAbs(bin) {
		return "", fmt.Errorf("the tailscale command %q is not an absolute path", bin)
	}
	real, err := private.TrustedExecutable(bin)
	if err != nil {
		return "", fmt.Errorf("the tailscale command: %w", err)
	}
	return real, nil
}

// TailscaleIP asks `bin ip` for this machine's Tailscale addresses (read-only).
func TailscaleIP(ctx context.Context, bin string) ([]netip.Addr, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "ip").Output()
	if err != nil {
		return nil, fmt.Errorf("tailscale ip: %w", err)
	}
	return parseAddrs(string(out)), nil
}

// TailscaleSelf asks `bin status --json` for this machine's Tailscale
// addresses and its MagicDNS name (Self.DNSName, without its final dot), for
// the Host names the server answers. Addresses outside Tailscale's ranges
// are left out (read-only).
func TailscaleSelf(ctx context.Context, bin string) ([]netip.Addr, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "status", "--json").Output()
	if err != nil {
		return nil, "", fmt.Errorf("tailscale status: %w", err)
	}
	return parseStatus(out)
}

func parseStatus(out []byte) ([]netip.Addr, string, error) {
	var st struct {
		Self struct {
			DNSName      string
			TailscaleIPs []string
		}
	}
	if err := json.Unmarshal(out, &st); err != nil {
		return nil, "", fmt.Errorf("tailscale status: %w", err)
	}
	var addrs []netip.Addr
	for _, s := range st.Self.TailscaleIPs {
		if a, err := netip.ParseAddr(s); err == nil && inTailscaleRange(a) {
			addrs = append(addrs, a.Unmap())
		}
	}
	return addrs, strings.TrimSuffix(st.Self.DNSName, "."), nil
}

func parseAddrs(out string) []netip.Addr {
	var addrs []netip.Addr
	for _, line := range strings.Fields(out) {
		if a, err := netip.ParseAddr(line); err == nil {
			addrs = append(addrs, a)
		}
	}
	return addrs
}
