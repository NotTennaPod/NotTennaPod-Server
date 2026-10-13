package httpserver

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// Hosts decides which Host names the server answers: a page whose name an attacker points at the
// server (DNS rebinding) is refused before routing, authentication or rate limiting. Answered
// are:
//   - loopback names and addresses (localhost, 127.0.0.1, [::1]), any port;
//   - the address the connection arrived at (the listening address, or with
//     a wildcard listener, the machine's address the client used);
//   - the names given to NewHosts: the listening host name, --public-url's
//     host and every --allow-host;
//   - this machine's Tailscale addresses and MagicDNS name, as Tailscale
//     reports them.
//
// Ports are not compared: the connection already reached this server.
type Hosts struct {
	names     map[string]bool
	tailscale func(ctx context.Context) ([]netip.Addr, string, error)

	mu      sync.Mutex
	tsNames map[string]bool
	tsAsked time.Time
}

// tailscaleRecheck is how often an unknown Host asks Tailscale again (it may
// have started after the server).
const tailscaleRecheck = 30 * time.Second

// NewHosts answers names (host names, IP addresses, or host:port forms) and
// what tailscale reports (nil: none).
func NewHosts(names []string, tailscale func(ctx context.Context) ([]netip.Addr, string, error)) *Hosts {
	h := &Hosts{names: map[string]bool{}, tailscale: tailscale}
	for _, n := range names {
		if n = hostName(n); n != "" {
			h.names[n] = true
		}
	}
	return h
}

// hostName is a Host value's name: without its port or IPv6 brackets, in
// lower case, without a final dot; an IP address in its normal form.
func hostName(host string) string {
	host = strings.TrimSpace(host)
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")), ".")
	if a, err := netip.ParseAddr(host); err == nil {
		return a.Unmap().String()
	}
	return host
}

// Allowed reports whether the server answers r's Host.
func (h *Hosts) Allowed(r *http.Request) bool {
	name := hostName(r.Host)
	if name == "" {
		return false
	}
	if name == "localhost" {
		return true
	}
	a, err := netip.ParseAddr(name)
	isAddr := err == nil
	if isAddr && a.IsLoopback() {
		return true
	}
	if isAddr {
		if local, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr); ok {
			if ap, err := netip.ParseAddrPort(local.String()); err == nil && ap.Addr().Unmap() == a {
				return true
			}
		}
	}
	if h == nil {
		return false
	}
	if h.names[name] {
		return true
	}
	return h.tailscaleKnows(r.Context(), name)
}

// tailscaleKnows reports a name Tailscale reports for this machine, asking
// Tailscale at most every tailscaleRecheck.
func (h *Hosts) tailscaleKnows(ctx context.Context, name string) bool {
	if h.tailscale == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.tsNames[name] {
		return true
	}
	if !h.tsAsked.IsZero() && time.Since(h.tsAsked) < tailscaleRecheck {
		return false
	}
	h.tsAsked = time.Now()
	addrs, dnsName, err := h.tailscale(ctx)
	if err != nil {
		return false
	}
	h.tsNames = map[string]bool{}
	for _, a := range addrs {
		h.tsNames[a.Unmap().String()] = true
	}
	if n := hostName(dnsName); n != "" {
		h.tsNames[n] = true
	}
	return h.tsNames[name]
}

// misdirected answers a request for a Host the server does not answer.
func misdirected(w http.ResponseWriter) {
	// The connection ends with the answer: its body is never read.
	w.Header().Set("Connection", "close")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusMisdirectedRequest)
	_, _ = w.Write([]byte(`{"error":"misdirected_request","message":"This server does not answer to this host name. ` +
		`Its administrator can allow the name with --allow-host."}` + "\n"))
}
