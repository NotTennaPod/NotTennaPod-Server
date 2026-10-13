package main

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/httpserver"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/listen"
)

// hostList is the repeatable --allow-host option: more host names the server
// answers.
type hostList []string

func (l *hostList) String() string { return strings.Join(*l, ",") }

func (l *hostList) Set(v string) error {
	v = strings.TrimSpace(v)
	name := v
	if h, _, err := net.SplitHostPort(v); err == nil {
		name = h
	}
	name = strings.TrimSuffix(strings.TrimPrefix(name, "["), "]")
	if name == "" || strings.ContainsAny(v, "/@?# \t") {
		return fmt.Errorf("--allow-host takes a host name or address, like nas.example or 192.168.1.5, not %q", v)
	}
	*l = append(*l, v)
	return nil
}

// serverHosts are the Host names the server answers: loopback and the
// address each connection arrived at (always), the listening host, the
// public URL's host, every --allow-host, and this machine's Tailscale
// addresses and MagicDNS name, asked of the verified tailscale command
// (tailscale; empty finds and verifies it).
func serverHosts(address, publicURL string, allow hostList, tailscale string) *httpserver.Hosts {
	names := append([]string{}, allow...)
	if host, _, err := net.SplitHostPort(address); err == nil && host != "" {
		names = append(names, host)
	}
	if u, err := url.Parse(publicURL); err == nil && u.Host != "" {
		names = append(names, u.Host)
	}
	return httpserver.NewHosts(names, func(ctx context.Context) ([]netip.Addr, string, error) {
		bin, err := tailscale, error(nil)
		if bin == "" {
			bin, err = listen.FindTailscale()
		} else {
			bin, err = listen.ResolveTool(bin) // run the verified file, not the spelling
		}
		if err != nil {
			return nil, "", err
		}
		return listen.TailscaleSelf(ctx, bin)
	})
}
