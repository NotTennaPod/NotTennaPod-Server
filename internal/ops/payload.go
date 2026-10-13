// Package ops implements the operation types of PROTOCOL §7.2 on the protocol's
// handler framework.
package ops

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
)

var errPayload = errors.New("invalid payload")

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errPayload, fmt.Sprintf(format, args...))
}

// fields checks an object's members: all required present, nothing unknown.
func fields(obj map[string]any, required []string, optional ...string) error {
	allowed := map[string]bool{}
	for _, k := range required {
		allowed[k] = true
		if _, ok := obj[k]; !ok {
			return invalidf("missing %s", k)
		}
	}
	for _, k := range optional {
		allowed[k] = true
	}
	for k := range obj {
		if !allowed[k] {
			return invalidf("unknown member %s", k)
		}
	}
	return nil
}

func str(obj map[string]any, k string) (string, bool) {
	s, ok := obj[k].(string)
	return s, ok
}

func object(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

// integer reads an integer: a decoded JSON number, or a Go integer from a
// value the server built itself (a parsed feed's fields).
func integer(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case interface{ String() string }:
		i, err := strconv.ParseInt(n.String(), 10, 64)
		return i, err == nil
	}
	return 0, false
}

func boolean(v any) (bool, bool) {
	b, ok := v.(bool)
	return b, ok
}

func prefixedHex(s, prefix string) bool {
	h, ok := strings.CutPrefix(s, prefix)
	if !ok || len(h) != 64 {
		return false
	}
	for _, c := range h {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// HasUserinfo reports a URL with HTTP userinfo (credentials must travel as secrets).
func HasUserinfo(raw string) bool {
	_, rest, ok := strings.Cut(raw, "://")
	if !ok {
		return false
	}
	end := strings.IndexAny(rest, "/?#")
	if end < 0 {
		end = len(rest)
	}
	return strings.Contains(rest[:end], "@")
}

// EpisodeRef is a validated episode reference (PROTOCOL §7).
type EpisodeRef struct {
	EpisodeID, PodcastID, GenerationID string
	Occurrence                         map[string]any
	// Original is the episode ID the fact named when acceptance resolved the
	// reference to another (a merge or an occurrence mapping, PROTOCOL §9.7).
	Original string
	// The reference as the fact named it, once routed (PROTOCOL §9.7).
	named                                       bool
	NamedEpisode, NamedPodcast, NamedGeneration string
}

var episodeKinds = map[string]bool{contract.KindRSSGUID: true, contract.KindAtomID: true, contract.KindGUIDAndEnclosure: true, contract.KindEnclosure: true}

// parseEpisodeRef validates shape and that an e1: ID is the derivation of its
// podcast and occurrence identity (PROTOCOL §3.3).
func parseEpisodeRef(v any, namespace string) (EpisodeRef, error) {
	obj, ok := object(v)
	if !ok {
		return EpisodeRef{}, invalidf("episode reference must be an object")
	}
	if err := fields(obj, []string{"episode_id", "podcast_id", "generation_id", "occurrence"}); err != nil {
		return EpisodeRef{}, err
	}
	r := EpisodeRef{}
	r.EpisodeID, _ = str(obj, "episode_id")
	r.PodcastID, _ = str(obj, "podcast_id")
	r.GenerationID, _ = str(obj, "generation_id")
	if !prefixedHex(r.PodcastID, "p1:") || !prefixedHex(r.GenerationID, "g1:") {
		return EpisodeRef{}, invalidf("bad podcast or generation ID")
	}
	occ, ok := object(obj["occurrence"])
	if !ok {
		return EpisodeRef{}, invalidf("occurrence must be an object")
	}
	if err := fields(occ, []string{"k", "v"}, "guid", "enclosure_url", "title", "publication_time"); err != nil {
		return EpisodeRef{}, err
	}
	k, _ := str(occ, "k")
	val, ok := str(occ, "v")
	if !ok {
		return EpisodeRef{}, invalidf("occurrence v must be a string")
	}
	// e0: episodes (no GUID and no enclosure, PROTOCOL §3.3 rule 5) carry k "none", v "".
	if !episodeKinds[k] && !(k == "none" && val == "" && strings.HasPrefix(r.EpisodeID, "e0:")) {
		return EpisodeRef{}, invalidf("unknown identity kind")
	}
	for _, f := range []string{"guid", "enclosure_url", "title"} {
		if x, present := occ[f]; present {
			if _, ok := x.(string); !ok {
				return EpisodeRef{}, invalidf("occurrence %s must be a string", f)
			}
		}
	}
	if u, ok := str(occ, "enclosure_url"); ok && HasUserinfo(u) {
		return EpisodeRef{}, invalidf("credentials in a URL")
	}
	if x, present := occ["publication_time"]; present {
		if _, ok := integer(x); !ok {
			return EpisodeRef{}, invalidf("publication_time must be an integer")
		}
	}
	// The ID is not re-derived from k/v: a known alias may map an item to an
	// existing episode ID (PROTOCOL §3.3 rule 1).
	if !prefixedHex(r.EpisodeID, "e1:") && !(strings.HasPrefix(r.EpisodeID, "e0:") && contract.ValidUUID(strings.TrimPrefix(r.EpisodeID, "e0:"))) {
		return EpisodeRef{}, invalidf("bad episode ID")
	}
	_ = namespace
	r.Occurrence = occ
	return r, nil
}

// checkTargets requires targets to be exactly the given entities.
func checkTargets(f *protocol.Fact, want []protocol.Target) error {
	sort.Slice(want, func(i, j int) bool {
		a, b := want[i], want[j]
		if a.Entity != b.Entity {
			return a.Entity < b.Entity
		}
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		return a.Generation < b.Generation
	})
	if len(want) != len(f.Targets) {
		return invalidf("targets do not match the payload")
	}
	for i := range want {
		if want[i] != f.Targets[i] {
			return invalidf("targets do not match the payload")
		}
	}
	return nil
}
