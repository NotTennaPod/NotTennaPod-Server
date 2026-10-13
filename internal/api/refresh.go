package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/refresh"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The refresh API (PROTOCOL §9.4): POST /v1/refresh with sync
// authentication asks the server to refresh one podcast ({"podcast_id"}) or
// every podcast it keeps updated ({} or no body). It answers 202 at once with
// each podcast's live status; phones never wait for the fetch.

func (req *request) refresh() (int, any, error) {
	r := req.s.cfg.Refresher
	if r == nil {
		return 0, nil, errNotFound
	}
	obj := map[string]any{}
	if req.r.ContentLength != 0 {
		var err error
		if obj, err = req.readProtocolBody(); err != nil {
			return 0, nil, err
		}
	}
	if err := onlyFields(obj, "podcast_id"); err != nil {
		return 0, nil, err
	}
	podcastID := ""
	if raw, present := obj["podcast_id"]; present {
		s, ok := raw.(string)
		if !ok || !validPodcastID(s) {
			return 0, nil, invalid("podcast_id must be a podcast ID (p1:…).")
		}
		podcastID = s
	}
	statuses, err := r.AuthorizedRequest(req.ctx, req.principal.accountID(), podcastID, func(tx *storage.WriteTx) error {
		_, err := req.revalidate(tx)
		return err
	})
	if errors.Is(err, refresh.ErrUnknownPodcast) {
		return 0, nil, errNotFound
	}
	if err != nil {
		return 0, nil, err
	}
	list := make([]any, 0, len(statuses))
	for _, st := range statuses {
		item := map[string]any{"podcast_id": st.PodcastID, "last_attempt": nil, "last_success": nil, "error": nil,
			"next_attempt": nil, "in_progress": st.InProgress}
		for k, ms := range map[string]int64{"last_attempt": st.LastAttempt, "last_success": st.LastSuccess, "next_attempt": st.NextAttempt} {
			if ms != 0 {
				item[k] = ms
			}
		}
		if st.Error != nil {
			item["error"] = st.Error
		}
		list = append(list, item)
	}
	return http.StatusAccepted, map[string]any{"podcasts": list}, nil
}

// validPodcastID accepts "p1:" and 64 lower-case hex digits.
func validPodcastID(s string) bool {
	hex, ok := strings.CutPrefix(s, "p1:")
	if !ok || len(hex) != 64 {
		return false
	}
	for _, c := range hex {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
