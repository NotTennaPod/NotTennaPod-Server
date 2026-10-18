package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/refresh"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The refresh API (PROTOCOL §9.6): POST /v1/refresh with sync
// authentication asks the server to refresh one podcast ({"podcast_id"}) or
// every podcast it keeps updated ({} or no body). It answers 202 at once with
// each podcast's live status and the sequence its request got; phones never
// wait for the fetch. GET /v1/refresh?podcast_id=… reads the named podcasts'
// statuses and schedules nothing, so a client can tell when its request was
// answered, also by a fetch that changed nothing.

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
	return http.StatusAccepted, map[string]any{"podcasts": statusList(statuses)}, nil
}

// refreshStatus answers GET /v1/refresh?podcast_id=a,b,… (the parameter
// repeated or comma-separated, at most refresh.MaxStatusPodcasts): the named
// podcasts' statuses, in that order, each once; a podcast the account does
// not have is left out. Each podcast is read by its keys, never the library.
func (req *request) refreshStatus() (int, any, error) {
	r := req.s.cfg.Refresher
	if r == nil {
		return 0, nil, errNotFound
	}
	q := req.r.URL.Query()
	for k := range q {
		if k != "podcast_id" {
			return 0, nil, invalid("Unknown parameter: " + k)
		}
	}
	var ids []string
	for _, v := range q["podcast_id"] {
		for _, id := range strings.Split(v, ",") {
			if !validPodcastID(id) {
				return 0, nil, invalid("podcast_id must list podcast IDs (p1:…).")
			}
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return 0, nil, invalid("podcast_id is required.")
	}
	if len(ids) > refresh.MaxStatusPodcasts {
		return 0, nil, invalid(fmt.Sprintf("At most %d podcasts at a time.", refresh.MaxStatusPodcasts))
	}
	statuses, err := r.Status(req.ctx, req.principal.accountID(), ids, func(tx *storage.ReadTx) error {
		_, err := req.authorizeSyncRead()(tx)
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, map[string]any{"podcasts": statusList(statuses)}, nil
}

// statusList is the refresh API's status items: {podcast_id, last_attempt,
// last_success, error, next_attempt, in_progress, request_seq, done_seq,
// running, merged_into}. request_seq is null in a request's answer for a
// podcast it recorded no request for (left to its Retry-After); a request is
// answered once done_seq reaches its request_seq, or when the podcast was
// merged (merged_into, its survivor).
func statusList(statuses []refresh.Status) []any {
	list := make([]any, 0, len(statuses))
	for _, st := range statuses {
		item := map[string]any{"podcast_id": st.PodcastID, "last_attempt": nil, "last_success": nil, "error": nil,
			"next_attempt": nil, "in_progress": st.InProgress, "request_seq": nil, "done_seq": st.DoneSeq,
			"running": st.Running, "merged_into": nil}
		for k, ms := range map[string]int64{"last_attempt": st.LastAttempt, "last_success": st.LastSuccess, "next_attempt": st.NextAttempt,
			"request_seq": st.RequestSeq} {
			if ms != 0 {
				item[k] = ms
			}
		}
		if st.Error != nil {
			item["error"] = st.Error
		}
		if st.MergedInto != "" {
			item["merged_into"] = st.MergedInto
		}
		list = append(list, item)
	}
	return list
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
