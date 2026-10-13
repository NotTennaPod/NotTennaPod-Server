package projection

import "sort"

// Effect is a fact's typed effect on one scope (stored as canonical JSON in
// contributions). Only the members a kind uses are set.
type Effect struct {
	Kind string `json:"kind"`
	// Episode scope.
	PlayState     string `json:"play_state,omitempty"`
	Position      *int64 `json:"position,omitempty"`
	ResetPosition bool   `json:"reset_position,omitempty"`
	On            *bool  `json:"on,omitempty"`
	// Listening (PROTOCOL §8).
	Session         string       `json:"session,omitempty"`
	Ordinal         int64        `json:"ordinal,omitempty"`
	Contribution    int64        `json:"contribution,omitempty"`
	Activity        bool         `json:"activity,omitempty"`
	Completed       bool         `json:"completed,omitempty"`
	RemoveFromQueue bool         `json:"remove_from_queue,omitempty"`
	History         bool         `json:"history,omitempty"`
	Claim           string       `json:"claim,omitempty"`      // a finalization's validated claim; a leave's observed claim
	Claiming        bool         `json:"claiming,omitempty"`   // a checkpoint's intentional resume: a claim on its path (PROTOCOL §8.1)
	Transition      string       `json:"transition,omitempty"` // a leave's pending transition
	Time            int64        `json:"time,omitempty"`       // the node's normalized time
	Duration        *int64       `json:"duration,omitempty"`
	Installation    string       `json:"installation,omitempty"`
	Covered         []string     `json:"covered,omitempty"` // history entries a history cut covers
	Cuts            []SessionCut `json:"cuts,omitempty"`    // a statistics reset's per-session cut ordinals
	Removal         string       `json:"removal,omitempty"` // the removal a history restore cancels
	// Copied installations (PROTOCOL §6.10).
	Author   string    `json:"author,omitempty"`   // the session fact's effective author (its continuation)
	Parent   string    `json:"parent,omitempty"`   // its continuation's parent continuation ("" for the root)
	Base     int64     `json:"base,omitempty"`     // the parent's last ordinal the continuation shares
	Baseline string    `json:"baseline,omitempty"` // a start's accounting baseline
	Save     *SaveMeta `json:"save,omitempty"`     // an export's save identity
	Copied   bool      `json:"copied,omitempty"`   // a finalization by a descendant author
	Target   string    `json:"target,omitempty"`   // the continuation a finalization names its ordinal in
	// Queue (PROTOCOL §7.6).
	QueueKey        string `json:"queue_key,omitempty"`        // a placement's key (a start's: its queue add), a candidate for the published key
	PositionID      string `json:"position_id,omitempty"`      // its position ID (a UUID)
	Automatic       bool   `json:"automatic,omitempty"`        // an automatic add (discovery, PROTOCOL §9)
	Reason          string `json:"reason,omitempty"`           // a queue removal's reason
	EnqueueLocation string `json:"enqueue_location,omitempty"` // account queue policy
	KeepSorted      bool   `json:"keep_sorted,omitempty"`
	SortOrder       string `json:"sort_order,omitempty"` // set with keep_sorted (one register)
	// Podcast scope.
	Generation       string  `json:"generation,omitempty"`
	Predecessor      string  `json:"predecessor,omitempty"` // generation an opened one follows
	Mode             string  `json:"mode,omitempty"`        // resolution mode for subscribe/preview
	Preview          bool    `json:"preview,omitempty"`
	NewEpisodes      *string `json:"new_episodes_action,omitempty"`
	KeepUpdated      *bool   `json:"keep_updated,omitempty"`
	SecretID         *string `json:"secret_id,omitempty"` // "" clears
	RefreshURL       string  `json:"refresh_url,omitempty"`
	DiscoveryDefault string  `json:"discovery_default,omitempty"`
	// Catalog (PROTOCOL §9.4).
	Observed     map[string]any `json:"observed,omitempty"`      // an observation's record value; a refresh status
	RevisionBase string         `json:"revision_base,omitempty"` // the revision the observer last held
	FetchTime    int64          `json:"fetch_time,omitempty"`    // the observation's (clamped) fetch time
	Admission    map[string]any `json:"admission,omitempty"`     // an admission record's value (with its filing: QueueKey, PositionID)
	// AdmissionRank is an admission's decision rank, attached when a scope
	// is loaded for folding (never stored): of two admissions in one scope,
	// the earlier by rank files (PROTOCOL §9.7).
	AdmissionRank *DecisionRank  `json:"-"`
	Refresh       map[string]any `json:"refresh,omitempty"` // a refresh status, with or without an observation
	// Identity (PROTOCOL §9.7): a member effect of an episode reference
	// that was resolved to another episode names its original; one node's
	// several effects in one scope are a compound.
	Member   string   `json:"member,omitempty"`
	Compound []Effect `json:"compound,omitempty"`
	// Ref is the episode reference a fact's effect was made through, as the
	// fact named it (PROTOCOL §9.7): where the effect lives is recomputed from it, with
	// the identity state, whenever that changes. nil for a decision's effect.
	Ref *Ref `json:"ref,omitempty"`
	// Import (PROTOCOL §11.5): an imported episode's state, and a created
	// generation that starts archived.
	Imported *Imported `json:"imported,omitempty"`
	Archived bool      `json:"archived,omitempty"`
}

// Ref is an episode reference as a fact named it: its episode, podcast and
// generation ("" for a catalog record), its GUID kind ("atom-id", else ""
// for rss-guid) and its occurrence evidence (trimmed GUID, normalized
// enclosure, publication time; "" and nil when absent).
type Ref struct {
	Episode    string `json:"episode"`
	Podcast    string `json:"podcast"`
	Generation string `json:"generation,omitempty"`
	Kind       string `json:"kind,omitempty"`
	GUID       string `json:"guid,omitempty"`
	Enclosure  string `json:"enclosure,omitempty"`
	Pub        *int64 `json:"pub,omitempty"`
}

// Effect kinds.
const (
	KindMarkPlayed     = "mark_played"
	KindMarkUnplayed   = "mark_unplayed"
	KindResetPosition  = "reset_position"
	KindRestoreState   = "restore_state"
	KindDismiss        = "dismiss"
	KindFavorite       = "favorite"
	KindAdmissionNew   = "admission_new" // automatic filing (PROTOCOL §9)
	KindSessionStart   = "session_start"
	KindCheckpoint     = "session_checkpoint"
	KindSessionEnd     = "session_end"
	KindFinalize       = "session_finalize"
	KindLeave          = "episode_leave" // a paused load's switch, skip or end (PROTOCOL §8)
	KindHistoryRemove  = "history_remove"
	KindHistoryRestore = "history_restore"
	KindHistoryClear   = "history_clear"
	KindStatsReset     = "statistics_reset"
	KindQueueAdd       = "queue_add" // membership in and position (queue.add member)
	// KindResolvedAdd is a start's server-resolved queue add (PROTOCOL §7.6): the
	// start's node contributes it beside its own effect, and it lives where
	// its decision's episode lives.
	KindResolvedAdd    = "resolved_add"
	KindQueueRestore   = "queue_restore" // Undo of a removal
	KindQueueRemove    = "queue_remove"  // membership out (queue.remove, queue.clear)
	KindQueueMove      = "queue_move"    // a move's target (no membership; its place is the replay's)
	KindQueuePolicy    = "queue_policy"  // account scope
	KindClose          = "close"         // the scope's generation was closed
	KindResolve        = "resolve"       // subscribe / register_preview resolution
	KindArchive        = "archive"
	KindRestore        = "restore"
	KindUnsubscribe    = "unsubscribe"
	KindSetPolicy      = "set_policy"
	KindSetCredentials = "set_credentials"
	KindChangeURL      = "change_url"
	KindDiscovery      = "discovery_policy"
	KindImport         = "import" // an imported episode's state (PROTOCOL §11.5)
)

// Contribution is one node's effect on a scope, with the node's order rank.
type Contribution struct {
	Node   string
	Rank   int64
	Effect Effect
}

// DecisionRank is a decision's rank (contract v1): (epoch, seq, decision ID).
type DecisionRank struct {
	Epoch, Seq int64
	ID         string
}

// Less orders ranks.
func (r DecisionRank) Less(o DecisionRank) bool {
	if r.Epoch != o.Epoch {
		return r.Epoch < o.Epoch
	}
	if r.Seq != o.Seq {
		return r.Seq < o.Seq
	}
	return r.ID < o.ID
}

// Status is a node's result in a fold: "applied", "superseded" or "not_applied"
// with a reason.
type Status struct {
	Outcome string
	Reason  string
}

// sortByRank orders contributions by rank; a compound's parts keep their order.
func sortByRank(cs []Contribution) {
	sort.SliceStable(cs, func(i, j int) bool { return cs[i].Rank < cs[j].Rank })
}

// GenerationState is one generation's lifecycle and per-generation settings.
type GenerationState struct {
	ID          string
	State       string // not_subscribed, subscribed, archived, removed
	Rank        int64  // rank of the opening contribution
	NewEpisodes string
	KeepUpdated bool
	SecretID    string
	RefreshURL  string
	OpeningURL  string
	UserURL     string // the refresh URL the user set: opening or change_url, never a server move (PROTOCOL §9.4)
	SecretURL   string // the user's URL when the current secret was set: a secret without approved origins approves only its origin
	Predecessor string
	Policy      map[string]string // writer node per policy field
	SecretBy    string
	StateBy     string
	RefreshBy   string
	// StateByOpening is the opening fact (provenance of the opening URL).
	StateByOpening string
}

// PodcastState is the fold of a podcast scope.
type PodcastState struct {
	Generations map[string]*GenerationState
	Live        *GenerationState // nil when every generation is closed
	Latest      *GenerationState // the most recently opened generation
	Status      map[string]Status
}

// RefreshURL is the live generation's refresh URL; with every generation closed,
// the latest generation's opening URL (a closed generation's writes are hidden).
func (st PodcastState) RefreshURL() (string, string) {
	if st.Live != nil {
		return st.Live.RefreshURL, st.Live.RefreshBy
	}
	if st.Latest != nil {
		return st.Latest.OpeningURL, st.Latest.StateByOpening
	}
	return "", ""
}

// FoldPodcast folds all contributions of a podcast scope (PROTOCOL §7.2):
// every generation exists from its opening fact with its base state and policy whatever the
// order; then joined subscribes, lifecycle changes, policy, credentials and URL writes apply in
// rank order.
func FoldPodcast(cs []Contribution) PodcastState {
	sortByRank(cs)
	st := PodcastState{Generations: map[string]*GenerationState{}, Status: map[string]Status{}}
	// Pass 1: open generations.
	for _, c := range cs {
		e := c.Effect
		if e.Kind != KindResolve || (e.Mode != "created" && e.Mode != "opened") {
			continue
		}
		g := &GenerationState{ID: e.Generation, State: "subscribed", Rank: c.Rank, NewEpisodes: "global", KeepUpdated: true,
			RefreshURL: e.RefreshURL, OpeningURL: e.RefreshURL, UserURL: e.RefreshURL, Predecessor: e.Predecessor, Policy: map[string]string{},
			StateBy: c.Node, RefreshBy: c.Node, StateByOpening: c.Node}
		if e.Preview {
			g.State, g.KeepUpdated = "not_subscribed", false
		}
		if e.Archived {
			g.State = "archived" // an imported archived subscription (PROTOCOL §11.5)
		}
		if e.NewEpisodes != nil {
			g.NewEpisodes, g.Policy["new_episodes_action"] = *e.NewEpisodes, c.Node
		}
		if e.KeepUpdated != nil {
			g.KeepUpdated, g.Policy["keep_updated"] = *e.KeepUpdated, c.Node
		}
		st.Generations[e.Generation] = g
	}
	// Pass 2: writes in rank order.
	type write struct{ gen, field string }
	written := map[string][]write{}
	always := map[string]bool{}
	notApplied := func(node, reason string) { st.Status[node] = Status{Outcome: "not_applied", Reason: reason} }
	for _, c := range cs {
		e := c.Effect
		g := st.Generations[e.Generation]
		switch e.Kind {
		case KindResolve:
			switch e.Mode {
			case "created", "opened":
				always[c.Node] = true
				if e.SecretID != nil {
					g.SecretID, g.SecretBy, g.SecretURL = *e.SecretID, c.Node, g.UserURL
				}
			case "joined":
				if g == nil {
					notApplied(c.Node, "unknown_generation")
					continue
				}
				if g.State == "removed" {
					notApplied(c.Node, "generation_closed")
					continue
				}
				if e.Preview {
					always[c.Node] = true // register-preview never changes anything
				} else {
					if g.State == "not_subscribed" {
						// Subscribing from a preview turns keep updated on (Android).
						g.KeepUpdated, g.Policy["keep_updated"] = true, c.Node
						written[c.Node] = append(written[c.Node], write{g.ID, "keep_updated"})
					}
					g.State, g.StateBy = "subscribed", c.Node
					written[c.Node] = append(written[c.Node], write{g.ID, "state"})
				}
				if e.SecretID != nil {
					g.SecretID, g.SecretBy, g.SecretURL = *e.SecretID, c.Node, g.UserURL
					written[c.Node] = append(written[c.Node], write{g.ID, "credentials"})
				}
			default:
				notApplied(c.Node, "stale_subscribe")
			}
		case KindMove:
			// A server move (PROTOCOL §9.4): a refresh-URL write when it carries one.
			if g == nil || g.State == "removed" || e.RefreshURL == "" {
				always[c.Node] = true
				continue
			}
			g.RefreshURL, g.RefreshBy = e.RefreshURL, c.Node
			written[c.Node] = append(written[c.Node], write{g.ID, "refresh_url"})
		case KindArchive, KindRestore, KindUnsubscribe, KindSetPolicy, KindSetCredentials, KindChangeURL:
			if g == nil {
				notApplied(c.Node, "unknown_generation")
				continue
			}
			if g.State == "removed" {
				notApplied(c.Node, "generation_closed")
				continue
			}
			switch e.Kind {
			case KindArchive, KindRestore:
				if g.State == "not_subscribed" {
					notApplied(c.Node, "not_subscribed")
					continue
				}
				if e.Kind == KindArchive {
					g.State = "archived"
				} else {
					g.State = "subscribed"
				}
				g.StateBy = c.Node
				written[c.Node] = append(written[c.Node], write{g.ID, "state"})
			case KindUnsubscribe:
				g.State, g.StateBy = "removed", c.Node
				always[c.Node] = true
			case KindSetPolicy:
				if e.NewEpisodes != nil {
					g.NewEpisodes, g.Policy["new_episodes_action"] = *e.NewEpisodes, c.Node
					written[c.Node] = append(written[c.Node], write{g.ID, "new_episodes_action"})
				}
				if e.KeepUpdated != nil {
					g.KeepUpdated, g.Policy["keep_updated"] = *e.KeepUpdated, c.Node
					written[c.Node] = append(written[c.Node], write{g.ID, "keep_updated"})
				}
			case KindSetCredentials:
				g.SecretID, g.SecretBy, g.SecretURL = *e.SecretID, c.Node, g.UserURL
				written[c.Node] = append(written[c.Node], write{g.ID, "credentials"})
			case KindChangeURL:
				g.RefreshURL, g.RefreshBy, g.UserURL = e.RefreshURL, c.Node, e.RefreshURL
				if e.Mode == "claimed" {
					always[c.Node] = true
				}
				written[c.Node] = append(written[c.Node], write{g.ID, "refresh_url"})
			}
		}
	}
	// Generations form a chain through their predecessors (acceptance order):
	// the latest is the one no other generation follows.
	followed := map[string]bool{}
	for _, g := range st.Generations {
		if g.Predecessor != "" {
			followed[g.Predecessor] = true
		}
	}
	for _, g := range st.Generations {
		if !followed[g.ID] && (st.Latest == nil || g.Rank > st.Latest.Rank) {
			st.Latest = g
		}
		if g.State != "removed" && (st.Live == nil || g.Rank > st.Live.Rank) {
			st.Live = g
		}
	}
	writer := func(w write) string {
		g := st.Generations[w.gen]
		switch w.field {
		case "state":
			return g.StateBy
		case "credentials":
			return g.SecretBy
		case "refresh_url":
			return g.RefreshBy
		}
		return g.Policy[w.field]
	}
	for _, c := range cs {
		if _, decided := st.Status[c.Node]; decided {
			continue
		}
		if always[c.Node] {
			st.Status[c.Node] = Status{Outcome: "applied"}
			continue
		}
		final := false
		for _, w := range written[c.Node] {
			if writer(w) == c.Node {
				final = true
			}
		}
		if final {
			st.Status[c.Node] = Status{Outcome: "applied"}
		} else if len(written[c.Node]) > 0 {
			st.Status[c.Node] = Status{Outcome: "superseded"}
		}
	}
	return st
}

// AccountState is the fold of the account scope.
type AccountState struct {
	DiscoveryPolicy string // "" until set
	DiscoveryBy     string
	// Queue policy (PROTOCOL §7.6): two registers, "" until set.
	EnqueueLocation, EnqueueBy string
	KeepSorted                 bool
	SortOrder, SortingBy       string
	Status                     map[string]Status
}

// Queue policy defaults (Android 3.12.1).
const (
	DefaultEnqueueLocation = "back"
	DefaultSortOrder       = "date_new_old"
)

// FoldAccount folds account-scope contributions: each register's last write in
// order wins; a fact is applied if it determines a register it wrote.
func FoldAccount(cs []Contribution) AccountState {
	sortByRank(cs)
	st := AccountState{Status: map[string]Status{}}
	wrote := map[string]int{}
	for _, c := range cs {
		e := c.Effect
		switch e.Kind {
		case KindDiscovery:
			st.DiscoveryPolicy, st.DiscoveryBy = e.DiscoveryDefault, c.Node
			wrote[c.Node]++
		case KindQueuePolicy:
			if e.EnqueueLocation != "" {
				st.EnqueueLocation, st.EnqueueBy = e.EnqueueLocation, c.Node
				wrote[c.Node]++
			}
			if e.SortOrder != "" {
				st.KeepSorted, st.SortOrder, st.SortingBy = e.KeepSorted, e.SortOrder, c.Node
				wrote[c.Node]++
			}
		}
	}
	for n := range wrote {
		if n == st.DiscoveryBy || n == st.EnqueueBy || n == st.SortingBy {
			st.Status[n] = Status{Outcome: "applied"}
		} else {
			st.Status[n] = Status{Outcome: "superseded"}
		}
	}
	return st
}

// QueuePolicy is the folded policy with Android's defaults filled in.
func (st AccountState) QueuePolicy() (location string, keepSorted bool, order string) {
	location, order = st.EnqueueLocation, st.SortOrder
	if location == "" {
		location = DefaultEnqueueLocation
	}
	if order == "" {
		order = DefaultSortOrder
	}
	return location, st.KeepSorted, order
}
