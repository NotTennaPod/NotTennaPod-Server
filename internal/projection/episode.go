package projection

import "sort"

// SessionCut is a statistics reset's cut of one session continuation: its
// greatest ordinal in the reset's causal past (PROTOCOL §8; per continuation, PROTOCOL §6.10).
type SessionCut struct {
	Session string `json:"session"`
	Author  string `json:"author,omitempty"`
	Ordinal int64  `json:"ordinal"`
}

// SaveMeta identifies the journal save an export carries (PROTOCOL §6.10). First is ""
// when the export covers no new save.
type SaveMeta struct {
	ID       string `json:"id"`
	Index    int64  `json:"index"`
	First    string `json:"first,omitempty"`
	Baseline string `json:"baseline"`
	Passive  bool   `json:"passive,omitempty"`
}

// Imported is an imported episode's state (PROTOCOL §11.5): one
// effect per import decision and episode scope.
type Imported struct {
	PlayState  string     `json:"play_state"` // played, unplayed or new (the Inbox)
	Position   int64      `json:"position"`
	Favorite   bool       `json:"favorite,omitempty"`
	LastPlayed int64      `json:"last_played,omitempty"` // 0: never played
	History    *int64     `json:"history,omitempty"`     // the history time, if in history
	PlayedTime int64      `json:"played_time,omitempty"` // the statistics baseline
	Units      []StatUnit `json:"units,omitempty"`       // other source rows of the episode
	// The source rows aggregated here, when the import grouped several
	// (ImportedRow); the fold reads only the aggregate above.
	Rows []ImportedRow `json:"rows,omitempty"`
}

// StatUnit is a source row merged into an episode whose statistics stay its
// own, as Android counts each row (PROTOCOL §11.5).
type StatUnit struct {
	ID         string `json:"id"`
	Media      int64  `json:"media"` // the source media row
	PlayedTime int64  `json:"played_time"`
	LastPlayed int64  `json:"last_played"`
	History    *int64 `json:"history"`
	Duration   int64  `json:"duration"`
	Played     bool   `json:"played"`   // the row's own state, frozen: "include marked as played"
	Position   int64  `json:"position"` // the row's own position: started with marked-played
}

// EpisodeState is the fold of one episode scope (one generation).
type EpisodeState struct {
	Hidden    bool // the generation is closed
	PlayState string
	Position  int64
	ClaimID   string
	Favorite  bool
	// Queue (PROTOCOL §7.6): membership only; the order comes from the
	// queue replay (PROTOCOL §7.6), which reads Events.
	InQueue       bool
	QueueReason   string
	HasMembership bool
	MembershipBy  string
	// Events are the membership effects that took effect at their place,
	// in rank order, one per node (the queue replay's membership at each
	// step, PROTOCOL §7.1); none for a closed generation.
	Events []QueueEvent
	// Listening (PROTOCOL §8).
	LastPlayed     int64
	History        *int64 // nil when every history entry is cut
	PlayedTime     int64
	Duration       int64
	DurationAuthor string
	// Imported statistical units not covered by a reset (PROTOCOL §11.5).
	Units    []StatUnit
	HasUnits bool
	UnitsBy  []string
	// Discovery (PROTOCOL §9.4): the generation's admission.
	Admission   map[string]any
	AdmissionBy string
	// Which field groups have a record (PROTOCOL §7.1 "Which records exist").
	HasPlayState, HasPosition, HasFavorite                bool
	HasLastPlayed, HasHistory, HasPlayedTime, HasDuration bool
	// Provenance per field group: the nodes that determine the value.
	PlayStateBy, PositionBy, FavoriteBy, DurationBy string
	LastPlayedBy                                    string
	HistoryBy, PlayedTimeBy                         []string
	Status                                          map[string]Status
}

// isBarrier reports explicit interactions that stop automatic Inbox filing
// (PROTOCOL §7.3: explicit play-state commands, dismissals, listening activity;
// PROTOCOL §7.6 adds queue adds). Favorites are not barriers.
func isBarrier(e Effect) bool {
	switch e.Kind {
	case KindMarkPlayed, KindMarkUnplayed, KindResetPosition, KindRestoreState, KindDismiss, KindSessionStart, KindQueueRestore:
		return true
	case KindCheckpoint, KindSessionEnd:
		return e.Activity
	case KindQueueAdd:
		return !e.Automatic
	}
	return false
}

// QueueEvent is a membership effect that took effect at its node: in or out
// of the queue from there on.
type QueueEvent struct {
	Node string
	In   bool
}

type sessionState struct {
	latest        int64 // greatest ordinal
	contributions map[int64]int64
	latestNode    string
}

type historyEntry struct {
	node       string
	time       int64
	activity   bool // a listening activity (also last played), not just a history mark
	lastPlayed bool // an imported last-played time only: no history entry (PROTOCOL §11.5)
}

// importBaseline is an import decision's statistics in one scope.
type importBaseline struct {
	node   string
	played int64
	units  []StatUnit
}

// FoldEpisode folds all contributions of one episode scope in rank order.
//
// An automatic queue add (PROTOCOL §9 filing) applies only if the scope has
// no explicit membership decision that actually took effect, whatever the order
// (PROTOCOL §7.6): a completion whose claim was lost removes nothing, so it is no such
// decision. Whether a completion fires never depends on queue membership, so a
// first pass without automatic adds finds the decisions that took effect.
func FoldEpisode(cs []Contribution) EpisodeState {
	cs = Expand(cs)
	sortByRank(cs)
	cs = earliestAdmission(cs)
	automatic := false
	for _, c := range cs {
		if c.Effect.Kind == KindQueueAdd && c.Effect.Automatic || c.Effect.Kind == KindAdmission && c.Effect.Admission["action"] == "queue" {
			automatic = true
		}
	}
	if !automatic {
		st, _ := foldEpisode(cs, false)
		return st
	}
	_, decided := foldEpisode(cs, true)
	st, _ := foldEpisode(cs, decided)
	return st
}

// foldEpisode folds a scope with automatic adds barred or not, and reports
// whether an explicit membership decision took effect.
func foldEpisode(cs []Contribution, autoBarred bool) (EpisodeState, bool) {
	st := EpisodeState{PlayState: "unplayed", Status: map[string]Status{}}
	barrier, explicitMembership := false, false
	for _, c := range cs {
		if c.Effect.Kind == KindClose {
			st.Hidden = true
		}
		if isBarrier(c.Effect) {
			barrier = true
		}
	}
	// writers[field] = node that last wrote it; attempted[node] = fields it wrote;
	// fired[node]: a conditional effect fired, so the fact is applied.
	attempted := map[string][]string{}
	fired := map[string]bool{}
	write := func(field, node string) {
		attempted[node] = append(attempted[node], field)
		switch field {
		case "play_state":
			st.PlayStateBy = node
		case "position":
			st.PositionBy = node
		case "favorite":
			st.FavoriteBy = node
		case "queue_membership":
			st.MembershipBy = node
		}
	}
	// member writes queue membership and records the effect as a membership
	// event at its node; record marks writes whose outcome follows the
	// mechanical rule (listening facts and decisions set their own).
	member := func(in bool, reason, node string, record bool) {
		explicitMembership = true
		st.HasMembership = true
		st.InQueue, st.QueueReason = in, reason
		if record {
			write("queue_membership", node)
		} else {
			st.MembershipBy = node
		}
		if n := len(st.Events); n > 0 && st.Events[n-1].Node == node {
			st.Events[n-1].In = in
		} else {
			st.Events = append(st.Events, QueueEvent{Node: node, In: in})
		}
	}
	claim := ""                            // the node owning the position (PROTOCOL §8)
	fams := families(cs)                   // session families: paths, save tree, claims (PROTOCOL §6.10)
	finalized := map[string]bool{}         // sessions whose finalization took effect (PROTOCOL §6.10)
	left := map[string]bool{}              // transitions whose leave took effect (PROTOCOL §8)
	leaveCompleted := false                // a leave's completion fired (ranked before)
	folded := map[string]bool{}            // exports folded so far (ranked before)
	sessions := map[string]*sessionState{} // by continuation (contKey)
	session := func(id string) *sessionState {
		s := sessions[id]
		if s == nil {
			s = &sessionState{contributions: map[int64]int64{0: 0}}
			sessions[id] = s
		}
		return s
	}
	var entries []historyEntry
	var imports []importBaseline
	historyCuts := map[string][]string{} // cut node -> covered entries
	restored := map[string]bool{}
	statCuts := map[string]int64{}
	statCutBy := map[string]string{}
	// complete applies the completion bundle under the validated claim.
	complete := func(node, by string) {
		st.HasPlayState, st.HasPosition = true, true
		st.PlayState = "played"
		st.Position, st.ClaimID = 0, by
		write("play_state", node)
		write("position", node)
		member(false, "completed", node, false) // the bundle's queue removal (PROTOCOL §7.6)
	}
	// dismissNew is listening's NEW → UNPLAYED (never PLAYED → UNPLAYED).
	dismissNew := func(node string) {
		st.HasPlayState = true
		if st.PlayState == "new" {
			st.PlayState = "unplayed"
			st.PlayStateBy = node
		}
	}
	for _, c := range cs {
		e := c.Effect
		if e.Kind == KindAdmission {
			// The generation's admission record, then its filing at its place.
			st.Admission, st.AdmissionBy = e.Admission, c.Node
			f, ok := filing(e)
			if !ok {
				st.Status[c.Node] = Status{Outcome: "applied"}
				continue
			}
			e = f
		}
		switch e.Kind {
		case KindAdmissionNew:
			st.HasPlayState = true
			// A leave is no barrier, but the automatic NEW never undoes its
			// completion: the episode stays played (reference CHOICE).
			if barrier || leaveCompleted {
				st.Status[c.Node] = Status{Outcome: "not_applied", Reason: "explicit_state"}
				continue
			}
			st.PlayState = "new"
			write("play_state", c.Node)
		case KindMarkPlayed:
			st.HasPlayState = true
			st.PlayState = "played"
			write("play_state", c.Node)
			if e.ResetPosition {
				st.HasPosition = true
				st.Position, st.ClaimID = 0, c.Node
				claim = c.Node
				write("position", c.Node)
			}
		case KindMarkUnplayed:
			st.HasPlayState = true
			st.PlayState = "unplayed"
			write("play_state", c.Node)
		case KindResetPosition:
			st.HasPlayState, st.HasPosition = true, true
			st.PlayState = "unplayed"
			st.Position, st.ClaimID = 0, c.Node
			claim = c.Node
			write("play_state", c.Node)
			write("position", c.Node)
		case KindRestoreState:
			st.HasPlayState, st.HasPosition = true, true
			st.PlayState = e.PlayState
			if e.Position != nil {
				st.Position = *e.Position
			}
			st.ClaimID = c.Node
			claim = c.Node
			write("play_state", c.Node)
			write("position", c.Node)
		case KindDismiss:
			// A valid dismissal is always applied: it is a barrier even when
			// the episode was not NEW at its place.
			dismissNew(c.Node)
			st.Status[c.Node] = Status{Outcome: "applied"}
		case KindFavorite:
			st.HasFavorite = true
			st.Favorite = e.On != nil && *e.On
			write("favorite", c.Node)
		case KindQueueAdd, KindQueueRestore:
			if e.Automatic && autoBarred {
				st.Status[c.Node] = Status{Outcome: "not_applied", Reason: "explicit_state"}
				continue
			}
			wasExplicit := explicitMembership
			member(true, "", c.Node, true)
			if e.Automatic {
				explicitMembership = wasExplicit // an automatic add is no explicit decision
			}
			if !e.Automatic && st.PlayState == "new" {
				fired[c.Node] = true // its NEW → UNPLAYED fired: applied whatever wins later
			}
			if !e.Automatic {
				dismissNew(c.Node)
			}
		case KindQueueRemove:
			member(false, e.Reason, c.Node, true)
		case KindQueueMove:
			// A move writes no membership; its place is the replay's (it is
			// executed at its place in the order, so it is applied).
			if _, set := st.Status[c.Node]; !set {
				st.Status[c.Node] = Status{Outcome: "applied"}
			}
		case KindResolvedAdd: // a start's add the server resolved
			member(true, "", c.Node, false)
		case KindSessionStart:
			if e.QueueKey != "" { // its queue add: the device's bundle
				member(true, "", c.Node, false)
			}
			s := session(contKey(e.Session, e.Author))
			s.latestNode = c.Node
			claim = c.Node
			st.HasPosition = true
			if e.Position != nil {
				st.Position = *e.Position
			}
			st.ClaimID = c.Node
			st.PositionBy = c.Node
			dismissNew(c.Node)
			if e.Activity {
				entries = append(entries, historyEntry{node: c.Node, time: e.Time, activity: true})
			}
			st.Status[c.Node] = Status{Outcome: "applied"}
		case KindCheckpoint, KindSessionEnd:
			s := session(contKey(e.Session, e.Author))
			fam := fams[e.Session]
			folded[c.Node] = true
			// Without a claim of its own, an export writes under the claim its
			// path holds, else the session's start (PROTOCOL §6.10.11).
			sclaim := fam.claimOf[c.Node]
			if fam.claims[c.Node] {
				// New listening after the copy claims at its own place (PROTOCOL §6.10).
				claim, sclaim = c.Node, c.Node
			}
			// Ordinals, not rank order, choose a session's latest save
			// (PROTOCOL §8): only a save newer than every save of its continuation
			// ranked before it may write the position. A passive export (a
			// restart or tail export) of a save someone went on listening past is
			// stale and writes nothing (PROTOCOL §6.10, as in the independent
			// reference).
			stale := e.Save != nil && e.Save.Passive && fam.hasChild[e.Save.ID]
			owns := sclaim != "" && sclaim == claim && e.Ordinal > s.latest && !stale
			if owns {
				st.HasPosition = true
				if e.Position != nil {
					st.Position = *e.Position
				}
				st.ClaimID, st.PositionBy = sclaim, c.Node
			}
			if e.Activity {
				dismissNew(c.Node)
				entries = append(entries, historyEntry{node: c.Node, time: e.Time, activity: true})
			}
			s.contributions[e.Ordinal] = e.Contribution
			if e.Ordinal >= s.latest {
				s.latest, s.latestNode = e.Ordinal, c.Node
			}
			if e.Kind == KindSessionEnd {
				if e.Completed && owns {
					complete(c.Node, claim)
				} else if e.RemoveFromQueue {
					member(false, "skipped", c.Node, false)
				}
				if e.History {
					entries = append(entries, historyEntry{node: c.Node, time: e.Time, activity: false})
				}
			}
			st.Status[c.Node] = Status{Outcome: "applied"}
		case KindFinalize:
			// Only a session's first eligible finalization takes effect; a copied
			// one is eligible only if nobody went on listening past the save it
			// names (PROTOCOL §6.10).
			fam := fams[e.Session]
			eligible := fam == nil || fam.eligible(e.Author, e.Target, e.Ordinal)
			if e.Copied && e.Target != e.Author && e.Ordinal > e.Base {
				eligible = false // an ordinal the copy never held (as in the independent reference)
			}
			first := !finalized[e.Session] && eligible
			if first {
				finalized[e.Session] = true
			}
			// Naming the session's start means the latest claim on the
			// finalizer's writing history ranked before it, the shared part
			// included (PROTOCOL §6.10.11).
			claimID := e.Claim
			if fam != nil && e.Claim == fam.start {
				for _, x := range fam.history(e.Author, e.Target, e.Base) {
					if fam.claims[x.node] && folded[x.node] {
						claimID = x.node
					}
				}
			}
			done := first && e.Completed && claimID != "" && claimID == claim
			if done {
				complete(c.Node, claim)
			} else if first && e.RemoveFromQueue {
				member(false, "skipped", c.Node, false)
			}
			if e.History {
				entries = append(entries, historyEntry{node: c.Node, time: e.Time, activity: false})
			}
			// Superseded if its completion lost to a takeover ordered before
			// it, or another finalization took effect (its history mark still
			// counts); otherwise applied.
			if !first || (e.Completed && !done) {
				st.Status[c.Node] = Status{Outcome: "superseded"}
			} else {
				st.Status[c.Node] = Status{Outcome: "applied"}
			}
		case KindLeave:
			// episode.leave (PROTOCOL §8): a history mark; the first effective
			// leave of a transition (in this scope) completes under the claim
			// it observed (both absent counts as equal), or else removes a
			// skip from the queue; a later one keeps only its history mark. It
			// claims nothing, dismisses no NEW and is no barrier.
			first := !left[e.Transition]
			done := first && e.Completed && e.Claim == claim
			removed := false
			if done {
				complete(c.Node, claim)
				leaveCompleted = true
			} else if first && e.RemoveFromQueue {
				member(false, "skipped", c.Node, false)
				removed = true
			}
			if done || removed {
				left[e.Transition] = true
			}
			if e.History {
				entries = append(entries, historyEntry{node: c.Node, time: e.Time, activity: false})
			}
			// Superseded if its reported completion did not fire (even when
			// its removal applied instead), or, without one, its reported
			// removal did not apply (reference CHOICE).
			if (e.Completed && !done) || (!e.Completed && e.RemoveFromQueue && !removed) {
				st.Status[c.Node] = Status{Outcome: "superseded"}
			} else {
				st.Status[c.Node] = Status{Outcome: "applied"}
			}
		case KindImport:
			// An imported state (PROTOCOL §11.5): written like a restore, but no barrier;
			// NEW is the Inbox, which explicit interactions stop as they stop an
			// automatic filing.
			im := e.Imported
			if im == nil {
				continue
			}
			st.HasPlayState = true
			if im.PlayState != "new" {
				st.PlayState = im.PlayState
				write("play_state", c.Node)
			} else if !barrier {
				st.PlayState = "new"
				write("play_state", c.Node)
			}
			st.HasPosition = true
			st.Position, st.ClaimID = im.Position, c.Node
			claim = c.Node
			write("position", c.Node)
			if im.Favorite {
				st.HasFavorite, st.Favorite = true, true
				write("favorite", c.Node)
			}
			if e.QueueKey != "" {
				member(true, "", c.Node, true)
			}
			if im.LastPlayed > 0 {
				entries = append(entries, historyEntry{node: c.Node, time: im.LastPlayed, activity: true, lastPlayed: true})
			}
			if im.History != nil {
				entries = append(entries, historyEntry{node: c.Node, time: *im.History})
			}
			imports = append(imports, importBaseline{node: c.Node, played: im.PlayedTime, units: im.Units})
			st.Status[c.Node] = Status{Outcome: "applied"}
		case KindHistoryRemove, KindHistoryClear:
			historyCuts[c.Node] = e.Covered
			st.Status[c.Node] = Status{Outcome: "applied"}
		case KindHistoryRestore:
			// Admission lets only the first restore of a removal contribute; it
			// cancels that removal's cut wherever the two are ranked.
			restored[e.Removal] = true
			st.Status[c.Node] = Status{Outcome: "applied"}
		case KindStatsReset:
			// The effective cut is the greatest covered ordinal across all
			// resets (PROTOCOL §8.8), whatever their order.
			for _, cut := range e.Cuts {
				k := contKey(cut.Session, cut.Author)
				if old, seen := statCuts[k]; !seen || cut.Ordinal > old {
					statCuts[k], statCutBy[k] = cut.Ordinal, c.Node
				}
			}
			st.Status[c.Node] = Status{Outcome: "applied"}
		}
		if e.Duration != nil {
			st.HasDuration = true
			st.Duration, st.DurationAuthor, st.DurationBy = *e.Duration, e.Installation, c.Node
		}
	}
	// Last played and history (PROTOCOL §7.1 derived values).
	cut := map[string]bool{}
	for node, covered := range historyCuts {
		if restored[node] {
			continue
		}
		for _, n := range covered {
			cut[n] = true
		}
	}
	for _, h := range entries {
		if !h.lastPlayed {
			st.HasHistory = true
		}
		if h.activity {
			st.HasLastPlayed = true
			if h.time > st.LastPlayed || st.LastPlayedBy == "" {
				st.LastPlayed, st.LastPlayedBy = h.time, h.node
			}
		}
		if !h.lastPlayed && !cut[h.node] && (st.History == nil || h.time > *st.History) {
			t := h.time
			st.History = &t
			st.HistoryBy = []string{h.node}
		}
	}
	if st.HasHistory && st.History == nil {
		for node := range historyCuts {
			if !restored[node] {
				st.HistoryBy = append(st.HistoryBy, node)
			}
		}
	}
	// Played time: per session family, the save tree's edges not covered by a
	// statistics reset (PROTOCOL §6.10); for a single continuation, max(0, C_latest − C_cut).
	ids := make([]string, 0, len(sessions))
	for id := range sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	cutsBySession := map[string]map[string]int64{}
	for k, o := range statCuts {
		for session, fam := range fams {
			for author := range fam.conts {
				if contKey(session, author) == k {
					if cutsBySession[session] == nil {
						cutsBySession[session] = map[string]int64{}
					}
					cutsBySession[session][author] = o
				}
			}
		}
	}
	sessionIDs := make([]string, 0, len(fams))
	for id := range fams {
		sessionIDs = append(sessionIDs, id)
	}
	sort.Strings(sessionIDs)
	for _, id := range sessionIDs {
		st.HasPlayedTime = true
		st.PlayedTime += fams[id].playedTime(cutsBySession[id])
	}
	for _, id := range ids {
		st.PlayedTimeBy = append(st.PlayedTimeBy, sessions[id].latestNode)
		if by := statCutBy[id]; by != "" {
			st.PlayedTimeBy = append(st.PlayedTimeBy, by)
		}
	}
	// Imported baselines (PROTOCOL §11.5): an import decision is one statistical
	// source, covered by a reset whose causal past holds it (its cut names the
	// decision as a one-ordinal session).
	for _, b := range imports {
		k := contKey(b.node, "")
		if b.played > 0 || len(b.units) > 0 {
			st.HasPlayedTime = true
			st.PlayedTimeBy = append(st.PlayedTimeBy, b.node)
			if by := statCutBy[k]; by != "" {
				st.PlayedTimeBy = append(st.PlayedTimeBy, by)
			}
		}
		covered := statCuts[k] >= 1
		if !covered {
			st.PlayedTime += b.played
		}
		if len(b.units) > 0 {
			st.HasUnits = true
			st.UnitsBy = append(st.UnitsBy, b.node)
			if by := statCutBy[k]; by != "" {
				st.UnitsBy = append(st.UnitsBy, by)
			}
			if !covered {
				st.Units = append(st.Units, b.units...)
			}
		}
	}
	for node, fields := range attempted {
		if _, set := st.Status[node]; set {
			continue
		}
		final := false
		for _, f := range fields {
			if (f == "play_state" && st.PlayStateBy == node) || (f == "position" && st.PositionBy == node) || (f == "favorite" && st.FavoriteBy == node) ||
				(f == "queue_membership" && st.MembershipBy == node) {
				final = true
			}
		}
		if final || fired[node] {
			st.Status[node] = Status{Outcome: "applied"}
		} else {
			st.Status[node] = Status{Outcome: "superseded"}
		}
	}
	if st.Hidden {
		st.Events = nil // a closed generation's entries are never members (PROTOCOL §7.2)
		for _, c := range cs {
			if c.Effect.Kind != KindClose {
				st.Status[c.Node] = Status{Outcome: "not_applied", Reason: "generation_closed"}
			}
		}
	}
	return st, explicitMembership
}

// earliestAdmission keeps, of several admissions in one scope (merged
// episodes, PROTOCOL §9.7, §7.1), only the earlier one's: the later
// admission's record and filing take no effect. Earlier is by the
// admissions' own decision rank (epoch, seq, decision ID), not by the
// place their filings sit at (two admissions of one proposal share it); an
// admission without a known rank orders after those with one, then by
// place.
func earliestAdmission(cs []Contribution) []Contribution {
	keep, n := "", 0
	var keepRank *DecisionRank
	seen := map[string]bool{}
	for _, c := range cs {
		if c.Effect.Kind != KindAdmission {
			continue
		}
		id, _ := c.Effect.Admission["admission_id"].(string)
		if seen[id] {
			continue
		}
		seen[id] = true
		n++
		r := c.Effect.AdmissionRank
		if keep == "" || (r != nil && (keepRank == nil || r.Less(*keepRank))) {
			keep, keepRank = id, r // cs is in rank order
		}
	}
	if n < 2 {
		return cs
	}
	out := make([]Contribution, 0, len(cs))
	for _, c := range cs {
		if c.Effect.Kind == KindAdmission {
			if id, _ := c.Effect.Admission["admission_id"].(string); id != keep {
				continue
			}
		}
		out = append(out, c)
	}
	return out
}
