package media

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sort"
	"sync"
	"time"
)

// token is a media URL's capability (PROTOCOL §13): a random 128-bit ID bound
// to the account, the session credential, the episode, the source URL, the
// credential revision and the pinned representation. It is held in memory:
// a restart or 12 idle hours end it, and the player asks for a new
// descriptor.
type token struct {
	id         string
	account    string
	credential string // the session credential's hash
	episode    string
	sourceURL  string
	credRev    string
	mode       string // relay or compat
	up         *upstream
	spool      *spoolFile // a private copy serves it (PROTOCOL §13)
	facts      *Facts
	profile    *Profile
	startMS    int64
	durationMS int64
	ctype      string // the Content-Type served
	now        func() time.Time

	mu         sync.Mutex
	lastUsed   time.Time
	nextStream int64
	active     int
	changed    chan struct{} // closed and replaced when a stream ends
	order      int64         // streams' end order
	complete   int64         // end order of the latest stream that completed
	completeID int64
	failed     int64 // end order of the latest stream that failed
	failedID   int64
	failure    *Error
}

func newTokenID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// Stream outcomes.
const (
	outCancelled = iota // the browser went away, or the player started another stream: neutral
	outPartial          // delivered what was asked, not the final byte: neutral
	outComplete         // delivered the representation's final byte, or a conversion reached the end
	outFailed
)

// begin gives a media request its stream ID (PROTOCOL §13).
func (t *token) begin(now time.Time) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextStream++
	t.active++
	t.lastUsed = now
	return t.nextStream
}

// finish records a stream's outcome under the token.
func (t *token) finish(id int64, outcome int, err *Error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.active--
	t.order++
	switch outcome {
	case outComplete:
		t.complete, t.completeID = t.order, id
	case outFailed:
		t.failed, t.failedID, t.failure = t.order, id, err
	}
	close(t.changed)
	t.changed = make(chan struct{})
}

// fail records a failure found before any stream ran (a re-check).
func (t *token) fail(err *Error) {
	id := t.begin(time.Now())
	t.finish(id, outFailed, err)
}

// Status is the out-of-band outcome of a token's streams (PROTOCOL §13).
type Status struct {
	State    string       `json:"state"`     // streaming, complete or failed
	StreamID int64        `json:"stream_id"` // the media request the state belongs to; 0 before any
	Error    *ErrorDetail `json:"error"`     // failed: why, with Android's label
}

// status waits up to wait for streams still running, so an `ended` cannot
// outrun the completion of the stream that delivered the end; then the
// newest of the latest completion and the latest failure decides.
func (t *token) status(ctx context.Context, wait time.Duration) Status {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	t.mu.Lock()
	defer t.mu.Unlock()
	for t.active > 0 {
		ch := t.changed
		t.mu.Unlock()
		waited := true
		select {
		case <-ch:
			waited = false
		case <-timer.C:
		case <-ctx.Done():
		}
		t.mu.Lock()
		if waited {
			break
		}
	}
	switch {
	case t.failed > t.complete:
		return Status{State: "failed", StreamID: t.failedID, Error: t.failure.Detail()}
	case t.complete > 0:
		return Status{State: "complete", StreamID: t.completeID}
	}
	return Status{State: "streaming", StreamID: t.nextStream}
}

// tokenTable holds the live tokens.
type tokenTable struct {
	mu         sync.Mutex
	byID       map[string]*token
	idle       time.Duration
	perAccount int
	dropped    func(*token) // releases what the token holds
}

func (tt *tokenTable) put(t *token, now time.Time) {
	t.lastUsed, t.changed = now, make(chan struct{})
	tt.mu.Lock()
	tt.byID[t.id] = t
	var mine []*token
	for _, x := range tt.byID {
		if x.account == t.account {
			mine = append(mine, x)
		}
	}
	var evicted []*token
	if len(mine) > tt.perAccount {
		sort.Slice(mine, func(i, j int) bool { return mine[i].used().Before(mine[j].used()) })
		for _, x := range mine[:len(mine)-tt.perAccount] {
			delete(tt.byID, x.id)
			evicted = append(evicted, x)
		}
	}
	tt.mu.Unlock()
	for _, x := range evicted {
		tt.dropped(x)
	}
}

func (t *token) used() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastUsed
}

// lookup finds a token for the session that holds it: another account's or
// another session's token is unknown, as an expired one is. Idle expiry is
// checked before the token is renewed, so a token past it is never revived
// by a lookup before the next sweep.
func (tt *tokenTable) lookup(id, account, credential string, now time.Time) *token {
	tt.mu.Lock()
	t := tt.byID[id]
	tt.mu.Unlock()
	if t == nil || t.account != account || t.credential != credential {
		return nil
	}
	t.mu.Lock()
	expired := t.active == 0 && now.Sub(t.lastUsed) > tt.idle
	if !expired {
		t.lastUsed = now
	}
	t.mu.Unlock()
	if expired {
		tt.mu.Lock()
		gone := tt.byID[id] == t
		if gone {
			delete(tt.byID, id)
		}
		tt.mu.Unlock()
		if gone {
			tt.dropped(t)
		}
		return nil
	}
	return t
}

// sweep drops tokens idle for longer than the limit.
func (tt *tokenTable) sweep(now time.Time) {
	tt.mu.Lock()
	var gone []*token
	for id, t := range tt.byID {
		t.mu.Lock()
		idle := t.active == 0 && now.Sub(t.lastUsed) > tt.idle
		t.mu.Unlock()
		if idle {
			delete(tt.byID, id)
			gone = append(gone, t)
		}
	}
	tt.mu.Unlock()
	for _, t := range gone {
		tt.dropped(t)
	}
}

// dropWhere drops the tokens whose session gone reports ended. gone is
// asked outside the table's lock (it may read the database).
func (tt *tokenTable) dropWhere(gone func(account, credential string) bool) {
	tt.mu.Lock()
	all := make([]*token, 0, len(tt.byID))
	for _, t := range tt.byID {
		all = append(all, t)
	}
	tt.mu.Unlock()
	ended := map[[2]string]bool{}
	var dropped []*token
	for _, t := range all {
		k := [2]string{t.account, t.credential}
		e, asked := ended[k]
		if !asked {
			e = gone(t.account, t.credential)
			ended[k] = e
		}
		if !e {
			continue
		}
		tt.mu.Lock()
		if tt.byID[t.id] == t {
			delete(tt.byID, t.id)
			dropped = append(dropped, t)
		}
		tt.mu.Unlock()
	}
	for _, t := range dropped {
		tt.dropped(t)
	}
}

func (tt *tokenTable) dropAll() {
	tt.mu.Lock()
	all := tt.byID
	tt.byID = map[string]*token{}
	tt.mu.Unlock()
	for _, t := range all {
		tt.dropped(t)
	}
}
