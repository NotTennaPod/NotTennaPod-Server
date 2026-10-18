package reference

import (
	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/stats"
)

// Accessors for the tests that compare the read model with the oracle.

// Parts returns an episode's statistics units as the oracle computes them.
func Parts(l *Library, e *Episode) []stats.Part { return l.parts(e) }

// QueueKey returns a queued episode's queue key.
func QueueKey(e *Episode) contract.QueueKey { return e.queueKey }

// Fields returns a record's decoded top-level fields.
func Fields(r Record) map[string]any { return r.fields }
