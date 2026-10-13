package auth

import (
	"math"
	"sync"
	"time"
)

// Limiter counts failed sign-ins per username and per non-loopback IP
// (PROTOCOL §5.5: 5 failures within 15 minutes, then 2^(n-5) s refusals, capped at 15 min).
type Limiter struct {
	mu      sync.Mutex
	now     func() time.Time
	buckets map[string]*bucket
}

type bucket struct {
	failures    []time.Time
	lockedUntil time.Time
}

const (
	window      = 15 * time.Minute
	freeFailure = 5
	maxLock     = 15 * time.Minute
)

// NewLimiter returns a limiter using now as its clock (time.Now if nil).
func NewLimiter(now func() time.Time) *Limiter {
	if now == nil {
		now = time.Now
	}
	return &Limiter{now: now, buckets: map[string]*bucket{}}
}

// Keys names the buckets of an attempt: the username and, for non-loopback
// peers, the IP.
func Keys(usernameKey, ip string, loopback bool) []string {
	keys := []string{"u:" + usernameKey}
	if !loopback && ip != "" {
		keys = append(keys, "ip:"+ip)
	}
	return keys
}

// Allow reports whether an attempt may be verified now, and if not how long to wait.
func (l *Limiter) Allow(keys []string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	var wait time.Duration
	for _, k := range keys {
		if b := l.buckets[k]; b != nil && now.Before(b.lockedUntil) {
			if d := b.lockedUntil.Sub(now); d > wait {
				wait = d
			}
		}
	}
	return wait == 0, wait
}

// Fail records a verified failure for every key.
func (l *Limiter) Fail(keys []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	for _, k := range keys {
		b := l.buckets[k]
		if b == nil {
			b = &bucket{}
			l.buckets[k] = b
		}
		b.prune(now)
		b.failures = append(b.failures, now)
		if n := len(b.failures); n > freeFailure-1 {
			d := time.Duration(math.Pow(2, float64(n-freeFailure))) * time.Second
			if d > maxLock || d <= 0 {
				d = maxLock
			}
			b.lockedUntil = now.Add(d)
		}
	}
	l.sweep(now)
}

// Succeed resets the buckets.
func (l *Limiter) Succeed(keys []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, k := range keys {
		delete(l.buckets, k)
	}
}

func (b *bucket) prune(now time.Time) {
	i := 0
	for i < len(b.failures) && now.Sub(b.failures[i]) >= window {
		i++
	}
	b.failures = b.failures[i:]
}

// sweep drops idle buckets so memory stays bounded.
func (l *Limiter) sweep(now time.Time) {
	if len(l.buckets) < 1024 {
		return
	}
	for k, b := range l.buckets {
		b.prune(now)
		if len(b.failures) == 0 && !now.Before(b.lockedUntil) {
			delete(l.buckets, k)
		}
	}
}
