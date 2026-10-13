package auth

// Unit tests for the authentication primitives (PROTOCOL §5, "Identities and secrets" and "Login
// rate limiting").

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

var fast = Params{Memory: 1024, Time: 1, Threads: 1}

func TestDefaultParamsAreTheDesignedOnes(t *testing.T) {
	if DefaultParams != (Params{Memory: 64 * 1024, Time: 3, Threads: 4}) {
		t.Errorf("DefaultParams %+v, want m=65536,t=3,p=4", DefaultParams)
	}
}

func TestHashAndVerify(t *testing.T) {
	ctx := context.Background()
	h := NewHasher(fast)
	phc := regexp.MustCompile(`^\$argon2id\$v=19\$m=1024,t=1,p=1\$([A-Za-z0-9+/]+)\$([A-Za-z0-9+/]+)$`)
	for _, password := range []string{"correct horse", "ééé日本語-password", strings.Repeat("x", 1024)} {
		enc, err := h.Hash(ctx, password)
		if err != nil {
			t.Fatal(err)
		}
		m := phc.FindStringSubmatch(enc)
		if m == nil {
			t.Fatalf("hash %q is not an Argon2id PHC string", enc)
		}
		salt, _ := base64.RawStdEncoding.DecodeString(m[1])
		key, _ := base64.RawStdEncoding.DecodeString(m[2])
		if len(salt) != 16 || len(key) != 32 {
			t.Errorf("salt %d bytes, hash %d bytes; want 16 and 32", len(salt), len(key))
		}
		if strings.Contains(enc, password) {
			t.Error("the hash contains the password")
		}
		if ok, rehash, err := h.Verify(ctx, password, enc); !ok || rehash || err != nil {
			t.Errorf("Verify(right) = %v, %v, %v", ok, rehash, err)
		}
		if ok, rehash, err := h.Verify(ctx, password+"!", enc); ok || rehash || err != nil {
			t.Errorf("Verify(wrong) = %v, %v, %v", ok, rehash, err)
		}
		again, _ := h.Hash(ctx, password)
		if again == enc {
			t.Error("two hashes of one password are identical (salt not random)")
		}
		if ok, _, _ := h.Verify(ctx, password, again); !ok {
			t.Error("the second hash does not verify")
		}
	}
	// An unknown username (empty stored hash) never verifies, whatever the password.
	for _, password := range []string{"", "not a real password", "x"} {
		if ok, rehash, err := h.Verify(ctx, password, ""); ok || rehash || err != nil {
			t.Errorf("Verify(%q, \"\") = %v, %v, %v", password, ok, rehash, err)
		}
	}
}

func TestVerifyRejectsMalformedHashes(t *testing.T) {
	ctx := context.Background()
	h := NewHasher(fast)
	good, err := h.Hash(ctx, "password")
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(good, "$") // "", argon2id, v=19, params, salt, hash
	short := base64.RawStdEncoding.EncodeToString([]byte("short"))
	cases := map[string]string{
		"plain text":       "password",
		"bcrypt":           "$2b$10$abcdefghijklmnopqrstuuIRLrDxiOhOhDNBZpDZHuVNxQ2SfSPwq",
		"argon2i":          strings.Replace(good, "$argon2id$", "$argon2i$", 1),
		"other version":    strings.Replace(good, "$v=19$", "$v=16$", 1),
		"missing version":  "$argon2id$" + strings.Join(parts[3:], "$"),
		"missing params":   "$argon2id$v=19$" + parts[4] + "$" + parts[5],
		"zero time":        strings.Replace(good, "t=1", "t=0", 1),
		"zero lanes":       strings.Replace(good, "p=1", "p=0", 1),
		"garbage params":   strings.Replace(good, "m=1024", "m=lots", 1),
		"bad salt":         strings.Join([]string{"", parts[1], parts[2], parts[3], "!!!!", parts[5]}, "$"),
		"bad hash":         strings.Join([]string{"", parts[1], parts[2], parts[3], parts[4], "!!!!"}, "$"),
		"short salt":       strings.Join([]string{"", parts[1], parts[2], parts[3], short, parts[5]}, "$"),
		"short hash":       strings.Join([]string{"", parts[1], parts[2], parts[3], parts[4], short}, "$"),
		"extra field":      good + "$extra",
		"truncated":        good[:len(good)/2],
		"padded base64":    strings.Join([]string{"", parts[1], parts[2], parts[3], parts[4] + "==", parts[5]}, "$"),
		"empty components": "$argon2id$$$$",
	}
	for name, enc := range cases {
		ok, rehash, err := h.Verify(ctx, "password", enc)
		if ok || rehash {
			t.Errorf("%s: verified a malformed hash", name)
		}
		if !errors.Is(err, ErrMalformedHash) {
			t.Errorf("%s: err %v, want ErrMalformedHash", name, err)
		}
	}
}

func TestNeedsRehashWhenParametersChange(t *testing.T) {
	ctx := context.Background()
	weak := NewHasher(fast)
	strong := NewHasher(Params{Memory: 2048, Time: 2, Threads: 1})
	enc, _ := weak.Hash(ctx, "password-1")
	if ok, rehash, _ := strong.Verify(ctx, "password-1", enc); !ok || !rehash {
		t.Errorf("weaker stored parameters: ok=%v rehash=%v, want true, true", ok, rehash)
	}
	if ok, rehash, _ := weak.Verify(ctx, "password-1", enc); !ok || rehash {
		t.Errorf("current parameters: ok=%v rehash=%v, want true, false", ok, rehash)
	}
	if ok, rehash, _ := strong.Verify(ctx, "password-2", enc); ok || rehash {
		t.Errorf("wrong password: ok=%v rehash=%v, want false, false", ok, rehash)
	}
	enc2, _ := strong.Hash(ctx, "password-1")
	if ok, rehash, _ := strong.Verify(ctx, "password-1", enc2); !ok || rehash {
		t.Errorf("after rehashing: ok=%v rehash=%v, want true, false", ok, rehash)
	}
}

// At most two Argon2id computations run at once with a small queue; beyond that
// requests fail fast with ErrBusy instead of piling up memory.
func TestHasherRefusesWorkBeyondTwoRunningAndASmallQueue(t *testing.T) {
	ctx := context.Background()
	h := NewHasher(fast)
	if cap(h.running) != 2 {
		t.Fatalf("%d computations may run at once, want 2", cap(h.running))
	}
	if h.maxWait < 1 || h.maxWait > 16 {
		t.Fatalf("queue of %d is not small", h.maxWait)
	}
	// Two long computations hold both slots.
	h.running <- struct{}{}
	h.running <- struct{}{}
	deadline := time.Now().Add(10 * time.Second)
	waitFor := func(n int32) {
		t.Helper()
		for h.waiting.Load() != n {
			if time.Now().After(deadline) {
				t.Fatalf("%d requests waiting, want %d", h.waiting.Load(), n)
			}
			time.Sleep(time.Millisecond)
		}
	}
	// A queued request whose client goes away stops waiting.
	cctx, cancel := context.WithCancel(ctx)
	abandoned := make(chan error, 1)
	go func() { _, err := h.Hash(cctx, "abandoned"); abandoned <- err }()
	waitFor(1)
	cancel()
	if err := <-abandoned; !errors.Is(err, context.Canceled) {
		t.Errorf("abandoned request: %v, want context.Canceled", err)
	}
	waitFor(0)
	// Fill the queue.
	queued := make(chan error, h.maxWait)
	var wg sync.WaitGroup
	for range h.maxWait {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h.Hash(ctx, "queued")
			queued <- err
		}()
	}
	waitFor(h.maxWait)
	start := time.Now()
	if _, err := h.Hash(ctx, "one too many"); !errors.Is(err, ErrBusy) {
		t.Errorf("Hash beyond the queue: %v, want ErrBusy", err)
	}
	enc, _ := NewHasher(fast).Hash(ctx, "x")
	if _, _, err := h.Verify(ctx, "x", enc); !errors.Is(err, ErrBusy) {
		t.Errorf("Verify beyond the queue: %v, want ErrBusy", err)
	}
	if _, _, err := h.Verify(ctx, "x", ""); !errors.Is(err, ErrBusy) {
		t.Errorf("unknown-user Verify beyond the queue: %v, want ErrBusy", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("refusals took %v; they must not wait", d)
	}
	// The long computations finish; every queued request completes.
	<-h.running
	<-h.running
	wg.Wait()
	close(queued)
	for err := range queued {
		if err != nil {
			t.Errorf("a queued request failed: %v", err)
		}
	}
	if _, err := h.Hash(ctx, "after"); err != nil {
		t.Errorf("the hasher did not recover: %v", err)
	}
}

func TestCredentials(t *testing.T) {
	seen := map[string]bool{}
	for range 200 {
		secret, hash := NewCredential()
		if !strings.HasPrefix(secret, "ntp1_") || len(secret) != len("ntp1_")+43 {
			t.Fatalf("credential %q is not ntp1_ + 43 base64url characters", secret)
		}
		raw, err := base64.RawURLEncoding.DecodeString(secret[5:])
		if err != nil || len(raw) != 32 {
			t.Fatalf("credential payload: %d bytes, %v", len(raw), err)
		}
		sum := sha256.Sum256([]byte(secret))
		if !bytes.Equal(hash, sum[:]) || !bytes.Equal(HashCredential(secret), sum[:]) {
			t.Fatal("the stored hash is not SHA-256 of the credential")
		}
		if !WellFormed(secret) {
			t.Fatalf("WellFormed(%q) = false", secret)
		}
		if seen[secret] {
			t.Fatal("a credential repeated")
		}
		seen[secret] = true
	}
	secret, _ := NewCredential()
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	last := strings.IndexByte(alphabet, secret[len(secret)-1])
	payload := secret[5:]
	std := base64.StdEncoding.EncodeToString(make([]byte, 32))
	for name, s := range map[string]string{
		"empty":             "",
		"prefix only":       "ntp1_",
		"other prefix":      "ntp2_" + payload,
		"no prefix":         payload,
		"one short":         secret[:len(secret)-1],
		"one long":          secret + "A",
		"bearer included":   "Bearer " + secret,
		"standard alphabet": "ntp1_" + strings.NewReplacer("-", "+", "_", "/").Replace(payload[:42]) + "+",
		"padded":            "ntp1_" + std,
		"non-canonical":     secret[:len(secret)-1] + string(alphabet[last|1]),
		"space inside":      secret[:10] + " " + secret[11:],
	} {
		if WellFormed(s) {
			t.Errorf("%s: WellFormed(%q) = true", name, s)
		}
	}
}

func TestSetupCodes(t *testing.T) {
	for _, c := range "01OIL" {
		if strings.ContainsRune(SetupCodeAlphabet, c) {
			t.Errorf("the setup code alphabet contains the ambiguous %q", c)
		}
	}
	seen := map[string]bool{}
	for range 200 {
		code := NewSetupCode()
		if len(code) != 10 || strings.Trim(code, SetupCodeAlphabet) != "" {
			t.Fatalf("setup code %q is not 10 characters from the alphabet", code)
		}
		seen[code] = true
	}
	if len(seen) < 200 {
		t.Errorf("setup codes repeat: %d distinct of 200", len(seen))
	}
}

type clock struct{ now time.Time }

func (c *clock) Now() time.Time          { return c.now }
func (c *clock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newTestLimiter() (*Limiter, *clock) {
	c := &clock{now: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)}
	return NewLimiter(c.Now), c
}

func failN(l *Limiter, keys []string, n int) {
	for range n {
		l.Fail(keys)
	}
}

func wantAllowed(t *testing.T, l *Limiter, keys []string, allowed bool, wait time.Duration) {
	t.Helper()
	ok, d := l.Allow(keys)
	if ok != allowed || d != wait {
		t.Errorf("Allow(%v) = %v, %v; want %v, %v", keys, ok, d, allowed, wait)
	}
}

func TestLimiterKeys(t *testing.T) {
	if k := Keys("alice", "192.0.2.1", false); !slices.Equal(k, []string{"u:alice", "ip:192.0.2.1"}) {
		t.Errorf("non-loopback keys %v", k)
	}
	if k := Keys("alice", "127.0.0.1", true); !slices.Equal(k, []string{"u:alice"}) {
		t.Errorf("loopback keys %v: loopback peers must not share an IP bucket", k)
	}
}

func TestLimiterLocksAfterFiveFailuresAndDoubles(t *testing.T) {
	l, c := newTestLimiter()
	keys := Keys("alice", "", true)
	failN(l, keys, 4)
	wantAllowed(t, l, keys, true, 0)
	l.Fail(keys)
	wantAllowed(t, l, keys, false, time.Second)
	c.Advance(999 * time.Millisecond)
	wantAllowed(t, l, keys, false, time.Millisecond)
	c.Advance(time.Millisecond)
	wantAllowed(t, l, keys, true, 0)
	for _, want := range []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second} {
		l.Fail(keys)
		wantAllowed(t, l, keys, false, want)
		c.Advance(want)
	}
}

func TestLimiterCapsTheLockAtFifteenMinutes(t *testing.T) {
	l, _ := newTestLimiter()
	keys := Keys("alice", "", true)
	failN(l, keys, 40)
	wantAllowed(t, l, keys, false, 15*time.Minute)
}

func TestLimiterRefusalsDoNotCount(t *testing.T) {
	l, c := newTestLimiter()
	keys := Keys("alice", "", true)
	failN(l, keys, 5)
	for range 100 {
		l.Allow(keys)
	}
	c.Advance(time.Second)
	wantAllowed(t, l, keys, true, 0)
}

func TestLimiterForgetsFailuresAfterFifteenMinutes(t *testing.T) {
	l, c := newTestLimiter()
	keys := Keys("alice", "", true)
	failN(l, keys, 5)
	c.Advance(15*time.Minute + time.Second)
	l.Fail(keys)
	wantAllowed(t, l, keys, true, 0)
	failN(l, keys, 3) // 4 in the window
	wantAllowed(t, l, keys, true, 0)
	// Failures inside the window still count.
	l2, c2 := newTestLimiter()
	failN(l2, keys, 4)
	c2.Advance(14*time.Minute + 59*time.Second)
	l2.Fail(keys)
	wantAllowed(t, l2, keys, false, time.Second)
}

func TestLimiterSuccessResetsUsernameAndIP(t *testing.T) {
	l, _ := newTestLimiter()
	keys := Keys("alice", "192.0.2.1", false)
	failN(l, keys, 4)
	l.Succeed(keys)
	failN(l, keys, 4)
	wantAllowed(t, l, keys, true, 0)
	wantAllowed(t, l, Keys("bob", "192.0.2.1", false), true, 0)
}

func TestLimiterBucketsAreSeparate(t *testing.T) {
	l, _ := newTestLimiter()
	failN(l, Keys("alice", "", true), 5)
	wantAllowed(t, l, Keys("bob", "", true), true, 0)
	// One non-loopback IP trying many usernames is locked as an IP.
	for _, u := range []string{"u1", "u2", "u3", "u4", "u5"} {
		l.Fail(Keys(u, "192.0.2.1", false))
	}
	wantAllowed(t, l, Keys("carol", "192.0.2.1", false), false, time.Second)
	wantAllowed(t, l, Keys("carol", "192.0.2.2", false), true, 0)
	wantAllowed(t, l, Keys("u1", "", true), true, 0)
}
