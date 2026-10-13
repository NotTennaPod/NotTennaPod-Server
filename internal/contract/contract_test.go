package contract

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"math/big"
	mrand "math/rand/v2"
	"sort"
	"strings"
	"testing"
)

func TestNormalizeURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"HTTPS://Example.COM/Feed.xml", "https://example.com/Feed.xml"},
		{"http://example.com:80/a", "http://example.com/a"},
		{"https://example.com:443/a", "https://example.com/a"},
		{"http://example.com:443/a", "http://example.com:443/a"},
		{"https://example.com:8443/a", "https://example.com:8443/a"},
		{"https://example.com:/a", "https://example.com/a"},
		{"https://example.com:0443/a", "https://example.com:0443/a"},
		{"https://User:Pa@ss@Host.Example/x", "https://User:Pa@ss@host.example/x"},
		{"http://[2001:DB8::1]:80/x", "http://[2001:db8::1]/x"},
		{"http://[2001:DB8::1]:8080/x", "http://[2001:db8::1]:8080/x"},
		{"https://example.com/a?b=1#frag?x", "https://example.com/a?b=1"},
		{"https://example.com#top", "https://example.com"},
		{"https://example.com/a/%7e/../b/?Q=%2F&q=1", "https://example.com/a/%7e/../b/?Q=%2F&q=1"},
		{"https://Exämple.COM/x", "https://exämple.com/x"},
		{"feed://Example.com/rss", "feed://example.com/rss"},
		{"itpc:Example.com/rss#x", "itpc:Example.com/rss"},
		{"example.com/feed.xml", "example.com/feed.xml"},
		{"", ""},
		{"https://example.com", "https://example.com"},
		{"https://example.com?x", "https://example.com?x"},
		{"https://%4A.EXAMPLE/audio", "https://%4A.example/audio"},
		{"https://%4a.EXAMPLE/%4A", "https://%4a.example/%4A"},
		{"https://A%4.EXAMPLE/x", "https://a%4.example/x"},
		{"https://[2001:DB8::1]:443/audio", "https://[2001:db8::1]/audio"},
		{"https://[2001:DB8::1]:/audio", "https://[2001:db8::1]/audio"},
		{"https://[2001:DB8::1]/audio", "https://[2001:db8::1]/audio"},
		{"https://[::1/x", "https://[::1/x"},
		{"https://[::A]x:80/x", "https://[::a]x:80/x"},
		{"mailto:Someone@Example.com#x", "mailto:Someone@Example.com"},
	}
	for _, c := range cases {
		if got := NormalizeURL(c.in); got != c.want {
			t.Errorf("NormalizeURL(%q) = %q, want %q", c.in, got, c.want)
		}
		if again := NormalizeURL(NormalizeURL(c.in)); again != NormalizeURL(c.in) {
			t.Errorf("not idempotent for %q: %q", c.in, again)
		}
	}
}

func TestTrimGUID(t *testing.T) {
	cases := []struct {
		in, want string
		ok       bool
	}{
		{"  abc  ", "abc", true},
		{"\t\n\x00abc def\x1f ", "abc def", true},
		{" abc ", " abc ", true},
		{"null", "", false},
		{" null ", "", false},
		{"NULL", "NULL", true},
		{"   ", "", false},
		{"", "", false},
		{"ABC", "ABC", true},
	}
	for _, c := range cases {
		got, ok := TrimGUID(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("TrimGUID(%q) = %q,%v want %q,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestIDsAreDistinctByKind(t *testing.T) {
	ns, op := "8b1f6a39-4f5c-4d2e-9c1a-1b2c3d4e5f60", "0f0e0d0c-0b0a-4908-8706-050403020100"
	p := PodcastID(ns, op)
	a, _ := EpisodeID(ns, p, KindRSSGUID, "x")
	b, _ := EpisodeID(ns, p, KindAtomID, "x")
	c, _ := EpisodeID(ns, p, KindEnclosure, "x")
	if a == b || b == c || a == c {
		t.Fatal("kinds must separate identities")
	}
	if _, err := EpisodeID(ns, p, "other", "x"); !errors.Is(err, ErrUnknownKind) {
		t.Fatal("unknown kind accepted")
	}
	// L() prevents boundary ambiguity: ("ab","c") vs ("a","bc").
	if string(LengthPrefixed("ab", "c")) == string(LengthPrefixed("a", "bc")) {
		t.Fatal("length prefix ambiguity")
	}
}

func randomU(r *mrand.Rand) [16]byte {
	var u [16]byte
	for i := range u {
		u[i] = byte(r.IntN(256))
	}
	return u
}

func TestQueueKeyAllocatorOrderingAndUniqueness(t *testing.T) {
	r := mrand.New(mrand.NewPCG(1, 2))
	var keys []QueueKey
	seen := map[string]bool{}
	for i := 0; i < 3000; i++ {
		pos := r.IntN(len(keys) + 1)
		var low, high *QueueKey
		if pos > 0 {
			low = &keys[pos-1]
		}
		if pos < len(keys) {
			high = &keys[pos]
		}
		k, err := QueueKeyBetween(low, high, randomU(r))
		if err != nil {
			t.Fatal(err)
		}
		if seen[k.String()] {
			t.Fatalf("duplicate key %s", k)
		}
		seen[k.String()] = true
		parsed, err := ParseQueueKey(k.String())
		if err != nil || CompareQueueKeys(parsed, k) != 0 {
			t.Fatalf("round trip failed for %s: %v", k, err)
		}
		keys = append(keys, QueueKey{})
		copy(keys[pos+1:], keys[pos:])
		keys[pos] = k
	}
	if !sort.SliceIsSorted(keys, func(i, j int) bool { return CompareQueueKeys(keys[i], keys[j]) < 0 }) {
		t.Fatal("keys not strictly ordered")
	}
}

func TestQueueKeySmallestGapRepeated(t *testing.T) {
	r := mrand.New(mrand.NewPCG(3, 4))
	low, err := QueueKeyBetween(nil, nil, randomU(r))
	if err != nil {
		t.Fatal(err)
	}
	high, err := QueueKeyBetween(&low, nil, randomU(r))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		mid, err := QueueKeyBetween(&low, &high, randomU(r))
		if err != nil {
			t.Fatal(err)
		}
		if CompareQueueKeys(low, mid) >= 0 || CompareQueueKeys(mid, high) >= 0 {
			t.Fatalf("iteration %d: not between", i)
		}
		if i%2 == 0 {
			high = mid
		} else {
			low = mid
		}
	}
}

func TestQueueKeySameIDDeterministicAndDistinctIDsDiffer(t *testing.T) {
	var u1, u2 [16]byte
	_, _ = rand.Read(u1[:])
	u2 = u1
	u2[15] ^= 1
	a, _ := QueueKeyBetween(nil, nil, u1)
	b, _ := QueueKeyBetween(nil, nil, u1)
	c, _ := QueueKeyBetween(nil, nil, u2)
	if a.String() != b.String() {
		t.Fatal("same position ID must give the same key")
	}
	if a.String() == c.String() {
		t.Fatal("distinct position IDs gave the same key")
	}
}

func TestParseQueueKeyRejects(t *testing.T) {
	bad := []string{"", "q1:1", "q2:1:1", "q1:01:1", "q1:1:01", "q1:1:2", "q1:1:A", "q1:0:1", "q1:1:3", "q1:-1:1", "q1:1:1:1"}
	for _, s := range bad {
		if _, err := ParseQueueKey(s); err == nil {
			t.Errorf("accepted %q", s)
		}
	}
	if k, err := ParseQueueKey("q1:1:1"); err != nil || k.M.Cmp(big.NewInt(1)) != 0 {
		t.Fatal("q1:1:1 (= 1/2) must parse")
	}
}

func TestCausalOrderIsTopologicalAndPermutationInvariant(t *testing.T) {
	// a(10) <- c(5): dependency beats time. b(7) is independent.
	facts := []OrderedFact{
		{ID: "c", Dependencies: []string{"a"}, NormalizedTime: 5},
		{ID: "a", NormalizedTime: 10},
		{ID: "b", NormalizedTime: 7},
	}
	want := []string{"b", "a", "c"}
	r := mrand.New(mrand.NewPCG(5, 6))
	for i := 0; i < 50; i++ {
		r.Shuffle(len(facts), func(i, j int) { facts[i], facts[j] = facts[j], facts[i] })
		got, err := CausalOrder(append([]OrderedFact(nil), facts...))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
			t.Fatalf("order %v, want %v", got, want)
		}
	}
	// A pairwise comparator (time, then dependency) would be non-transitive here:
	// x(1) depends on z(3); y(2) is independent. Kahn order: y, z, x.
	got, err := CausalOrder([]OrderedFact{{ID: "x", Dependencies: []string{"z"}, NormalizedTime: 1}, {ID: "y", NormalizedTime: 2}, {ID: "z", NormalizedTime: 3}})
	if err != nil || got[0] != "y" || got[1] != "z" || got[2] != "x" {
		t.Fatalf("got %v %v", got, err)
	}
	if _, err := CausalOrder([]OrderedFact{{ID: "x", Dependencies: []string{"missing"}}}); !errors.Is(err, ErrDependencyMissing) {
		t.Fatal("missing dependency not reported")
	}
	if _, err := CausalOrder([]OrderedFact{{ID: "x", Dependencies: []string{"y"}}, {ID: "y", Dependencies: []string{"x"}}}); !errors.Is(err, ErrDependencyCycle) {
		t.Fatal("cycle not reported")
	}
}

func TestNormalizeTime(t *testing.T) {
	cases := []struct {
		client, receive, want int64
		deps                  []int64
	}{
		{client: 100, receive: 200, want: 100},
		{client: 300, receive: 200, want: 200},
		{client: 50, receive: 200, deps: []int64{80, 60}, want: 80},
		{client: 50, receive: 70, deps: []int64{80}, want: 80}, // server clock behind dependencies
	}
	for _, c := range cases {
		if got := NormalizeTime(c.client, c.receive, c.deps); got != c.want {
			t.Errorf("NormalizeTime(%d,%d,%v) = %d want %d", c.client, c.receive, c.deps, got, c.want)
		}
	}
}

func TestCanonicalJSON(t *testing.T) {
	cases := []struct{ in, want string }{
		{`{"b":1,"a":[true,null,"x"]}`, `{"a":[true,null,"x"],"b":1}`},
		{`{"€":1,"😀":2,"z":3}`, `{"z":3,"€":1,"😀":2}`},
		{`"a\u0001\b\"\\/ "`, "\"a\\u0001\\b\\\"\\\\/ \""},
		{`9007199254740991`, `9007199254740991`},
		{`-5`, `-5`},
	}
	for _, c := range cases {
		v, err := DecodeJSON([]byte(c.in))
		if err != nil {
			t.Fatal(err)
		}
		got, err := Canonical(v)
		if err != nil || string(got) != c.want {
			t.Errorf("Canonical(%s) = %s (%v), want %s", c.in, got, err, c.want)
		}
	}
	if v, err := DecodeJSON([]byte(`"\ud83d\ude00"`)); err != nil || v != "😀" {
		t.Errorf("surrogate pair: %v %v", v, err)
	}
	deep := strings.Repeat("[", MaxDepth) + strings.Repeat("]", MaxDepth)
	if _, err := DecodeJSON([]byte(deep)); err != nil {
		t.Errorf("depth %d rejected: %v", MaxDepth, err)
	}
	for _, bad := range []string{`1.5`, `1e3`, `-0`, `9007199254740992`, `{"a":1} x`, `{"a":1,"a":2}`, `"\ud800"`, `"\udc00\ud800"`, `"\ud800\u0041"`, "\"a\x01\"", `01`, `+1`, `"\x"`, `[1,]`, `{"a":1,}`, `tru`, "[" + deep + "]", ``, `"abc`} {
		v, err := DecodeJSON([]byte(bad))
		if err == nil {
			_, err = Canonical(v)
		}
		if err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestCursorRoundTripAndMAC(t *testing.T) {
	c := Cursor{LogID: "6a1f0e2c-1111-4222-8333-944455556666", Epoch: 2, Seq: 41, Digest: make([]byte, 32)}
	c.Digest[0] = 0xab
	s, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeCursor(s)
	if err != nil || back.Seq != 41 || back.Epoch != 2 || back.Digest[0] != 0xab || back.LogID != c.LogID {
		t.Fatalf("round trip: %+v %v", back, err)
	}
	if _, err := DecodeCursor(s + "A"); err == nil {
		t.Fatal("tampered cursor accepted")
	}
	key := []byte("0123456789abcdef0123456789abcdef")
	receipt := map[string]any{"op_id": "x", "seq": json.Number("3")}
	mac, err := SignedMAC(key, "acct-1", receipt)
	if err != nil {
		t.Fatal(err)
	}
	receipt["mac"] = mac
	if !VerifyMAC(key, "acct-1", receipt) || VerifyMAC(key, "acct-2", receipt) {
		t.Fatal("MAC must bind the account")
	}
	receipt["seq"] = json.Number("4")
	if VerifyMAC(key, "acct-1", receipt) {
		t.Fatal("MAC must cover the payload")
	}
}
