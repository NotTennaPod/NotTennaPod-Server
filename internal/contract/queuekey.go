package contract

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// QueueKey is the reduced dyadic rational M / 2^N strictly between 0 and 1 (PROTOCOL §3.6).
// The zero value is not a key; the sentinels 0 and 1 are represented by nil bounds.
type QueueKey struct {
	N uint
	M *big.Int
}

// MaxQueueKeyExponent bounds n (PROTOCOL §3).
const MaxQueueKeyExponent = 1 << 20

// ErrInvalidQueueKey reports a key that is not a valid contract-v1 queue key.
var ErrInvalidQueueKey = errors.New("contract: invalid queue key")

var (
	one    = big.NewInt(1)
	two129 = new(big.Int).Lsh(big.NewInt(1), 129)
	thresh = new(big.Int).Lsh(big.NewInt(1), 130)
)

// String returns the wire form "q1:<n>:<m hex>".
func (k QueueKey) String() string {
	return "q1:" + strconv.FormatUint(uint64(k.N), 10) + ":" + k.M.Text(16)
}

// ParseQueueKey parses and validates a wire key: odd M, no leading zeros, lowercase
// hex, and 0 < M/2^N < 1.
func ParseQueueKey(s string) (QueueKey, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 3 || parts[0] != "q1" {
		return QueueKey{}, ErrInvalidQueueKey
	}
	ns, ms := parts[1], parts[2]
	if ns == "" || (len(ns) > 1 && ns[0] == '0') || ms == "" || (len(ms) > 1 && ms[0] == '0') {
		return QueueKey{}, ErrInvalidQueueKey
	}
	for _, c := range ns {
		if c < '0' || c > '9' {
			return QueueKey{}, ErrInvalidQueueKey
		}
	}
	for _, c := range ms {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return QueueKey{}, ErrInvalidQueueKey
		}
	}
	n, err := strconv.ParseUint(ns, 10, 32)
	if err != nil || n > MaxQueueKeyExponent {
		return QueueKey{}, ErrInvalidQueueKey
	}
	m, ok := new(big.Int).SetString(ms, 16)
	if !ok || m.Bit(0) != 1 {
		return QueueKey{}, ErrInvalidQueueKey
	}
	// 0 < m / 2^n < 1  ⇔  0 < m < 2^n
	if m.Sign() <= 0 || m.Cmp(new(big.Int).Lsh(one, uint(n))) >= 0 {
		return QueueKey{}, ErrInvalidQueueKey
	}
	return QueueKey{N: uint(n), M: m}, nil
}

// CompareQueueKeys compares two keys numerically: -1, 0 or +1.
func CompareQueueKeys(a, b QueueKey) int {
	left := new(big.Int).Lsh(a.M, b.N)
	right := new(big.Int).Lsh(b.M, a.N)
	return left.Cmp(right)
}

// boundNumerator returns bound * 2^denomExp as an integer, for a bound that is a key
// (with N <= denomExp) or a sentinel (nil low = 0, nil high = 1).
func boundNumerator(bound *QueueKey, sentinel int64, denomExp uint) *big.Int {
	if bound == nil {
		return new(big.Int).Lsh(big.NewInt(sentinel), denomExp)
	}
	return new(big.Int).Lsh(bound.M, denomExp-bound.N)
}

// QueueKeyBetween allocates the key strictly between low and high (nil means the
// sentinels 0 and 1) for the persisted 128-bit position-write ID u (PROTOCOL §3.6):
// the smallest n >= 1 with (high-low)*2^n > 2^130, then the smallest m > low*2^n
// with m ≡ 2u+1 (mod 2^129).
func QueueKeyBetween(low, high *QueueKey, u [16]byte) (QueueKey, error) {
	var denom uint // common denominator exponent of low and high
	if low != nil {
		denom = low.N
	}
	if high != nil && high.N > denom {
		denom = high.N
	}
	lowNum := boundNumerator(low, 0, denom)
	highNum := boundNumerator(high, 1, denom)
	diff := new(big.Int).Sub(highNum, lowNum) // (high - low) * 2^denom
	if diff.Sign() <= 0 {
		return QueueKey{}, fmt.Errorf("contract: queue bounds not increasing: %w", ErrInvalidQueueKey)
	}
	// Smallest n >= 1 with diff * 2^n > 2^(130 + denom).
	target := new(big.Int).Lsh(thresh, denom)
	// diff < 2^bitlen, so no n below 130+denom-bitlen+1 can qualify; start there
	// (the loop then runs at most twice).
	n := uint(1)
	if start := int(130+denom) - diff.BitLen() + 1; start > 1 {
		n = uint(start)
	}
	for new(big.Int).Lsh(diff, n).Cmp(target) <= 0 {
		n++
	}
	if n > MaxQueueKeyExponent {
		return QueueKey{}, fmt.Errorf("contract: queue key exponent bound exceeded: %w", ErrInvalidQueueKey)
	}
	// base = floor(low * 2^n) + 1, with low * 2^n = lowNum * 2^n / 2^denom.
	base := new(big.Int).Lsh(lowNum, n)
	base.Rsh(base, denom) // floor division by 2^denom for non-negative values
	base.Add(base, one)
	r := new(big.Int).SetBytes(u[:])
	r.Lsh(r, 1).Add(r, one) // 2u + 1
	// m = base + ((r - base) mod 2^129)
	delta := new(big.Int).Sub(r, base)
	delta.Mod(delta, two129) // Euclidean: non-negative
	m := new(big.Int).Add(base, delta)
	key := QueueKey{N: n, M: m}
	// Postconditions (cheap, exact): low < key < high.
	if low != nil && CompareQueueKeys(*low, key) >= 0 || high != nil && CompareQueueKeys(key, *high) >= 0 || m.Sign() <= 0 {
		return QueueKey{}, fmt.Errorf("contract: allocator postcondition failed: %w", ErrInvalidQueueKey)
	}
	return key, nil
}

// QueueKeyCarries reports whether key carries position ID u (PROTOCOL §3.6: the
// allocator chooses m with m ≡ 2u+1 mod 2^129), so a key cannot be claimed for
// another position write.
func QueueKeyCarries(key QueueKey, u [16]byte) bool {
	r := new(big.Int).SetBytes(u[:])
	r.Lsh(r, 1).Add(r, one)
	m := new(big.Int).Mod(key.M, two129)
	return m.Cmp(r) == 0
}
