package feed

import (
	"encoding/base64"
	"encoding/binary"
	"sort"
	"sync"
	"unicode/utf8"
)

// Time zones of zone names, from the tzdata rules in datezones.go: only the
// listed transitions (to 2037), nothing beyond. A nil *dateZone is GMT.

// dateZone is one zone's rules, decoded.
type dateZone struct {
	id       string
	times    []int64 // transition times, seconds since the epoch, ascending
	raws     []int64 // the standard offset in force from each transition, ms
	dsts     []int64 // the daylight saving in force from each transition, ms
	daylight []bool  // whether each transition is into a daylight type
	raw      int64   // the zone's raw offset: of its latest standard transition, ms
	earliest int64   // the offset before the first transition, ms

	// The latest transition into daylight time and the saving it brought
	// against the latest standard type; hasSaving is false without either.
	lastDaylight int64
	saving       int64
	hasSaving    bool
}

// dateZones decodes every zone of dateZoneRules on first use.
var dateZones = sync.OnceValue(func() map[string]*dateZone {
	zones := make(map[string]*dateZone, len(dateZoneRules))
	for id, rule := range dateZoneRules {
		zones[id] = decodeDateZone(id, rule)
	}
	return zones
})

// dateZoneByID is a zone of dateZoneRules, or nil.
func dateZoneByID(id string) *dateZone { return dateZones()[id] }

func decodeDateZone(id string, rule dateZoneRule) *dateZone {
	b, err := base64.StdEncoding.DecodeString(rule.trans)
	if err != nil {
		panic("feed: bad transitions of zone " + id)
	}
	offset := func(typ int) int64 { return int64(rule.offsets[typ]) * 1000 }
	isDaylight := func(typ int) bool { return rule.isDst[typ] == '1' }

	z := &dateZone{id: id}
	var types []int
	for at := int64(0); len(b) > 0; {
		delta, n := binary.Varint(b)
		if n <= 0 || n >= len(b) || int(b[n]) >= len(rule.offsets) {
			panic("feed: bad transitions of zone " + id)
		}
		at += delta
		z.times = append(z.times, at)
		types = append(types, int(b[n]))
		b = b[n+1:]
	}

	z.raw = offset(0)
	lastStandard := -1
	for i, typ := range types {
		if !isDaylight(typ) {
			z.raw, lastStandard = offset(typ), i
		}
	}
	z.earliest = z.raw
	for typ := range rule.offsets {
		if !isDaylight(typ) {
			z.earliest = offset(typ)
			break
		}
	}

	raw := z.earliest
	lastDaylight := -1
	for i, typ := range types {
		off := offset(typ)
		if isDaylight(typ) {
			z.raws = append(z.raws, raw)
			z.dsts = append(z.dsts, off-raw)
			lastDaylight = i
		} else {
			raw = off
			z.raws = append(z.raws, off)
			z.dsts = append(z.dsts, 0)
		}
		z.daylight = append(z.daylight, isDaylight(typ))
	}
	if lastDaylight >= 0 && lastStandard >= 0 {
		z.hasSaving = true
		z.lastDaylight = z.times[lastDaylight]
		z.saving = offset(types[lastDaylight]) - offset(types[lastStandard])
	}
	return z
}

// rawOffset is the zone's raw offset (0 for GMT).
func (z *dateZone) rawOffset() int64 {
	if z == nil {
		return 0
	}
	return z.raw
}

// transitionAt is the index of the last transition at or before the instant u
// (ms), or -1.
func (z *dateZone) transitionAt(u int64) int {
	sec := dateFloorDiv(u, 1000)
	return sort.Search(len(z.times), func(i int) bool { return z.times[i] > sec }) - 1
}

// offsetsAt is the standard offset and the daylight saving in force at the
// instant u (ms).
func (z *dateZone) offsetsAt(u int64) (raw, dst int64) {
	if z == nil {
		return 0, 0
	}
	i := z.transitionAt(u)
	if i < 0 {
		return z.earliest, 0
	}
	return z.raws[i], z.dsts[i]
}

// daylightAt reports daylight time at the instant u (ms).
func (z *dateZone) daylightAt(u int64) bool {
	if z == nil {
		return false
	}
	i := z.transitionAt(u)
	return i >= 0 && z.daylight[i]
}

// savings is the zone's daylight saving as of now (ms): that of its latest
// daylight transition if that is not before now (rounded up to a second),
// else 0.
func (z *dateZone) savings(now int64) int64 {
	if !z.hasSaving {
		return 0
	}
	sec := now / 1000
	if now > 0 {
		sec = (now + 999) / 1000 // wraps for now within a second of the maximum
	}
	if z.lastDaylight < sec {
		return 0
	}
	return z.saving
}

// dateZoneNameMaxRunes bounds the length of a zone name in code points.
const dateZoneNameMaxRunes = 64

// dateZoneNameKeys are the zone names as fold keys (dateFoldKey) of their
// code points, in dateZoneNames' order.
var dateZoneNameKeys = sync.OnceValue(func() [][]rune {
	keys := make([][]rune, len(dateZoneNames))
	for i, n := range dateZoneNames {
		if utf8.RuneCountInString(n.name) > dateZoneNameMaxRunes || n.name == "" {
			panic("feed: zone name too long: " + n.name)
		}
		for _, r := range n.name {
			keys[i] = append(keys[i], dateFoldKey(r))
		}
	}
	return keys
})
