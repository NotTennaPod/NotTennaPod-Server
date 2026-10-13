// Command gendatezones writes internal/feed/datezones.go: the Locale.US date
// symbols and the time zone data that Android 16's SimpleDateFormat parses
// zone names with. The symbols come from contract/vectors/feed_date_symbols.json,
// dumped on the API 36 emulator. Zone names are ICU's zone/en.txt over
// zone/root.txt and the metazone mappings misc/metaZones.txt
// (platform/external/icu); zone rules are Android's tzdata file
// (platform/system/timezone). The emulator's zone strings and raw offsets in the
// dump are checked against the derived data.
//
//	go run ./internal/gendatezones -out datezones.go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// The android16-release heads of platform/external/icu and platform/system/timezone.
const (
	icuRef  = "f17caeafcf20bd38074a9963c31df3629b70b5f5"
	tzRef   = "3584420250d8375c82809f0c20782844bedd3629"
	icuBase = "https://android.googlesource.com/platform/external/icu/+/" + icuRef + "/icu4c/source/data/"
	tzBase  = "https://android.googlesource.com/platform/system/timezone/+/" + tzRef + "/output_data/iana/"
)

// sources maps a -src file name to its AOSP path.
var sources = map[string]string{
	"en.txt":            icuBase + "zone/en.txt",
	"root.txt":          icuBase + "zone/root.txt",
	"metaZones.txt":     icuBase + "misc/metaZones.txt",
	"timezoneTypes.txt": icuBase + "misc/timezoneTypes.txt",
	"tzdata":            tzBase + "tzdata",
}

// dumpTime is the emulator run, for picking each zone's current metazone.
const dumpTime = "2026-10-05 19:06"

func main() {
	src := flag.String("src", "", "directory with en.txt, root.txt, metaZones.txt, timezoneTypes.txt and tzdata (default: fetch from AOSP)")
	symbolsPath := flag.String("symbols", "../../contract/vectors/feed_date_symbols.json", "the emulator's DateFormatSymbols dump")
	out := flag.String("out", "datezones.go", "output file")
	flag.Parse()

	hashes := map[string]string{}
	read := func(name string) []byte {
		var b []byte
		if *src != "" {
			var err error
			if b, err = os.ReadFile(filepath.Join(*src, name)); err != nil {
				log.Fatal(err)
			}
		} else {
			resp, err := http.Get(sources[name] + "?format=TEXT")
			if err != nil {
				log.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Fatalf("%s: %s", name, resp.Status)
			}
			if b, err = io.ReadAll(base64.NewDecoder(base64.StdEncoding, resp.Body)); err != nil {
				log.Fatal(err)
			}
		}
		sum := sha256.Sum256(b)
		hashes[name] = fmt.Sprintf("%x", sum)
		return b
	}

	var sym struct {
		Weekdays      []string   `json:"weekdays"`
		ShortWeekdays []string   `json:"short_weekdays"`
		Months        []string   `json:"months"`
		ShortMonths   []string   `json:"short_months"`
		ZoneStrings   [][]string `json:"zone_strings"`
		AvailableIDs  [][]any    `json:"available_ids"`
		Fingerprint   string     `json:"fingerprint"`
	}
	b, err := os.ReadFile(*symbolsPath)
	if err != nil {
		log.Fatal(err)
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(&sym); err != nil {
		log.Fatal(err)
	}

	names := zoneNames(read("root.txt"), read("en.txt"))
	meta := parseResource(read("metaZones.txt"))
	types := parseResource(read("timezoneTypes.txt"))
	tzdata := parseTzdata(read("tzdata"))

	mapTZ := meta.table("metaZones").table("mapTimezones")
	info := meta.table("metaZones").table("metazoneInfo")
	aliases := types.table("timezoneTypes").table("typeAlias").table("timezone")

	// Searchable names: long and short, standard and daylight (ExtendedTimeZoneNames).
	type nameRow struct {
		name   string
		dst    bool
		tz, mz string
	}
	var rows []nameRow
	seen := map[string]bool{}
	refs := map[string]string{}
	zones := map[string]bool{"Etc/GMT": true}
	for key, n := range names {
		for _, t := range []string{"ls", "ss", "ld", "sd"} {
			v, ok := n[t]
			if !ok {
				continue
			}
			if seen[v] {
				log.Fatalf("name %q is used twice; matching would depend on ICU's trie order", v)
			}
			seen[v] = true
			r := nameRow{name: v, dst: t[1] == 'd'}
			if mz, ok := strings.CutPrefix(key, "meta:"); ok {
				r.mz = mz
				m := mapTZ.table(mz)
				ref := m.str("US")
				if ref == "" {
					ref = m.str("001")
				}
				if ref == "" {
					log.Fatalf("metazone %s has no reference zone", mz)
				}
				refs[mz] = ref
				zones[ref] = true
			} else {
				r.tz = strings.ReplaceAll(key, ":", "/")
				zones[r.tz] = true
			}
			rows = append(rows, r)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })

	// Each zone's metazones (getAvailableMetaZoneIDs), keeping those with names.
	zoneMetas := map[string][]string{}
	for z := range zones {
		set := map[string]bool{}
		for _, e := range info.table(strings.ReplaceAll(z, "/", ":")).list() {
			if mz := e.list()[0].s; refs[mz] != "" {
				set[mz] = true
			}
		}
		for mz := range set {
			zoneMetas[z] = append(zoneMetas[z], mz)
		}
		sort.Strings(zoneMetas[z])
	}

	// Check the emulator's zone strings: every name that is not a GMT offset
	// fallback is the zone's own name or its current metazone's name.
	checked := 0
	for _, row := range sym.ZoneStrings {
		id := row[0]
		canon := id
		if a := aliases.str(strings.ReplaceAll(id, "/", ":")); a != "" {
			canon = a
		}
		mz := ""
		for _, e := range info.table(strings.ReplaceAll(canon, "/", ":")).list() {
			l := e.list()
			if len(l) == 1 || (l[1].s <= dumpTime && dumpTime < l[2].s) {
				mz = l[0].s
			}
		}
		for i, t := range []string{"ls", "ss", "ld", "sd"} {
			got := row[i+1]
			if strings.HasPrefix(got, "GMT+") || strings.HasPrefix(got, "GMT-") {
				continue
			}
			want := names[strings.ReplaceAll(canon, "/", ":")][t]
			if want == "" && mz != "" {
				want = names["meta:"+mz][t]
			}
			if got != want {
				log.Fatalf("zone strings of %s (%s): emulator %q, ICU data %q", id, t, got, want)
			}
			checked++
		}
	}

	// Zone rules, checked against the emulator's raw offsets.
	rawOffsets := map[string]int64{}
	for _, p := range sym.AvailableIDs {
		v, _ := p[1].(json.Number).Int64()
		rawOffsets[p[0].(string)] = v
	}
	var ids []string
	for z := range zones {
		ids = append(ids, z)
	}
	sort.Strings(ids)
	rules := map[string]tzif{}
	for _, z := range ids {
		r, ok := tzdata.zones[z]
		if !ok {
			log.Fatalf("%s is not in tzdata", z)
		}
		want, ok := rawOffsets[z]
		if !ok {
			log.Fatalf("%s is not among the emulator's time zone IDs", z)
		}
		if got := r.rawOffset() * 1000; got != want {
			log.Fatalf("%s: raw offset %d in tzdata, %d on the emulator", z, got, want)
		}
		rules[z] = r
	}

	var w bytes.Buffer
	fmt.Fprintf(&w, "// Code generated by gendatezones from contract/vectors/feed_date_symbols.json\n")
	fmt.Fprintf(&w, "// (%s), AOSP platform/external/icu %s\n", sym.Fingerprint, icuRef)
	fmt.Fprintf(&w, "// (zone/en.txt, zone/root.txt, misc/metaZones.txt) and platform/system/timezone\n")
	fmt.Fprintf(&w, "// %s (output_data/iana/tzdata, %s). DO NOT EDIT.\n", tzRef, tzdata.version)
	var hs []string
	for name, h := range hashes {
		hs = append(hs, name+" "+h)
	}
	sort.Strings(hs)
	for _, h := range hs {
		fmt.Fprintf(&w, "// sha256 %s\n", h)
	}
	fmt.Fprintf(&w, "// Checked: %d zone strings and %d raw offsets of the emulator.\n\n", checked, len(ids))
	fmt.Fprintf(&w, "package feed\n\n")
	fmt.Fprintf(&w, "// Locale.US DateFormatSymbols on Android 16.\nvar (\n")
	fmt.Fprintf(&w, "dateWeekdays = %#v\n", sym.Weekdays)
	fmt.Fprintf(&w, "dateShortWeekdays = %#v\n", sym.ShortWeekdays)
	fmt.Fprintf(&w, "dateMonths = %#v\n", sym.Months)
	fmt.Fprintf(&w, "dateShortMonths = %#v\n)\n\n", sym.ShortMonths)

	fmt.Fprintf(&w, "// dateZoneNames are ICU's English standard and daylight zone names, long\n")
	fmt.Fprintf(&w, "// and short, with their own zone or their metazone.\n")
	fmt.Fprintf(&w, "var dateZoneNames = [...]dateZoneName{\n")
	for _, r := range rows {
		fmt.Fprintf(&w, "{%q, %v, %q, %q},\n", r.name, r.dst, r.tz, r.mz)
	}
	fmt.Fprintf(&w, "}\n\n")

	fmt.Fprintf(&w, "// dateMetaZoneRefs are the reference zones of the metazones for region US,\n")
	fmt.Fprintf(&w, "// else 001 (TimeZoneNames.getReferenceZoneID).\n")
	fmt.Fprintf(&w, "var dateMetaZoneRefs = map[string]string{\n")
	var mzs []string
	for mz := range refs {
		mzs = append(mzs, mz)
	}
	sort.Strings(mzs)
	for _, mz := range mzs {
		fmt.Fprintf(&w, "%q: %q,\n", mz, refs[mz])
	}
	fmt.Fprintf(&w, "}\n\n")

	fmt.Fprintf(&w, "// dateZoneMetaZones are the named metazones each zone has used\n")
	fmt.Fprintf(&w, "// (TimeZoneNames.getAvailableMetaZoneIDs).\n")
	fmt.Fprintf(&w, "var dateZoneMetaZones = map[string][]string{\n")
	for _, z := range ids {
		if len(zoneMetas[z]) > 0 {
			fmt.Fprintf(&w, "%q: %#v,\n", z, zoneMetas[z])
		}
	}
	fmt.Fprintf(&w, "}\n\n")

	fmt.Fprintf(&w, "// dateZoneRules are the TZif rules of those zones in Android's tzdata: the\n")
	fmt.Fprintf(&w, "// types' UTC offsets in seconds, their DST flags, and the transitions as\n")
	fmt.Fprintf(&w, "// base64 of (zigzag varint delta seconds, type index) pairs.\n")
	fmt.Fprintf(&w, "var dateZoneRules = map[string]dateZoneRule{\n")
	for _, z := range ids {
		r := rules[z]
		var dst strings.Builder
		for _, v := range r.isDst {
			dst.WriteByte('0' + v)
		}
		var enc []byte
		prev := int64(0)
		for i, t := range r.transitions {
			enc = binary.AppendVarint(enc, t-prev)
			enc = append(enc, r.types[i])
			prev = t
		}
		fmt.Fprintf(&w, "%q: {%#v, %q, %q},\n", z, r.offsets, dst.String(), base64.StdEncoding.EncodeToString(enc))
	}
	fmt.Fprintf(&w, "}\n")

	src2, err := format.Source(w.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(*out, src2, 0o644); err != nil {
		log.Fatal(err)
	}
}

// zoneNames returns each zoneStrings key's names, en over root
// (ICUResourceBundle.getAllItemsWithFallback).
func zoneNames(root, en []byte) map[string]map[string]string {
	out := map[string]map[string]string{}
	for _, b := range [][]byte{root, en} {
		r := parseResource(b)
		var top *resource
		for _, k := range r.keys {
			top = r.tab[k]
		}
		zs := top.table("zoneStrings")
		for _, k := range zs.keys {
			v := zs.tab[k]
			if v.tab == nil {
				continue
			}
			if out[k] == nil {
				out[k] = map[string]string{}
			}
			for _, t := range []string{"ls", "ss", "ld", "sd", "lg", "sg"} {
				if s := v.str(t); s != "" {
					out[k][t] = s
				}
			}
		}
	}
	return out
}

// resource is a node of an ICU resource bundle source file: a string, an
// array or a table.
type resource struct {
	s    string
	arr  []*resource
	keys []string
	tab  map[string]*resource
}

func (r *resource) table(k string) *resource {
	if r == nil || r.tab == nil {
		return nil
	}
	return r.tab[k]
}

func (r *resource) str(k string) string {
	if v := r.table(k); v != nil {
		return v.s
	}
	return ""
}

func (r *resource) list() []*resource {
	if r == nil {
		return nil
	}
	return r.arr
}

type token struct {
	kind byte // '{', '}', ',', '"' (string) or 'w' (bare word)
	s    string
}

func tokenize(b []byte) []token {
	s := strings.TrimPrefix(string(b), "\uFEFF")
	var toks []token
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case strings.HasPrefix(s[i:], "//"):
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case c == '{' || c == '}' || c == ',':
			toks = append(toks, token{kind: c})
			i++
		case c == '"':
			var sb strings.Builder
			i++
			for s[i] != '"' {
				if s[i] == '\\' {
					if s[i+1] == 'u' {
						v, err := strconv.ParseUint(s[i+2:i+6], 16, 32)
						if err != nil {
							log.Fatal(err)
						}
						sb.WriteRune(rune(v))
						i += 6
						continue
					}
					sb.WriteByte(s[i+1])
					i += 2
					continue
				}
				sb.WriteByte(s[i])
				i++
			}
			i++
			toks = append(toks, token{kind: '"', s: sb.String()})
		default:
			j := i
			for j < len(s) && !strings.ContainsRune(" \t\r\n{},\"", rune(s[j])) {
				j++
			}
			toks = append(toks, token{kind: 'w', s: s[i:j]})
			i = j
		}
	}
	return toks
}

// parseResource parses a whole file as a table of its top-level entries.
func parseResource(b []byte) *resource {
	toks := tokenize(b)
	root := &resource{tab: map[string]*resource{}}
	for i := 0; i < len(toks); {
		k := strings.SplitN(toks[i].s, ":", 2)[0]
		v, n := parseBraced(toks, i+1)
		root.keys = append(root.keys, k)
		root.tab[k] = v
		i = n
	}
	return root
}

// parseBraced parses the value that starts with the '{' at toks[i] and
// returns it with the index after its '}'.
func parseBraced(toks []token, i int) (*resource, int) {
	if toks[i].kind != '{' {
		log.Fatalf("expected {, got %q", toks[i].s)
	}
	i++
	r := &resource{}
	for toks[i].kind != '}' {
		t := toks[i]
		switch {
		case t.kind == ',':
			i++
		case t.kind == '{':
			v, n := parseBraced(toks, i)
			r.arr = append(r.arr, v)
			i = n
		case toks[i+1].kind == '{':
			if r.tab == nil {
				r.tab = map[string]*resource{}
			}
			v, n := parseBraced(toks, i+1)
			k := strings.SplitN(t.s, ":table", 2)[0]
			r.keys = append(r.keys, k)
			r.tab[k] = v
			i = n
		default:
			r.arr = append(r.arr, &resource{s: t.s})
			i++
		}
	}
	if len(r.arr) == 1 && r.tab == nil && r.arr[0].arr == nil && r.arr[0].tab == nil {
		r.s = r.arr[0].s
	}
	return r, i + 1
}

// tzif is a zone's 64-bit TZif data as ZoneInfoData reads it.
type tzif struct {
	transitions []int64
	types       []byte
	offsets     []int32
	isDst       []byte
}

// rawOffset is ZoneInfoData's raw offset in seconds: the offset of the last
// standard-time transition.
func (z tzif) rawOffset() int64 {
	if len(z.transitions) == 0 {
		return int64(z.offsets[0])
	}
	for i := len(z.transitions) - 1; i >= 0; i-- {
		if z.isDst[z.types[i]] == 0 {
			return int64(z.offsets[z.types[i]])
		}
	}
	log.Fatal("no standard-time transition")
	return 0
}

type tzdataFile struct {
	version string
	zones   map[string]tzif
}

// parseTzdata reads Android's tzdata file: a header, an index of 52-byte
// entries (40-byte ID, offset, length, raw offset) and TZif data.
func parseTzdata(b []byte) tzdataFile {
	be := binary.BigEndian
	f := tzdataFile{version: strings.TrimRight(string(b[:12]), "\x00"), zones: map[string]tzif{}}
	index, data := int(be.Uint32(b[12:])), int(be.Uint32(b[16:]))
	for e := index; e < data; e += 52 {
		id := strings.TrimRight(string(b[e:e+40]), "\x00")
		off, n := int(be.Uint32(b[e+40:])), int(be.Uint32(b[e+44:]))
		f.zones[id] = parseTZif(b[data+off : data+off+n])
	}
	return f
}

func parseTZif(t []byte) tzif {
	be := binary.BigEndian
	header := func(p int) (timecnt, typecnt, charcnt, leapcnt, isstdcnt, isutcnt int) {
		if string(t[p:p+4]) != "TZif" {
			log.Fatal("bad TZif header")
		}
		c := func(i int) int { return int(be.Uint32(t[p+20+4*i:])) }
		return c(3), c(4), c(5), c(2), c(1), c(0)
	}
	timecnt, typecnt, charcnt, leapcnt, isstdcnt, isutcnt := header(0)
	p := 44 + timecnt*5 + typecnt*6 + charcnt + leapcnt*8 + isstdcnt + isutcnt
	timecnt, typecnt, _, _, _, _ = header(p)
	p += 44
	var z tzif
	for i := 0; i < timecnt; i++ {
		z.transitions = append(z.transitions, int64(be.Uint64(t[p+8*i:])))
	}
	p += 8 * timecnt
	z.types = append(z.types, t[p:p+timecnt]...)
	p += timecnt
	for i := 0; i < typecnt; i++ {
		z.offsets = append(z.offsets, int32(be.Uint32(t[p+6*i:])))
		z.isDst = append(z.isDst, t[p+6*i+4])
	}
	return z
}
