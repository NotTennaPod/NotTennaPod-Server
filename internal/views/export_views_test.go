package views

import (
	"reflect"
	"strings"
	"time"
	"unsafe"
)

// Footprint is what a registered view keeps reachable of its own: Bytes
// counts every allocation reachable from the view (slices at their
// capacity, including the elements past their length, and every string's
// bytes), apart from what it shares with its version, which the version
// retention bounds: the version, its trees' nodes, its episodes and
// podcasts, and strings whose bytes are the version's own (a plan's ranges
// name podcasts by the version's ID strings; any other string, even with
// equal contents, is the view's and counted).
// Ranges is its plan's; Positions its resume cursors'; ResumeCap and
// ResumeBase are the resume slots' capacity and first slot's address.
type Footprint struct {
	Ranges, Positions int
	Bytes             int64
	ResumeCap         int
	ResumeBase        uintptr
}

// ViewFootprint measures view id by walking its value.
func ViewFootprint(s *Spool, id string) (Footprint, bool) {
	s.mu.Lock()
	v, ok := s.views[id]
	s.mu.Unlock()
	if !ok {
		return Footprint{}, false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	var f Footprint
	for _, c := range v.resume {
		f.Positions += len(c.pos)
	}
	if l, ok := v.src.(*EpisodeList); ok {
		f.Ranges = l.Ranges()
	}
	f.ResumeCap = cap(v.resume)
	if cap(v.resume) > 0 {
		f.ResumeBase = uintptr(unsafe.Pointer(unsafe.SliceData(v.resume)))
	}
	w := &walker{seen: map[[2]uintptr]bool{}, versionStrings: map[*byte]bool{}}
	var ver *Version
	switch src := v.src.(type) {
	case *EpisodeList:
		ver = src.v
	case *SubscriptionList:
		ver = src.lib.V
	}
	if ver != nil {
		for id, p := range ver.podcasts.All() {
			w.versionStrings[unsafe.StringData(id)] = true
			w.versionStrings[unsafe.StringData(p.ID)] = true
		}
	}
	w.walk(reflect.ValueOf(v))
	f.Bytes = w.bytes
	return f, true
}

// shared are the pointer types a view shares with its version.
var shared = map[reflect.Type]bool{
	reflect.TypeOf((*Version)(nil)):       true,
	reflect.TypeOf((*Library)(nil)):       true,
	reflect.TypeOf((*EpisodeItem)(nil)):   true,
	reflect.TypeOf((*PodcastItem)(nil)):   true,
	reflect.TypeOf((*time.Location)(nil)): true,
}

type walker struct {
	seen           map[[2]uintptr]bool // (address, type size) of the allocations counted
	versionStrings map[*byte]bool      // the version's own string bytes
	bytes          int64
}

func (w *walker) walk(v reflect.Value) {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() || shared[v.Type()] || strings.HasSuffix(v.Type().Elem().PkgPath(), "/views/ptree") {
			return
		}
		key := [2]uintptr{v.Pointer(), v.Type().Elem().Size()}
		if w.seen[key] {
			return
		}
		w.seen[key] = true
		w.bytes += int64(v.Type().Elem().Size())
		w.walk(v.Elem())
	case reflect.Slice:
		if v.IsNil() || v.Cap() == 0 {
			return
		}
		key := [2]uintptr{v.Pointer(), uintptr(v.Cap())}
		if w.seen[key] {
			return
		}
		w.seen[key] = true
		w.bytes += int64(v.Cap()) * int64(v.Type().Elem().Size())
		all := v.Slice(0, v.Cap())
		for i := range all.Len() {
			w.walk(all.Index(i))
		}
	case reflect.String:
		str := v.String()
		if len(str) == 0 || w.versionStrings[unsafe.StringData(str)] {
			return
		}
		key := [2]uintptr{uintptr(unsafe.Pointer(unsafe.StringData(str))), uintptr(len(str))}
		if w.seen[key] {
			return
		}
		w.seen[key] = true
		w.bytes += int64(len(str))
	case reflect.Array:
		for i := range v.Len() {
			w.walk(v.Index(i))
		}
	case reflect.Struct:
		for i := range v.NumField() {
			w.walk(v.Field(i))
		}
	case reflect.Map:
		if v.IsNil() {
			return
		}
		w.bytes += int64(v.Len()) * int64(v.Type().Key().Size()+v.Type().Elem().Size())
		it := v.MapRange()
		for it.Next() {
			w.walk(it.Key())
			w.walk(it.Value())
		}
	case reflect.Interface:
		if v.IsNil() {
			return
		}
		e := v.Elem()
		if e.Kind() != reflect.Pointer {
			w.bytes += int64(e.Type().Size())
		}
		w.walk(e)
	}
}
