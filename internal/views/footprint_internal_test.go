package views

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

// The footprint walker charges what a view could keep beyond its plan and
// cursors: a page kept as encoded rows, a page kept as one string, and
// strings that only repeat the version's (copies), while strings whose
// bytes are the version's own are not the view's.
func TestFootprintWalkerCountsWhatAViewKeeps(t *testing.T) {
	page := strings.Repeat(`{"episode_id":"e1:x"}`, 1000) // 21,000 bytes
	shared := strings.Repeat("p1:", 30)
	measure := func(v any, version ...string) int64 {
		w := &walker{seen: map[[2]uintptr]bool{}, versionStrings: map[*byte]bool{}}
		for _, s := range version {
			w.versionStrings[unsafe.StringData(s)] = true
		}
		w.walk(reflect.ValueOf(v))
		return w.bytes
	}
	type rows struct{ items []json.RawMessage }
	type text struct{ page string }
	type names struct{ ids []string }
	if got := measure(&rows{items: []json.RawMessage{json.RawMessage(page)}}); got < int64(len(page)) {
		t.Errorf("a page kept as rows: %d bytes", got)
	}
	if got := measure(&text{page: page}); got < int64(len(page)) {
		t.Errorf("a page kept as a string: %d bytes", got)
	}
	ids := []string{shared, shared, shared}
	own := measure(&names{ids: ids}, shared)
	copies := measure(&names{ids: []string{strings.Clone(shared), strings.Clone(shared), strings.Clone(shared)}}, shared)
	if own != int64(unsafe.Sizeof(names{}))+int64(cap(ids))*int64(unsafe.Sizeof("")) || copies != own+3*int64(len(shared)) {
		t.Errorf("the version's own strings %d bytes, copies of them %d", own, copies)
	}
}
