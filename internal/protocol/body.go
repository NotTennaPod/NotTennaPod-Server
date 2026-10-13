package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"unicode/utf16"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// encodeRaw writes v as JSON with members in canonical order, embedding every
// json.RawMessage byte for byte: pages carry stored canonical commits and items
// unchanged (no encoder may escape U+2028/U+2029 or anything else in them).
func encodeRaw(buf *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case json.RawMessage:
		buf.Write(x)
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return lessUTF16(keys[i], keys[j]) })
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			kb, err := contract.Canonical(k)
			if err != nil {
				return err
			}
			buf.Write(kb)
			buf.WriteByte(':')
			if err := encodeRaw(buf, x[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := encodeRaw(buf, e); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case int:
		return encodeRaw(buf, int64(x))
	case nil, bool, string, int64, json.Number:
		b, err := contract.Canonical(x)
		if err != nil {
			return err
		}
		buf.Write(b)
	default:
		return fmt.Errorf("protocol: cannot encode %T", v)
	}
	return nil
}

func lessUTF16(a, b string) bool {
	ua, ub := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		if ua[i] != ub[i] {
			return ua[i] < ub[i]
		}
	}
	return len(ua) < len(ub)
}

// Body encodes the page with its commits embedded byte for byte.
func (r *PullResponse) Body() ([]byte, error) {
	m := map[string]any{"head": r.Head, "from": r.From, "has_more": r.HasMore, "install_watermark": r.InstallWatermark, "items": r.Items}
	if r.Next != "" {
		m["next"] = r.Next
	}
	var buf bytes.Buffer
	err := encodeRaw(&buf, m)
	return buf.Bytes(), err
}

// Body encodes the page with its items embedded byte for byte.
func (r *SnapshotResponse) Body() ([]byte, error) {
	if r.Prefix {
		var buf bytes.Buffer
		err := encodeRaw(&buf, map[string]any{"prefix": true, "cursor": r.Cursor, "head": r.Head, "install_watermark": r.InstallWatermark})
		return buf.Bytes(), err
	}
	m := map[string]any{"head": r.Head, "has_more": r.HasMore, "install_watermark": r.InstallWatermark, "items": r.Items}
	if r.Next != "" {
		m["next"] = r.Next
	}
	if r.Manifest != nil {
		m["manifest"] = r.Manifest
	}
	var buf bytes.Buffer
	err := encodeRaw(&buf, m)
	return buf.Bytes(), err
}
