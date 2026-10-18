package protocol

import (
	"bytes"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
)

// A recovery job runs the request it stores (PROTOCOL §6.11): the stored
// form decodes to the same request (facts, parts, receipts, decisions,
// secrets, steps), whose canonical bytes are the uploaded ones; identical
// requests have one key and any difference another.
func TestRecoveryJobRequestRoundTrip(t *testing.T) {
	fact := func(text string) *Archived {
		v, err := contract.DecodeJSON([]byte(`{"op_id":"00000000-0000-4000-8000-000000000001","author_id":"a","author_seq":1,"dependencies":[],` +
			`"type":"test.note","type_version":1,"client_time":1700000000000,"targets":[],"payload":{"text":"` + text + `","n":15}}`))
		if err != nil {
			t.Fatal(err)
		}
		a, err := ParseFact(v)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	object := func(s string) map[string]any {
		v, err := contract.DecodeJSON([]byte(s))
		if err != nil {
			t.Fatal(err)
		}
		return v.(map[string]any)
	}
	req := RecoveryUploadRequest{AccountID: "account", InstallID: "install", RecoveryID: "recovery", Steps: AllRecoveryEndSteps,
		Entries: []RecoveryEntry{
			{Fact: fact("one"), Receipt: object(`{"op_id":"00000000-0000-4000-8000-000000000001","epoch":1,"seq":2,"index":0,"mac":"ab"}`)},
			{Part: &Part{TransactionID: "00000000-0000-4000-8000-000000000002", Index: 1, Count: 3, ContentHash: "cd", Data: []byte{0, 1, 2}},
				Receipt: object(`{"epoch":1,"seq":3}`)},
			{Decision: object(`{"decision_id":"00000000-0000-4000-8000-000000000003","inputs":{},"result":{"x":[1,2]}}`)},
		},
		Secrets: []Secret{{ID: "s1", Username: "u", Password: "p"}, {ID: "s2", Username: "u", Password: "p", ApprovedOrigins: []string{}}}}
	data, key, err := encodeRecoveryJob(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeRecoveryJob(data, "account")
	if err != nil {
		t.Fatal(err)
	}
	again, key2, err := encodeRecoveryJob(got)
	if err != nil {
		t.Fatal(err)
	}
	if key2 != key || !bytes.Equal(again, data) {
		t.Fatalf("the decoded request encodes differently:\n  %s\n  %s", data, again)
	}
	if got.AccountID != "account" || got.InstallID != "install" || got.RecoveryID != "recovery" || got.Steps != AllRecoveryEndSteps ||
		len(got.Entries) != 3 {
		t.Fatalf("decoded %+v", got)
	}
	if !bytes.Equal(got.Entries[0].Fact.Canonical, req.Entries[0].Fact.Canonical) || got.Entries[0].Fact.ContentHash != req.Entries[0].Fact.ContentHash {
		t.Error("the fact's canonical bytes changed")
	}
	if p := got.Entries[1].Part; p == nil || !bytes.Equal(p.Data, []byte{0, 1, 2}) || p.Index != 1 || p.Count != 3 {
		t.Errorf("part %+v", p)
	}
	if got.Secrets[0].ApprovedOrigins != nil || got.Secrets[1].ApprovedOrigins == nil {
		t.Error("approved origins: absent and empty must stay apart")
	}
	other := req
	other.Entries = append([]RecoveryEntry{}, req.Entries...)
	other.Entries[0] = RecoveryEntry{Fact: fact("two"), Receipt: req.Entries[0].Receipt}
	if _, k, _ := encodeRecoveryJob(other); k == key {
		t.Error("a different request has the same key")
	}
	other = req
	other.InstallID = "another"
	if _, k, _ := encodeRecoveryJob(other); k == key {
		t.Error("another installation's request has the same key")
	}
}
