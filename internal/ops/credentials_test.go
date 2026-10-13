package ops

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

func (e *env) rawPush(d *device, ops []protocol.PushOp, secrets []protocol.Secret) (*protocol.PushResponse, error) {
	inst := d.inst
	return e.svc.Push(context.Background(), protocol.PushRequest{AccountID: e.account, Cursor: e.head(), InstallID: inst.InstallID,
		AckedInstallSeq: d.seq, Operations: ops, Secrets: secrets}, func(tx *storage.WriteTx) (*storage.Installation, error) {
		i, err := tx.Account(e.account).Installation(inst.InstallID)
		return &i, err
	})
}

// dataFiles returns the database and WAL bytes.
func (e *env) dataFiles() []byte {
	var all []byte
	for _, name := range []string{"nottennapod.db", "nottennapod.db-wal"} {
		b, _ := os.ReadFile(filepath.Join(e.svc.Store.Dir(), name))
		all = append(all, b...)
	}
	return all
}

func TestCredentialsNeverReachTheDatabaseInClear(t *testing.T) {
	e := newEnv(t)
	a := e.device("A")
	const user, pass = "listener-name-7731", "s3cret-pass-phrase-9246"
	secretID := newID()
	// A subscribe naming a secret the server does not hold waits.
	sub := e.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://private.example.com/feed",
		"credentials_secret": secretID})
	parsed, err := protocol.ParseFact(sub)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := e.rawPush(a, []protocol.PushOp{{InstallSeq: 1, Fact: parsed}}, nil)
	if err != nil || resp.Results[0].Status != "dependency_missing" || resp.Results[0].Missing[0] != secretID {
		t.Fatalf("missing secret: %+v %v", resp, err)
	}
	// With the secret beside it, it is admitted.
	resp, err = e.rawPush(a, []protocol.PushOp{{InstallSeq: 1, Fact: parsed}}, []protocol.Secret{{ID: secretID, Username: user, Password: pass}})
	if err != nil || resp.Results[0].Status != "accepted" {
		t.Fatalf("with secret: %+v %v", resp, err)
	}
	a.seq = 1
	// A different value under the same ID is refused.
	if _, err := e.rawPush(a, nil, []protocol.Secret{{ID: secretID, Username: user, Password: "other"}}); err == nil {
		t.Fatal("a different secret under a known ID was accepted")
	}
	// The account's installations can read it; another account cannot.
	v, err := e.svc.SecretValue(context.Background(), e.account, secretID)
	if err != nil || v["username"] != user || v["password"] != pass {
		t.Fatalf("secret value: %v %v", v, err)
	}
	var other string
	e.svc.Store.Write(context.Background(), func(tx *storage.WriteTx) error { o, err := tx.CreateAccount(); other = o.ID; return err })
	if _, err := e.svc.SecretValue(context.Background(), other, secretID); err == nil {
		t.Fatal("another account read the secret")
	}
	// A URL with userinfo is refused before anything is stored (unframed).
	leaky := e.fact(a, "podcast.subscribe", nil, []any{}, map[string]any{"feed_url": "https://" + user + ":" + pass + "@private.example.com/other"})
	lp, _ := protocol.ParseFact(leaky)
	if _, err := e.rawPush(a, []protocol.PushOp{{InstallSeq: 2, Fact: lp}}, nil); err == nil {
		t.Fatal("credential URL accepted")
	}
	// The same fact framed in parts: parts are stored encrypted, and the
	// completed fact is refused without consuming the slot.
	canonical, _ := contract.Canonical(leaky)
	hash, _ := contract.ContentHash(leaky)
	tx := newID()
	half := len(canonical) / 2
	r1, err := e.rawPush(a, []protocol.PushOp{{InstallSeq: 2, Part: &protocol.Part{TransactionID: tx, Index: 0, Count: 2, ContentHash: hash, Data: canonical[:half]}}}, nil)
	if err != nil || r1.Results[0].Status != "part_stored" {
		t.Fatalf("first part: %+v %v", r1, err)
	}
	if db := e.dataFiles(); bytes.Contains(db, []byte(pass)) || bytes.Contains(db, []byte(user)) {
		t.Fatal("a staged part holds the credentials in clear")
	}
	r2, err := e.rawPush(a, []protocol.PushOp{{InstallSeq: 2, Part: &protocol.Part{TransactionID: tx, Index: 1, Count: 2, ContentHash: hash, Data: canonical[half:]}}}, nil)
	if err != nil || r2.Results[0].Status != "invalid_part" || r2.InstallWatermark != 1 {
		t.Fatalf("completed credential fact: %+v %v", r2, err)
	}
	if db := e.dataFiles(); bytes.Contains(db, []byte(pass)) || bytes.Contains(db, []byte(user)) {
		t.Fatal("credentials reached the database in clear")
	}
	if _, err := e.svc.SecretValue(context.Background(), e.account, newID()); !errors.As(err, new(*protocol.Error)) {
		t.Fatalf("unknown secret: %v", err)
	}
}

// A secret carries its approved origins (PROTOCOL §9.2, §9.4), sorted and without
// duplicates; a secret without them stays as it was.
func TestSecretApprovedOrigins(t *testing.T) {
	e := newEnv(t)
	a := e.device("A")
	withOrigins, plain := newID(), newID()
	_, err := e.rawPush(a, nil, []protocol.Secret{
		{ID: withOrigins, Username: "u", Password: "p", ApprovedOrigins: []string{"https://cdn.example.com:443", "https://example.com:443", "https://cdn.example.com:443"}},
		{ID: plain, Username: "u", Password: "p"},
	})
	if err != nil {
		t.Fatal(err)
	}
	v, err := e.svc.SecretValue(context.Background(), e.account, withOrigins)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := contract.Canonical(v["approved_origins"])
	if string(got) != `["https://cdn.example.com:443","https://example.com:443"]` {
		t.Errorf("approved origins %s", got)
	}
	if v, err := e.svc.SecretValue(context.Background(), e.account, plain); err != nil || v["approved_origins"] != nil {
		t.Errorf("a secret without origins: %v %v", v, err)
	}
}
