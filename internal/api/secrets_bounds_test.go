package api

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// A push of 500 secrets does not hold the writer for seconds, and the secret field
// bounds. That the push prepares its secrets before its transaction is proved by
// protocol's TestPushPreparesSecretsBeforeItsTransaction; here the writer's wait is
// only measured, as a wall-clock bound fails under load. What the transaction still
// does, storing the sealed secrets, is bounded by the request size and the
// 500-secret limit.

func TestSecretsMaximumWriter(t *testing.T) {
	e := newEnv(t, withReads(t))
	_, account := e.setupAdmin("owner", "correct horse battery")
	p := e.phoneOf("owner", "correct horse battery")
	list := make([]any, 500)
	for i := range list {
		list[i] = map[string]any{"secret_id": fmt.Sprintf("00000000-0000-4000-8000-%012d", i), "username": strings.Repeat("u", 4096), "password": strings.Repeat("p", 3800)}
	}
	raw := encode(t, "POST", map[string]any{"cursor": p.cursor, "install_id": p.install, "acked_install_seq": 0, "operations": []any{}, "secrets": list})
	if len(raw) > protocol.MaxRequestBytes {
		t.Fatal("fixture exceeds HTTP bound")
	}
	secrets, err := parseSecrets(list)
	if err != nil {
		t.Logf("40,000 secrets refused before transaction: %v", err)
		return
	}
	cur, err := contract.DecodeCursor(p.cursor)
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan struct{})
	writerDone := make(chan time.Duration, 1)
	go func() {
		<-acquired
		start := time.Now()
		err := e.store.Write(context.Background(), func(tx *storage.WriteTx) error { return nil })
		if err != nil {
			t.Error(err)
		}
		writerDone <- time.Since(start)
	}()
	start := time.Now()
	_, err = e.srv.cfg.Protocol.Push(context.Background(), protocol.PushRequest{AccountID: account, InstallID: p.install, Cursor: cur, Secrets: secrets}, func(tx *storage.WriteTx) (*storage.Installation, error) {
		inst, err := tx.Account(account).Installation(p.install)
		close(acquired)
		return &inst, err
	})
	if err != nil {
		t.Fatal(err)
	}
	wait := <-writerDone
	t.Logf("%d-byte legal push with no operations and %d secrets: total=%v blocked other writer=%v", len(raw), len(secrets), time.Since(start), wait)
}

func TestSecretFieldBoundaries(t *testing.T) {
	for _, field := range []string{"username", "password"} {
		for _, n := range []int{4096, 4097} {
			m := map[string]any{"secret_id": newID(), "username": "", "password": ""}
			m[field] = strings.Repeat("a", n)
			_, err := parseSecrets([]any{m})
			if (err == nil) != (n == 4096) {
				t.Errorf("%s bytes=%d: %v", field, n, err)
			}
		}
	}
	for _, n := range []int{16, 17} {
		origins := make([]any, n)
		for i := range origins {
			origins[i] = fmt.Sprintf("https://publisher%d.invalid:443", i)
		}
		_, err := parseSecrets([]any{map[string]any{"secret_id": newID(), "username": "", "password": "", "approved_origins": origins}})
		if (err == nil) != (n == 16) {
			t.Errorf("origins=%d: %v", n, err)
		}
	}
}
func TestRecoverySecretsAtLimit(t *testing.T) {
	r := newRecoveryEnv(t)
	list := make([]any, 500)
	for i := range list {
		list[i] = map[string]any{"secret_id": newID(), "username": "", "password": strings.Repeat("p", 4096)}
	}
	r.e.do("POST", "/v1/sync/recovery/upload", map[string]any{"recovery_id": r.rid, "entries": []any{}, "secrets": list}, r.p.cred).must(200)
}
