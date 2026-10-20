package storage

import (
	"bytes"
	"context"
	"testing"
)

// A browser's sign-out ends the web sessions along its same-nonce
// supersession path (PROTOCOL §5.4): the successors', used or not, and
// nothing else: not the first installation's, not a device token, and not
// past an installation with another nonce.
func TestSignOutPathEndsOnlyTheSuccessorsWebSessions(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, config{dataDir: t.TempDir()})
	account := createAccounts(t, s, 1)[0]
	nonce := newUUID()
	t1, y, z, other := newUUID(), newUUID(), newUUID(), newUUID()
	cred := func(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }
	if err := s.Write(ctx, func(w *WriteTx) error {
		a := w.Account(account)
		for _, i := range []Installation{{InstallID: t1, InstanceNonce: nonce}, {InstallID: y, InstanceNonce: nonce},
			{InstallID: z, InstanceNonce: nonce}, {InstallID: other, InstanceNonce: newUUID()}} {
			i.Name, i.Platform, i.AppVersion, i.Capabilities = "browser", "web", "1", "[]"
			if err := a.InsertInstallation(i); err != nil {
				return err
			}
		}
		// T → Y → Z share the nonce; Z's superseded_by names another nonce's
		// installation, which no handoff makes, so the walk must stop there.
		for _, link := range [][2]string{{t1, y}, {y, z}, {z, other}} {
			if err := a.execOne(`UPDATE installations SET superseded_by = ? WHERE account_id = ? AND install_id = ?`, link[1], account, link[0]); err != nil {
				return err
			}
		}
		for i, c := range []struct {
			kind, install string
		}{{"web", t1}, {"web", y}, {"device", y}, {"web", z}, {"web", other}} {
			if err := a.InsertCredential(cred(byte(i+1)), c.kind, c.install, nil); err != nil {
				return err
			}
		}
		return a.DeleteWebSessionsOnSupersessionPath(t1)
	}); err != nil {
		t.Fatal(err)
	}
	for i, want := range []bool{true, false, true, false, true} {
		if got := tokenWorks(t, s, cred(byte(i+1))); got != want {
			t.Errorf("credential %d stored %v, want %v", i+1, got, want)
		}
	}
}
