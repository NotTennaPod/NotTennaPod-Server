package ops

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
)

// The fixture is an account an earlier server version synced (migrations 1 to
// 6 applied), with those migrations' checksums as stored. Keep the dump and
// this whole-file SHA-256 fixed, so the upgrade tests start from that schema.
func TestHistoricalUpgradeFixtureBytes(t *testing.T) {
	const checksum = "02822bedca0a5a6a698bda7181d16b1ea37f8be9a71cccef1c871ab535cbd7fb"
	data, err := os.ReadFile("testdata/account-6b.sql")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != checksum {
		t.Fatalf("historical upgrade fixture bytes changed: SHA-256 %s, want %s", got, checksum)
	}
}
