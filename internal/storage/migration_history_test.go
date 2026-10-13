package storage

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

// These whole-file SHA-256 values are the checksums every database records for
// the migrations applied to it, comments and whitespace included. Keep them
// fixed: editing an applied migration makes those databases refuse to open.
// Schema changes belong in later migrations, which this guard permits appending.
func TestHistoricalMigrationBytes(t *testing.T) {
	historical := []struct {
		name, checksum string
	}{
		{"0001_init.sql", "8c93f51700c9c58a767426a89fae9ffd57d111a67f83287f5b945e682abd5f8f"},
		{"0002_auth.sql", "02fe40b41c31ed87ba6975af51abc2313e38e4d6c144f74335a46edf95a26c76"},
		{"0003_protocol.sql", "7c383d3b4e3e7463060449c1416f8942f5386d25215741270174dad63614567f"},
		{"0004_projections.sql", "1a11b2d199abeb6f4a445b81f73083e805682b72ebf1339fc61523a5d4f851a8"},
		{"0005_listening.sql", "fff92c47d2b40238211aacedb609d3530c544a5789d1053acf00375f01d97b95"},
		{"0006_queue.sql", "5736c450993ebeadcfed663ddc2c5fa4d3156249c7b3c449d155c200b2df57e8"},
		{"0007_authors.sql", "a5c84938070bff7dad4bd439b8b4f29dafaa6cabd05da8c9263a49022877e1c6"},
		{"0008_continuations.sql", "ebadec7058f047c05e4fd2bbce66a72352d1404f9ed94a4c16df2eadd712da5d"},
		{"0009_session_effect_authors.sql", "667d7d86f47f3609ef881554d40a1d087445d7fe60616b6960d54f8eaa0da994"},
		{"0010_place_archived_facts.sql", "3b5cb2bf0deb098600f48a5a1eae578b82016eaf095bd671634312894b42823b"},
		{"0011_feed_responses.sql", "d0ecca09163c9547d96236e18347e6df899859f31b47ef9a4791d8c78433bf2e"},
		{"0012_catalog.sql", "ff75cf3e432fdddbcb6c8f30f9e1c26a8a8d2e363d198bb2d8c428538a6f1450"},
		{"0013_identity.sql", "4b65491830ac5bc5dddffe22954c23a9ca1acd2ac493a8ff8584a5ce29c877bd"},
		{"0014_imports.sql", "91a1bb8c62f8001707a698eb7a13c0dabbfa2cac3d4a7d3fcadc3a04f17111a8"},
		{"0015_refresh_context.sql", "e291b5ff1f57a953462b84d40357454a4aa8ba0d757c3b2725b19820cdc8815a"},
		{"0016_identity_releases.sql", "c656e4eb35db865e8031c33005e7956de1ef0f6ffb4f0ce670a30fbbf60f0cf3"},
		{"0017_recovery.sql", "279ef7f805248abff3d3a3b4a812dd9ae0cb742032d57cf375418ffe84de264f"},
		{"0018_causal_indexes.sql", "11e8e411b1f1e933ea38f664d0ccc14b9d577b04f22da0d6ac5092f98623ce8b"},
		{"0019_release_additions.sql", "75dfdac5156170826837794fb7b3b67a69ec3bd25a596a215d710e8f81573861"},
		{"0020_recovery_frames.sql", "83aa8390a7506f788f8798a32543a7e659ec3b34ec9f2ef0081fbfa44d997dfc"},
		{"0021_account_cascades.sql", "1daf65c533b8e529bd74d12a41584fb5f485c5554eb43ced0aa5b8074de18c8a"},
		{"0022_listening_facts_by_scope.sql", "9223211f0ae6ac0b0e9f74fb28efbcd878e76173a1bd484605ddcc7806b365a5"},
		{"0023_queue_replay.sql", "047cd1200c41693d5779afc568a6bfaf8076457dd639dd7821b9409d2040fd1b"},
		{"0024_feed_response_reparse.sql", "4d3995a04f19e2f44e0342577ef19f835084570d65ea4b7b309b599fe47f3a4d"},
	}
	for _, h := range historical {
		t.Run(h.name, func(t *testing.T) {
			data, err := migrationFiles.ReadFile("migrations/" + h.name)
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != h.checksum {
				t.Fatalf("historical migration bytes changed: SHA-256 %s, want %s", got, h.checksum)
			}
		})
	}
}
