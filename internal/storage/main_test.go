package storage_test

import (
	"os"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// Every write transaction of the package's tests is audited for the read
// model's change capture (viewaudit.go), and every commit of the tests is
// checked by the read model's commit observer (viewstest.Main). A test
// binary has one TestMain, and the observer needs the read model, which
// imports this package: so the TestMain is the external package's, and it
// also runs the child processes of the crash tests (the test binary
// re-executed with NTP_STORAGE_CHILD set), which the observer does not see.
func TestMain(m *testing.M) {
	storage.AuditViewCaptureForTesting()
	if storage.RunTestChild() {
		os.Exit(0)
	}
	os.Exit(viewstest.Main(m))
}

func TestReadModelObserverActive(t *testing.T) { viewstest.RequireActive(t) }
