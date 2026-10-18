package importer

import (
	"os"
	"testing"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/views/viewstest"
)

// Every write transaction of the package's tests is audited for the read
// model's change capture (storage.AuditViewCaptureForTesting), and every
// commit is checked by the read model's commit observer (viewstest.Main).
func TestMain(m *testing.M) {
	storage.AuditViewCaptureForTesting()
	os.Exit(viewstest.Main(m))
}

func TestReadModelObserverActive(t *testing.T) { viewstest.RequireActive(t) }
