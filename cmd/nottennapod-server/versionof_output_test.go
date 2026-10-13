package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// The version check refuses a candidate that
// prints nothing, or anything but "nottennapod-server VERSION".
func TestVersionCheckRefusesUnusableVersions(t *testing.T) {
	for name, script := range map[string]string{
		"empty":     "#!/bin/sh\nexit 0\n",
		"no prefix": "#!/bin/sh\necho 2\n",
		"blank":     "#!/bin/sh\necho 'nottennapod-server '\n",
		"two words": "#!/bin/sh\necho 'nottennapod-server 2 extra'\n",
		"two lines": "#!/bin/sh\nprintf 'nottennapod-server 2\\nnottennapod-server 3\\n'\n",
	} {
		p := filepath.Join(t.TempDir(), "candidate")
		os.WriteFile(p, []byte(script), 0o700)
		if v, err := versionOf(context.Background(), p); err == nil {
			t.Errorf("%s: accepted version %q", name, v)
		}
	}
}
