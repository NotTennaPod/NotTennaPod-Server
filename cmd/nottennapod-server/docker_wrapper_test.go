package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// wrapperEnv runs the container's command wrapper
// (docker/root/usr/local/bin/nottennapod-server) outside a container: the
// supervisor's commands, flock and the server binary are stand-ins that log
// what they are asked to do.
type wrapperEnv struct {
	t      *testing.T
	bash   string
	script string
	dir    string
	path   string
}

func newWrapperEnv(t *testing.T) *wrapperEnv {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("no bash")
	}
	// The image's bash is 5.x; arrays empty under set -u need 4.4 or later.
	out, err := exec.Command(bash, "-c", `echo $((BASH_VERSINFO[0]*100+BASH_VERSINFO[1]))`).Output()
	if v, _ := strconv.Atoi(strings.TrimSpace(string(out))); err != nil || v < 404 {
		t.Skipf("bash %s is older than the image's", out)
	}
	dir := t.TempDir()
	src, err := os.ReadFile(filepath.Join("..", "..", "docker", "root", "usr", "local", "bin", "nottennapod-server"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(src)
	for name, value := range map[string]string{"BIN": filepath.Join(dir, "server"), "SVC": filepath.Join(dir, "svc"),
		"LOCK": filepath.Join(dir, "maintenance.lock")} {
		lines := strings.Split(script, "\n")
		found := false
		for i, l := range lines {
			if strings.HasPrefix(l, name+"=") {
				lines[i], found = name+"="+value, true
				break
			}
		}
		if !found {
			t.Fatalf("the wrapper sets no %s", name)
		}
		script = strings.Join(lines, "\n")
	}
	bin := filepath.Join(dir, "bin")
	os.Mkdir(bin, 0o700)
	log := filepath.Join(dir, "calls")
	up := filepath.Join(dir, "up")
	os.WriteFile(up, []byte("true\n"), 0o600)
	stubs := map[string]string{
		"s6-svok":      `exit 0`,
		"s6-svstat":    `case "$2" in wantedup) echo true ;; up) cat ` + up + ` ;; esac`,
		"s6-svc":       `echo "s6-svc $*" >>` + log + `; case " $* " in *" -d "*) echo false >` + up + ` ;; *" -u "*) echo true >` + up + ` ;; esac`,
		"flock":        `exit 0`,
		"s6-setuidgid": `shift; exec "$@"`,
	}
	for name, body := range stubs {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "server"), []byte("#!/bin/sh\necho \"server $*\" >>"+log+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "wrapper")
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return &wrapperEnv{t: t, bash: bash, script: path, dir: dir, path: bin + string(os.PathListSeparator) + os.Getenv("PATH")}
}

// run runs the wrapper with stdin that is not a terminal, as `docker exec`
// without -t gives it, and returns its status, its messages and what the
// stand-ins were asked to do.
func (w *wrapperEnv) run(args ...string) (int, string, []string) {
	w.t.Helper()
	os.Remove(filepath.Join(w.dir, "calls"))
	cmd := exec.Command(w.bash, append([]string{w.script}, args...)...)
	cmd.Env = append(os.Environ(), "PATH="+w.path)
	cmd.Env = append(cmd.Env, "LSIO_NON_ROOT_USER=")
	var stderr bytes.Buffer
	cmd.Stdin, cmd.Stderr = nil, &stderr // the null device: not a terminal
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		w.t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(w.dir, "calls"))
	var calls []string
	for _, l := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if l != "" {
			calls = append(calls, l)
		}
	}
	return code, stderr.String(), calls
}

// A command that reads the passphrase, run without a terminal and without
// --passphrase-file, is refused before the server is stopped; with
// --passphrase-file, or for a command without a passphrase, the server is
// stopped, the command runs and the server starts again.
func TestDockerWrapperRefusesAPassphrasePromptWithoutATerminal(t *testing.T) {
	w := newWrapperEnv(t)
	for _, args := range [][]string{
		{"keys", "export", "--out", "/config/export/keys.json"},
		{"keys", "install", "--keys", "/config/export/keys.json"},
		{"restore", "--from", "/config/export/b.db", "--keys", "/config/export/keys.json"},
		// The option given, but empty or overridden by an empty one: the
		// command still reads the terminal.
		{"keys", "export", "--out", "/config/export/keys.json", "--passphrase-file="},
		{"keys", "export", "--out", "/config/export/keys.json", "--passphrase-file", ""},
		{"restore", "--from", "/config/export/b.db", "--keys", "/config/export/keys.json", "--passphrase-file=/config/export/pass", "--passphrase-file="},
		{"keys", "install", "--keys", "/config/export/keys.json", "--passphrase-file"},
		// The option as the value of another, or after the options end.
		{"keys", "export", "--out", "--passphrase-file=/config/export/pass"},
		{"keys", "export", "--out", "/config/export/keys.json", "--", "--passphrase-file=/config/export/pass"},
	} {
		code, msg, calls := w.run(args...)
		if code == 0 || !strings.Contains(msg, "--passphrase-file") || !strings.Contains(msg, "docker exec -it") || len(calls) != 0 {
			t.Errorf("%v without a terminal: exit %d, %q, calls %q", args, code, msg, calls)
		}
	}
	for _, args := range [][]string{
		{"keys", "export", "--out", "/config/export/keys.json", "--passphrase-file", "/config/export/pass"},
		{"keys", "export", "--out", "/config/export/keys.json", "-passphrase-file=/config/export/pass"},
		{"keys", "export", "--passphrase-file=", "--out", "/config/export/keys.json", "--passphrase-file", "/config/export/pass"},
		{"restore", "--from", "/config/export/b.db", "--keys", "/config/export/keys.json", "--passphrase-file=/config/export/pass"},
		{"backup", "--out", "/config/backups"},
	} {
		words := 1
		if args[0] == "keys" {
			words = 2
		}
		want := "server " + strings.Join(args[:words], " ") + " --data-dir /config/data " + strings.Join(args[words:], " ")
		code, msg, calls := w.run(args...)
		if code != 0 || len(calls) != 3 || !strings.Contains(calls[0], " -d ") || calls[1] != want || !strings.Contains(calls[2], " -u ") {
			t.Errorf("%v: exit %d, %q, calls %q", args, code, msg, calls)
		}
	}
}
