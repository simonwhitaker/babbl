package main

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// babblBin is a compiled babbl, built once so these tests can assert on real
// process exit codes rather than on the error returned by Execute.
var babblBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "babbl-exit-test")
	if err != nil {
		fmt.Fprintf(os.Stderr, "creating temp dir: %v\n", err)
		os.Exit(1)
	}
	babblBin = filepath.Join(dir, "babbl")
	if out, err := exec.Command("go", "build", "-o", babblBin, ".").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building babbl: %v\n%s", err, out)
		os.RemoveAll(dir)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// runBinary executes the compiled binary and returns its combined output and
// exit status.
func runBinary(t *testing.T, env []string, args ...string) (string, int) {
	t.Helper()

	cmd := exec.Command(babblBin, args...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return string(out), ee.ExitCode()
	}
	t.Fatalf("running babbl %v: %v", args, err)
	return "", -1
}

func TestProcessExitCodes(t *testing.T) {
	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store))
	defer srv.Close()
	env := []string{"BABBL_ADDR=" + strings.TrimPrefix(srv.URL, "http://")}

	store.Append("has-messages", "hello", "")

	tests := []struct {
		name string
		args []string
		want int
		why  string
	}{
		// An incomplete invocation must not look like success. Cobra's default
		// for a root with no RunE is to print help and exit 0; this pins the
		// non-zero status that the pre-cobra CLI had.
		{"no arguments", nil, exitError, "a bare babbl is an incomplete invocation"},
		{"unknown command", []string{"bogus"}, exitError, "typo'd subcommand"},
		{"unknown flag", []string{"topics", "--bogus"}, exitError, "typo'd flag"},
		{"missing required flag", []string{"sub", "review"}, exitError, "--as omitted"},
		{"wrong arg count", []string{"peek"}, exitError, "topic omitted"},

		// Help is a successful outcome.
		{"help flag", []string{"--help"}, exitOK, "--help is not an error"},
		{"help subcommand", []string{"help"}, exitOK, "help is not an error"},
		{"subcommand help", []string{"pub", "--help"}, exitOK, "--help is not an error"},

		// Normal operation.
		{"topics", []string{"topics"}, exitOK, "a working command"},
		{"delivered message", []string{"sub", "has-messages", "--as", "codex"}, exitOK, "a message was printed"},

		// The distinct timeout code shell loops branch on.
		{"sub timeout", []string{"sub", "quiet", "--as", "codex", "--timeout", "1s"}, exitTimeout, "nothing to deliver"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, code := runBinary(t, env, tc.args...)
			if code != tc.want {
				t.Errorf("babbl %s exited %d, want %d (%s)\n---\n%s",
					strings.Join(tc.args, " "), code, tc.want, tc.why, out)
			}
		})
	}
}

// A bare babbl should still show the user what to do, not just fail silently.
func TestNoArgumentsPrintsHelp(t *testing.T) {
	out, code := runBinary(t, nil)
	if code != exitError {
		t.Errorf("exit code = %d, want %d", code, exitError)
	}
	for _, want := range []string{"Usage:", "babbl", "serve", "pub", "sub"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n---\n%s", want, out)
		}
	}
}

// A sub timeout is a normal outcome, so it must not print an error.
func TestSubTimeoutIsSilent(t *testing.T) {
	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store))
	defer srv.Close()
	env := []string{"BABBL_ADDR=" + strings.TrimPrefix(srv.URL, "http://")}

	out, code := runBinary(t, env, "sub", "quiet", "--as", "codex", "--timeout", "1s")
	if code != exitTimeout {
		t.Errorf("exit code = %d, want %d", code, exitTimeout)
	}
	if strings.TrimSpace(out) != "" {
		t.Errorf("expected no output on timeout, got:\n%s", out)
	}
}
