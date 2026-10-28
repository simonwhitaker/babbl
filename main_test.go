package main

import (
	"bytes"
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// startDaemon runs a throwaway daemon and points the CLI at it via BABBL_ADDR.
// Using the env var rather than appending --addr keeps the test args identical
// to what a user would type, which matters for the "--" and "--help" cases.
func startDaemon(t *testing.T) *Store {
	t.Helper()

	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store, nil))
	t.Cleanup(srv.Close)
	t.Setenv("BABBL_ADDR", strings.TrimPrefix(srv.URL, "http://"))
	return store
}

// runCLI executes the real cobra command tree, returning its output and error.
// This exercises the actual CLI surface rather than the functions underneath.
func runCLI(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()

	// Tests that never reach the network still must not be able to contact a
	// real daemon on the default port, so point them at a closed one.
	if os.Getenv("BABBL_ADDR") == "" {
		t.Setenv("BABBL_ADDR", "127.0.0.1:1")
	}

	var out bytes.Buffer
	root := rootCmd()
	root.SetArgs(args)
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetIn(strings.NewReader(stdin))
	err := root.Execute()
	return out.String(), err
}

// Regression test for the reason parseArgs existed: stdlib flag stopped at the
// first positional, so `babbl sub <topic> --as codex` ignored --as. pflag
// handles interspersed flags natively; this pins that behaviour down.
func TestFlagsAfterPositionalArgs(t *testing.T) {
	store := startDaemon(t)

	// --from comes after both positionals and must still be picked up.
	if _, err := runCLI(t, "", "pub", "review", "a comment", "--from", "claude"); err != nil {
		t.Fatalf("pub: %v", err)
	}
	msgs, _ := store.Peek("review", 0, 0)
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if msgs[0].Payload != "a comment" {
		t.Errorf("payload = %q, want %q (flag leaked into the payload?)", msgs[0].Payload, "a comment")
	}
	if msgs[0].From != "claude" {
		t.Errorf("from = %q, want %q (--from after positionals was ignored)", msgs[0].From, "claude")
	}

	// --as comes after the topic and must still be picked up.
	out, err := runCLI(t, "", "sub", "review", "--as", "codex")
	if err != nil {
		t.Fatalf("sub: %v", err)
	}
	if strings.TrimSpace(out) != "a comment" {
		t.Errorf("sub printed %q", out)
	}
}

func TestFlagsBeforePositionalArgs(t *testing.T) {
	store := startDaemon(t)

	if _, err := runCLI(t, "", "pub", "--from", "claude", "review", "a comment"); err != nil {
		t.Fatalf("pub: %v", err)
	}
	msgs, _ := store.Peek("review", 0, 0)
	if len(msgs) != 1 || msgs[0].Payload != "a comment" || msgs[0].From != "claude" {
		t.Fatalf("unexpected message: %+v", msgs)
	}
}

// "--" lets a payload start with a dash instead of being read as a flag.
func TestDoubleDashProtectsPayload(t *testing.T) {
	store := startDaemon(t)

	if _, err := runCLI(t, "", "pub", "notes", "--", "--this-looks-like-a-flag"); err != nil {
		t.Fatalf("pub: %v", err)
	}
	msgs, _ := store.Peek("notes", 0, 0)
	if len(msgs) != 1 || msgs[0].Payload != "--this-looks-like-a-flag" {
		t.Fatalf("unexpected message: %+v", msgs)
	}
}

func TestPubReadsStdin(t *testing.T) {
	store := startDaemon(t)

	if _, err := runCLI(t, "line one\nline two\n", "pub", "diffs"); err != nil {
		t.Fatalf("pub: %v", err)
	}
	msgs, _ := store.Peek("diffs", 0, 0)
	if len(msgs) != 1 || msgs[0].Payload != "line one\nline two" {
		t.Fatalf("unexpected message: %+v", msgs)
	}
}

func TestPubRejectsEmptyPayload(t *testing.T) {
	if _, err := runCLI(t, "", "pub", "notes"); err == nil {
		t.Fatal("expected an error for an empty payload")
	}
}

func TestSubRequiresAs(t *testing.T) {
	_, err := runCLI(t, "", "sub", "review")
	if err == nil {
		t.Fatal("expected an error when --as is missing")
	}
	if !strings.Contains(err.Error(), "as") {
		t.Errorf("error should name the missing flag, got: %v", err)
	}
}

// A sub timeout is signalled with errSubTimeout, which main() maps to exit 3.
func TestSubTimeoutReturnsSentinel(t *testing.T) {
	startDaemon(t)

	_, err := runCLI(t, "", "sub", "quiet", "--as", "codex", "--timeout", "200ms")
	if !errors.Is(err, errSubTimeout) {
		t.Fatalf("got %v, want errSubTimeout", err)
	}
}

func TestWrongArgCounts(t *testing.T) {
	cases := [][]string{
		{"sub", "--as", "codex"},               // no topic
		{"sub", "one", "two", "--as", "codex"}, // too many topics
		{"peek"},                               // no topic
		{"topics", "unexpected"},               // topics takes none
		{"topics", "ls", "unexpected"},         // nor does ls
		{"topics", "rm"},                       // no topic
		{"topics", "rm", "one", "two"},         // one topic at a time
		{"serve", "unexpected"},                // serve takes none
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			if _, err := runCLI(t, "", args...); err == nil {
				t.Fatal("expected an argument-count error")
			}
		})
	}
}

// --help must exit cleanly and advertise GNU-style double-dash flags.
func TestHelpIsCleanAndUsesDoubleDashes(t *testing.T) {
	for _, args := range [][]string{{"pub", "--help"}, {"sub", "--help"}, {"--help"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			out, err := runCLI(t, "", args...)
			if err != nil {
				t.Fatalf("--help returned an error: %v", err)
			}
			if strings.Contains(out, "flag: help requested") {
				t.Error("help output leaked an error message")
			}
			if !strings.Contains(out, "babbl") {
				t.Error("help output should name the babbl binary")
			}
		})
	}

	out, _ := runCLI(t, "", "pub", "--help")
	for _, want := range []string{"--from", "--json", "--addr", "<topic>"} {
		if !strings.Contains(out, want) {
			t.Errorf("pub --help missing %q\n---\n%s", want, out)
		}
	}
	// The old stdlib output rendered these as "-from"/"-json".
	if strings.Contains(out, "\n  -from") || strings.Contains(out, "\n  -json") {
		t.Errorf("help still shows single-dash flags\n---\n%s", out)
	}
}

// The point of readable help is that an agent can learn the whole invocation
// from it, so the environment variables must be discoverable there too.
func TestHelpDocumentsEnvironmentVariables(t *testing.T) {
	root, err := runCLI(t, "", "--help")
	if err != nil {
		t.Fatalf("--help: %v", err)
	}
	for _, want := range []string{"BABBL_ADDR", "BABBL_DIR"} {
		if !strings.Contains(root, want) {
			t.Errorf("root help does not mention %s\n---\n%s", want, root)
		}
	}

	serve, err := runCLI(t, "", "serve", "--help")
	if err != nil {
		t.Fatalf("serve --help: %v", err)
	}
	if !strings.Contains(serve, "BABBL_DIR") {
		t.Errorf("serve --help does not mention BABBL_DIR\n---\n%s", serve)
	}
}

func TestTopicsListsWithAndWithoutLs(t *testing.T) {
	store := startDaemon(t)
	store.Append("review", "one", "")

	for _, args := range [][]string{{"topics"}, {"topics", "ls"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			out, err := runCLI(t, "", args...)
			if err != nil {
				t.Fatalf("%v: %v", args, err)
			}
			if out != "review (1 message)\n" {
				t.Fatalf("got %q", out)
			}
		})
	}

	out, err := runCLI(t, "", "topics", "ls", "--json")
	if err != nil || !strings.Contains(out, `"name": "review"`) {
		t.Fatalf("topics ls --json: %v\n%s", err, out)
	}
}

func TestTopicsRm(t *testing.T) {
	for _, verb := range []string{"rm", "delete"} {
		t.Run(verb, func(t *testing.T) {
			store := startDaemon(t)
			store.Append("review", "one", "")
			store.Append("alerts", "boom", "")

			out, err := runCLI(t, "", "topics", verb, "review")
			if err != nil {
				t.Fatalf("topics %s: %v", verb, err)
			}
			if out != "" {
				t.Fatalf("expected no output on success, got %q", out)
			}
			if infos := store.Topics(); len(infos) != 1 || infos[0].Name != "alerts" {
				t.Fatalf("unexpected topics: %+v", infos)
			}
		})
	}
}

func TestTopicsRmUnknownTopicFails(t *testing.T) {
	startDaemon(t)
	_, err := runCLI(t, "", "topics", "rm", "nope")
	if err == nil || !strings.Contains(err.Error(), "no such topic") {
		t.Fatalf("got %v, want a no-such-topic error", err)
	}
}

// Completion asks the daemon for topic names, and offers nothing (rather than
// local filenames) when it can't reach one.
func TestTopicCompletion(t *testing.T) {
	store := startDaemon(t)
	store.Append("review", "one", "")
	store.Append("alerts", "boom", "")

	for _, args := range [][]string{
		{"topics", "rm", ""},
		{"pub", ""},
		{"sub", ""},
		{"peek", ""},
	} {
		t.Run(args[0], func(t *testing.T) {
			out, err := runCLI(t, "", append([]string{"__complete"}, args...)...)
			if err != nil {
				t.Fatalf("__complete: %v", err)
			}
			// sub also suggests its required --as flag, so look for the
			// topics rather than expecting them to be the whole output.
			if !strings.Contains(out, "\nalerts\nreview\n:4\n") && !strings.HasPrefix(out, "alerts\nreview\n:4\n") {
				t.Fatalf("unexpected completions:\n%s", out)
			}
		})
	}

	// Only the topic argument completes; pub's message does not.
	out, _ := runCLI(t, "", "__complete", "pub", "review", "")
	if !strings.HasPrefix(out, ":4\n") {
		t.Fatalf("pub message argument offered completions:\n%s", out)
	}
}

func TestTopicCompletionWithoutDaemon(t *testing.T) {
	out, err := runCLI(t, "", "__complete", "topics", "rm", "")
	if err != nil {
		t.Fatalf("__complete: %v", err)
	}
	if !strings.HasPrefix(out, ":4\n") {
		t.Fatalf("unexpected completions without a daemon:\n%s", out)
	}
}
