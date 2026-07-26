package main

import (
	"io"
	"slices"
	"testing"
	"time"
)

// Regression test: stdlib flag stops parsing at the first positional argument,
// so `babbl sub <topic> --as codex` silently ignored --as.
func TestParseArgsHandlesFlagsAfterPositionals(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantPos    []string
		wantAs     string
		wantJSON   bool
		wantTimout time.Duration
	}{
		{
			name:    "flags after positional",
			args:    []string{"review", "--as", "codex"},
			wantPos: []string{"review"},
			wantAs:  "codex",
		},
		{
			name:    "flags before positional",
			args:    []string{"--as", "codex", "review"},
			wantPos: []string{"review"},
			wantAs:  "codex",
		},
		{
			name:     "flags interleaved",
			args:     []string{"--as", "codex", "review", "--json"},
			wantPos:  []string{"review"},
			wantAs:   "codex",
			wantJSON: true,
		},
		{
			name:       "single-dash and equals forms",
			args:       []string{"review", "-as=codex", "-timeout=5s"},
			wantPos:    []string{"review"},
			wantAs:     "codex",
			wantTimout: 5 * time.Second,
		},
		{
			name:    "multiple positionals with trailing flag",
			args:    []string{"review", "some message", "--as", "codex"},
			wantPos: []string{"review", "some message"},
			wantAs:  "codex",
		},
		{
			name:    "double dash protects a leading-dash payload",
			args:    []string{"review", "--as", "codex", "--", "--not-a-flag"},
			wantPos: []string{"review", "--not-a-flag"},
			wantAs:  "codex",
		},
		{
			name:    "no flags at all",
			args:    []string{"review", "hello"},
			wantPos: []string{"review", "hello"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFlagSet("test")
			as := fs.String("as", "", "")
			asJSON := fs.Bool("json", false, "")
			timeout := fs.Duration("timeout", 0, "")

			pos, err := parseArgs(fs, tc.args)
			if err != nil {
				t.Fatalf("parseArgs: %v", err)
			}
			if !slices.Equal(pos, tc.wantPos) {
				t.Errorf("positional = %q, want %q", pos, tc.wantPos)
			}
			if *as != tc.wantAs {
				t.Errorf("--as = %q, want %q", *as, tc.wantAs)
			}
			if *asJSON != tc.wantJSON {
				t.Errorf("--json = %v, want %v", *asJSON, tc.wantJSON)
			}
			if *timeout != tc.wantTimout {
				t.Errorf("--timeout = %v, want %v", *timeout, tc.wantTimout)
			}
		})
	}
}

func TestParseArgsReportsUnknownFlag(t *testing.T) {
	fs := newFlagSet("test")
	fs.SetOutput(io.Discard) // don't print expected usage noise to stderr
	if _, err := parseArgs(fs, []string{"review", "--nope"}); err == nil {
		t.Fatal("expected an error for an unknown flag")
	}
}
