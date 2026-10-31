package main

import (
	"strings"
	"testing"
)

func TestVersionString(t *testing.T) {
	tests := []struct {
		name                      string
		stamped, module, revision string
		want                      string
	}{
		{"ldflags release", "0.0.9", "(devel)", "abc1234def", "babbl 0.0.9"},
		{"ldflags with v prefix", "v0.0.9", "", "", "babbl 0.0.9"},
		{"go install module version", "dev", "v0.0.9", "", "babbl 0.0.9"},
		{"source build with revision", "dev", "(devel)", "abc1234def5678", "babbl dev (abc1234)"},
		{"nothing available", "dev", "", "", "babbl dev"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := versionString(tt.stamped, tt.module, tt.revision); got != tt.want {
				t.Errorf("versionString(%q, %q, %q) = %q, want %q",
					tt.stamped, tt.module, tt.revision, got, tt.want)
			}
		})
	}
}

func TestVersionCommand(t *testing.T) {
	out, err := runCLI(t, "", "version")
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	if !strings.HasPrefix(out, "babbl ") || strings.Count(out, "\n") != 1 {
		t.Errorf("version printed %q, want a single line starting with \"babbl \"", out)
	}

	if _, err := runCLI(t, "", "version", "extra"); err == nil {
		t.Error("version with an argument succeeded, want an error")
	}
}
