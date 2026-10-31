package main

import (
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/spf13/cobra"
)

// version is stamped at release time by GoReleaser's default ldflags
// (-X main.version=...). Builds without it fall back to the Go build info.
var version = "dev"

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the babbl version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var module, revision string
			if info, ok := debug.ReadBuildInfo(); ok {
				module = info.Main.Version
				for _, s := range info.Settings {
					if s.Key == "vcs.revision" {
						revision = s.Value
					}
				}
			}
			_, err := fmt.Fprintln(cmd.OutOrStdout(), versionString(version, module, revision))
			return err
		},
	}
}

// versionString picks the most specific version available: the ldflags-stamped
// release version, then the module version (set by `go install ...@vX`), then
// the VCS revision of a source build.
func versionString(stamped, module, revision string) string {
	v := stamped
	if v == "" || v == "dev" {
		v = ""
		if module != "" && module != "(devel)" {
			v = module
		}
	}
	if v != "" {
		return "babbl " + strings.TrimPrefix(v, "v")
	}
	if revision != "" {
		return fmt.Sprintf("babbl dev (%.7s)", revision)
	}
	return "babbl dev"
}
