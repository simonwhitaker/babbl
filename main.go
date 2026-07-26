package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

const (
	exitOK      = 0
	exitError   = 1
	exitTimeout = 3 // `babbl sub` deadline expired with no message
)

const defaultAddr = "127.0.0.1:8787"

func main() {
	root := rootCmd()

	// A root command with no RunE makes cobra print help and exit 0 for a bare
	// `babbl`. An incomplete invocation must not look like success to a script
	// or agent harness, so handle that one case here. Everything else —
	// including unknown subcommands — stays with cobra, which already errors
	// and offers suggestions.
	if len(os.Args) == 1 {
		root.Help()
		fmt.Fprintln(os.Stderr, "babbl: a subcommand is required")
		os.Exit(exitError)
	}

	err := root.Execute()
	if err == nil {
		os.Exit(exitOK)
	}
	// A `sub` timeout is a normal outcome, not a failure: exit 3 silently so
	// shell loops can branch on it.
	if errors.Is(err, errSubTimeout) {
		os.Exit(exitTimeout)
	}
	fmt.Fprintf(os.Stderr, "babbl: %v\n", err)
	os.Exit(exitError)
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "babbl",
		Short: "A local pubsub daemon for coding agents",
		Long: `babbl is a local pubsub daemon for coding agents.

One agent publishes to a topic, another subscribes to it. Messages are held in
a durable log on disk and each named subscriber has its own cursor, so a
consumer that starts an hour later still gets everything published for it.

  babbl serve &
  babbl pub review-comments 'fix nil deref in foo.go:42'
  babbl sub review-comments --as codex

Environment:
  BABBL_ADDR   daemon address, same as --addr (default ` + defaultAddr + `)
  BABBL_DIR    state directory for the daemon, same as 'serve --dir'
               (default $XDG_STATE_HOME/babbl, else ~/.local/state/babbl)`,
		SilenceUsage: true, // a runtime failure isn't a usage error
		// Cobra prints errors itself, but with an "Error: " prefix that
		// doesn't match the rest of our output. We print them instead.
		SilenceErrors: true,
	}

	root.PersistentFlags().String("addr", addrFromEnv(), "daemon address [$BABBL_ADDR]")
	root.AddCommand(serveCmd(), pubCmd(), subCmd(), peekCmd(), topicsCmd())
	return root
}

// addr reads the --addr flag, which is defined once on the root command.
func addr(cmd *cobra.Command) string {
	v, _ := cmd.Flags().GetString("addr")
	return v
}

// stateDir resolves the state directory: $BABBL_DIR, else $XDG_STATE_HOME/babbl,
// else ~/.local/state/babbl.
func stateDir() (string, error) {
	if d := os.Getenv("BABBL_DIR"); d != "" {
		return d, nil
	}
	if d := os.Getenv("XDG_STATE_HOME"); d != "" {
		return filepath.Join(d, "babbl"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolving home directory: %w", err)
	}
	return filepath.Join(home, ".local", "state", "babbl"), nil
}

func addrFromEnv() string {
	if a := os.Getenv("BABBL_ADDR"); a != "" {
		return a
	}
	return defaultAddr
}

func serveCmd() *cobra.Command {
	var dir string
	var verbose bool

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the babbl daemon",
		Long: `Run the babbl daemon.

Binds to loopback only and has no authentication; it is not meant to be exposed
to a network.

--verbose prints a line to stderr as each message is published, delivered, or
peeked at:

  15:04:05 pub    review-comments#0        from=claude  26B  "fix nil deref…"
  15:04:11 sub    review-comments#0        as=codex     26B  "fix nil deref…"

Subscribers waiting on an empty topic are not logged; they re-poll on a timer
and would drown out everything else.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			d := dir
			if d == "" {
				var err error
				if d, err = stateDir(); err != nil {
					return err
				}
			}

			store, err := Open(d)
			if err != nil {
				return err
			}

			ln, err := net.Listen("tcp", addr(cmd))
			if err != nil {
				return fmt.Errorf("listening on %s: %w", addr(cmd), err)
			}
			var log *eventLog
			if verbose {
				log = newEventLog(cmd.ErrOrStderr())
			}
			srv := &http.Server{Handler: NewServer(store, log)}
			fmt.Fprintf(os.Stderr, "babbl serving on %s (state: %s)\n", ln.Addr(), d)

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			errCh := make(chan error, 1)
			go func() { errCh <- srv.Serve(ln) }()

			select {
			case err := <-errCh:
				if errors.Is(err, http.ErrServerClosed) {
					return nil
				}
				return err
			case <-ctx.Done():
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				srv.Shutdown(shutdownCtx)
				return nil
			}
		},
	}

	cmd.Flags().StringVar(&dir, "dir", "", "state directory [$BABBL_DIR] (default $XDG_STATE_HOME/babbl, else ~/.local/state/babbl)")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "log each message as it is published and delivered")
	return cmd
}

func pubCmd() *cobra.Command {
	var from string
	var asJSON bool

	cmd := &cobra.Command{
		Use:   "pub <topic> [message]",
		Short: "Publish a message to a topic",
		Long: `Publish a message to a topic.

Reads the payload from stdin if no message argument is given, which is how
multi-line content gets published.`,
		Example: `  babbl pub review-comments 'fix nil deref in foo.go:42'
  babbl pub review-comments 'needs a test' --from claude
  git diff | babbl pub diffs`,
		Args:    cobra.MinimumNArgs(1),
		Aliases: []string{"publish"},
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := args[0]

			var payload string
			if len(args) > 1 {
				payload = strings.Join(args[1:], " ")
			} else {
				data, err := io.ReadAll(cmd.InOrStdin())
				if err != nil {
					return fmt.Errorf("reading stdin: %w", err)
				}
				payload = strings.TrimRight(string(data), "\n")
			}
			if payload == "" {
				return errors.New("refusing to publish an empty message")
			}

			m, err := NewClient(addr(cmd)).Publish(cmd.Context(), topic, payload, from)
			if err != nil {
				return err
			}
			if asJSON {
				return printJSON(cmd.OutOrStdout(), m)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s#%d\n", m.Topic, m.ID)
			return nil
		},
	}

	cmd.Flags().StringVar(&from, "from", "", "publisher name recorded on the message")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the full message envelope")
	return cmd
}

// errSubTimeout maps to exit code 3 so shell loops can branch on "nothing
// arrived" without parsing output. It is silent: nothing is printed for it.
var errSubTimeout = errors.New("timed out waiting for a message")

func subCmd() *cobra.Command {
	var as string
	var timeout time.Duration
	var asJSON, fromLatest bool

	cmd := &cobra.Command{
		Use:   "sub <topic>",
		Short: "Block until the next unread message on a topic",
		Long: `Block until the next unread message on a topic, print it, and exit.

--as names the subscriber, and that name is what owns the read cursor. Every
distinct name receives every message on the topic; two subscribers sharing a
name share one cursor.

A name seen for the first time starts at the beginning of the topic and works
through the backlog. Pass --latest to start at the end instead.

Exit codes: 0 a message was printed, 3 timed out with nothing to deliver,
1 error.`,
		Example: `  babbl sub review-comments --as codex

  # drain a topic, then stop once it goes quiet
  while babbl sub review-comments --as codex --timeout 5s; do :; done`,
		Args:    cobra.ExactArgs(1),
		Aliases: []string{"subscribe"},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := contextWithOptionalTimeout(cmd.Context(), timeout)
			defer cancel()
			ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			m, ok, err := NewClient(addr(cmd)).Next(ctx, args[0], as, fromLatest)
			if err != nil {
				return err
			}
			if !ok {
				return errSubTimeout
			}
			if asJSON {
				return printJSON(cmd.OutOrStdout(), m)
			}
			fmt.Fprintln(cmd.OutOrStdout(), m.Payload)
			return nil
		},
	}

	cmd.Flags().StringVar(&as, "as", "", "subscriber name; owns the read cursor (required)")
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "give up after this long (0 waits forever)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the full message envelope instead of the payload")
	cmd.Flags().BoolVar(&fromLatest, "latest", false, "for a new subscriber, skip the backlog and start at the end")
	cmd.MarkFlagRequired("as")
	return cmd
}

func peekCmd() *cobra.Command {
	var from, limit int
	var asJSON bool

	cmd := &cobra.Command{
		Use:     "peek <topic>",
		Short:   "Read a topic without advancing any cursor",
		Args:    cobra.ExactArgs(1),
		Example: `  babbl peek review-comments --limit 5`,
		RunE: func(cmd *cobra.Command, args []string) error {
			msgs, err := NewClient(addr(cmd)).Peek(cmd.Context(), args[0], from, limit)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if asJSON {
				return printJSON(out, msgs)
			}
			for _, m := range msgs {
				fmt.Fprintf(out, "[%d] %s\n", m.ID, m.Payload)
			}
			return nil
		},
	}

	cmd.Flags().IntVar(&from, "from", 0, "start at this message ID")
	cmd.Flags().IntVar(&limit, "limit", 20, "maximum messages to show (0 for all)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print full message envelopes")
	return cmd
}

func topicsCmd() *cobra.Command {
	var asJSON bool

	cmd := &cobra.Command{
		Use:   "topics",
		Short: "List topics with message counts and subscriber cursors",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			infos, err := NewClient(addr(cmd)).Topics(cmd.Context())
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if asJSON {
				return printJSON(out, infos)
			}
			if len(infos) == 0 {
				fmt.Fprintln(out, "no topics yet")
				return nil
			}
			for _, info := range infos {
				fmt.Fprintf(out, "%s (%d %s)\n", info.Name, info.Count, plural(info.Count, "message"))
				for _, sub := range sortedKeys(info.Cursors) {
					pos := info.Cursors[sub]
					fmt.Fprintf(out, "  %s: %d/%d (%d unread)\n", sub, pos, info.Count, max(0, info.Count-pos))
				}
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	return cmd
}

func plural(n int, word string) string {
	if n == 1 {
		return word
	}
	return word + "s"
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// contextWithOptionalTimeout treats a zero or negative duration as "no deadline".
func contextWithOptionalTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, d)
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
