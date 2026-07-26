package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
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
)

const (
	exitOK      = 0
	exitError   = 1
	exitTimeout = 3 // `babbl sub` deadline expired with no message
)

const defaultAddr = "127.0.0.1:8787"

const usage = `babbl - a local pubsub daemon for coding agents

Usage:
  babbl serve [--addr ADDR] [--dir PATH]
  babbl pub <topic> [message]        publish (reads stdin if message omitted)
  babbl sub <topic> --as NAME        block until the next unread message
  babbl peek <topic> [--from N]      read without advancing any cursor
  babbl topics                       list topics, counts and cursors

Environment:
  BABBL_ADDR   daemon address (default ` + defaultAddr + `)
  BABBL_DIR    state directory (default $XDG_STATE_HOME/babbl)

Exit codes for sub: 0 message delivered, 3 timed out, 1 error.
`

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return exitError
	}

	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "serve":
		err = cmdServe(rest)
	case "pub", "publish":
		err = cmdPub(rest)
	case "sub", "subscribe":
		return cmdSub(rest)
	case "peek":
		err = cmdPeek(rest)
	case "topics":
		err = cmdTopics(rest)
	case "help", "-h", "--help":
		fmt.Print(usage)
		return exitOK
	default:
		fmt.Fprintf(os.Stderr, "babbl: unknown command %q\n\n%s", cmd, usage)
		return exitError
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "babbl: %v\n", err)
		return exitError
	}
	return exitOK
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

func cmdServe(args []string) error {
	fs := newFlagSet("serve")
	addr := fs.String("addr", addrFromEnv(), "address to listen on")
	dir := fs.String("dir", "", "state directory (default $BABBL_DIR or $XDG_STATE_HOME/babbl)")
	if _, err := parseArgs(fs, args); err != nil {
		return err
	}

	d := *dir
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

	srv := &http.Server{Handler: NewServer(store)}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", *addr, err)
	}

	fmt.Fprintf(os.Stderr, "babbl serving on %s (state: %s)\n", ln.Addr(), d)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
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
}

func cmdPub(args []string) error {
	fs := newFlagSet("pub")
	from := fs.String("from", "", "optional publisher name recorded on the message")
	asJSON := fs.Bool("json", false, "print the full message envelope")
	addr := fs.String("addr", addrFromEnv(), "daemon address")
	rest, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(rest) == 0 {
		return errors.New("usage: babbl pub <topic> [message]")
	}
	topic := rest[0]

	var payload string
	if len(rest) > 1 {
		payload = strings.Join(rest[1:], " ")
	} else {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("reading stdin: %w", err)
		}
		payload = strings.TrimRight(string(data), "\n")
	}
	if payload == "" {
		return errors.New("refusing to publish an empty message")
	}

	m, err := NewClient(*addr).Publish(context.Background(), topic, payload, *from)
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(m)
	}
	fmt.Printf("%s#%d\n", m.Topic, m.ID)
	return nil
}

func cmdSub(args []string) int {
	fs := newFlagSet("sub")
	as := fs.String("as", "", "subscriber name (required); each name has its own cursor")
	timeout := fs.Duration("timeout", 0, "give up after this long (0 = wait forever)")
	asJSON := fs.Bool("json", false, "print the full message envelope instead of the payload")
	fromLatest := fs.Bool("latest", false, "for a new subscriber, skip the backlog and start at the end")
	addr := fs.String("addr", addrFromEnv(), "daemon address")

	fail := func(err error) int {
		fmt.Fprintf(os.Stderr, "babbl: %v\n", err)
		return exitError
	}

	rest, err := parseArgs(fs, args)
	if err != nil {
		return fail(err)
	}
	if len(rest) != 1 {
		return fail(errors.New("usage: babbl sub <topic> --as NAME"))
	}
	if *as == "" {
		return fail(errors.New("--as NAME is required so the subscriber has its own cursor"))
	}

	ctx, cancel := contextWithOptionalTimeout(context.Background(), *timeout)
	defer cancel()
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	m, ok, err := NewClient(*addr).Next(ctx, rest[0], *as, *fromLatest)
	if err != nil {
		return fail(err)
	}
	if !ok {
		return exitTimeout
	}

	if *asJSON {
		if err := printJSON(m); err != nil {
			return fail(err)
		}
		return exitOK
	}
	fmt.Println(m.Payload)
	return exitOK
}

func cmdPeek(args []string) error {
	fs := newFlagSet("peek")
	from := fs.Int("from", 0, "start at this message ID")
	limit := fs.Int("limit", 20, "maximum messages to show (0 = all)")
	asJSON := fs.Bool("json", false, "print full message envelopes")
	addr := fs.String("addr", addrFromEnv(), "daemon address")
	rest, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(rest) != 1 {
		return errors.New("usage: babbl peek <topic>")
	}

	msgs, err := NewClient(*addr).Peek(context.Background(), rest[0], *from, *limit)
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(msgs)
	}
	for _, m := range msgs {
		fmt.Printf("[%d] %s\n", m.ID, m.Payload)
	}
	return nil
}

func cmdTopics(args []string) error {
	fs := newFlagSet("topics")
	asJSON := fs.Bool("json", false, "print JSON")
	addr := fs.String("addr", addrFromEnv(), "daemon address")
	if _, err := parseArgs(fs, args); err != nil {
		return err
	}

	infos, err := NewClient(*addr).Topics(context.Background())
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(infos)
	}
	if len(infos) == 0 {
		fmt.Println("no topics yet")
		return nil
	}
	for _, info := range infos {
		fmt.Printf("%s (%d %s)\n", info.Name, info.Count, plural(info.Count, "message"))
		for _, sub := range sortedKeys(info.Cursors) {
			pos := info.Cursors[sub]
			fmt.Printf("  %s: %d/%d (%d unread)\n", sub, pos, info.Count, max(0, info.Count-pos))
		}
	}
	return nil
}

// parseArgs parses flags that may appear before, after, or between positional
// arguments. Stdlib flag stops at the first non-flag token, which would make
// the natural `babbl sub <topic> --as codex` silently ignore --as.
//
// Everything after a literal "--" is treated as positional verbatim, which is
// how you publish a message that starts with "-".
func parseArgs(fs *flag.FlagSet, args []string) ([]string, error) {
	var verbatim []string
	for i, a := range args {
		if a == "--" {
			args, verbatim = args[:i], args[i+1:]
			break
		}
	}

	var positional []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		rest := fs.Args()
		if len(rest) == 0 {
			break
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
	return append(positional, verbatim...), nil
}

// newFlagSet returns a FlagSet that reports errors instead of calling os.Exit,
// so parseArgs can surface them through the normal error path.
func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	return fs
}

func plural(n int, word string) string {
	if n == 1 {
		return word
	}
	return word + "s"
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
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
