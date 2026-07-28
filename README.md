# babbl

A tiny local pubsub daemon for coding agents.

One agent publishes to a topic, another subscribes to it. Messages are held in a
durable log on disk and each named subscriber has its own cursor, so a consumer
that starts an hour later still gets everything that was published for it.

The motivating case: Claude finishes a code review and pushes the comments to a
topic; Codex picks them up and addresses them, whenever it next runs.

No broker and no config. The only dependency is cobra, for the CLI.

## Install

Using Homebrew:

```sh
brew install simonwhitaker/tap/babbl
```

Or using Go:

```sh
go install github.com/simonwhitaker/babbl@latest
```

Or from a checkout:

```sh
go build -o babbl .
```

## Use

Start the daemon once (it listens on `127.0.0.1:8787` by default):

```sh
babbl serve &
```

Publish:

```sh
babbl pub review-comments 'fix nil deref in foo.go:42' --from claude

# multi-line payloads come from stdin
git diff | babbl pub diffs
```

Subscribe. `--as` names the subscriber, which is what gives it its own cursor.
The command blocks until a message is available, prints the payload, and exits:

```sh
babbl sub review-comments --as codex
# fix nil deref in foo.go:42
```

Loop over a topic from a shell:

```sh
while babbl sub review-comments --as codex; do :; done
```

Inspect state without consuming anything:

```sh
babbl peek review-comments
babbl topics
```

## Watching traffic

`serve --verbose` prints a line to stderr as each message is published and
delivered, like a web server's request log:

```sh
babbl serve --verbose
```

```
15:04:05 pub    review-comments#0        from=claude        26B  "fix nil deref in foo.go:42"
15:04:11 sub    review-comments#0        as=codex           26B  "fix nil deref in foo.go:42"
15:04:12 pub    diffs#0                  from=claude      3.9kB  "diff --git a/main.go b/main.go\nindex 0a1b2c3…"
15:04:20 peek   review-comments          1 message
15:04:31 error  review-comments          invalid subscriber name: must match [A-Za-z0-9._-]+
```

Payloads are quoted and truncated, so a multi-line message stays on one line.

Subscribers parked on an empty topic are not logged: `babbl sub` re-polls every
minute for as long as it blocks, and logging that would bury the real traffic.
A blocked subscriber shows up when a message actually reaches it — or in
`babbl topics`, which lists every cursor.

## Semantics

- **Fan-out, not load balancing.** Every distinct `--as` name receives every
  message on the topic. Two agents subscribing as `codex` share one cursor; as
  `codex` and `claude` they each get their own copy.
- **Backlog by default.** A subscriber name seen for the first time starts at
  the beginning of the topic. Pass `--latest` to start at the end instead.
- **Subscribers register on first contact.** A name shows up in `babbl topics`
  as soon as it first asks for a message, even if it then blocks without
  receiving one, so a subscriber parked on a quiet topic is still visible.
- **At-most-once.** The cursor advances as soon as a message is handed over.
  There are no acks in this version, so a message is lost if the consumer dies
  mid-work.
- **Ordering.** Per topic, strictly in publish order.

## Exit codes for `babbl sub`

| Code | Meaning                                         |
| ---- | ----------------------------------------------- |
| `0`  | A message was delivered on stdout               |
| `3`  | Timed out (`--timeout`) with nothing to deliver |
| `1`  | Error                                           |

The distinct timeout code lets an agent loop branch without parsing output.
`--timeout 0` (the default) blocks indefinitely.

## Commands

```
babbl serve [--addr ADDR] [--dir PATH] [--verbose]
babbl pub <topic> [message] [--from NAME] [--json]
babbl sub <topic> --as NAME [--timeout D] [--latest] [--json]
babbl peek <topic> [--from N] [--limit N] [--json]
babbl topics [--json]
```

`babbl help <command>` or `babbl <command> --help` documents each one.

Flags may appear before or after the positional arguments. Use `--` to publish a
message that starts with a dash:

```sh
babbl pub notes -- '--this is not a flag'
```

Topic and subscriber names must match `[A-Za-z0-9._-]+`.

Shell completions:

```sh
babbl completion fish > ~/.config/fish/completions/babbl.fish
# also: bash, zsh, powershell
```

## Configuration

| Variable     | Default                                              | Purpose                        |
| ------------ | ---------------------------------------------------- | ------------------------------ |
| `BABBL_ADDR` | `127.0.0.1:8787`                                     | Daemon address (also `--addr`) |
| `BABBL_DIR`  | `$XDG_STATE_HOME/babbl`, else `~/.local/state/babbl` | State directory (also `--dir`) |

The daemon binds to loopback only and has no authentication — it is not meant to
be exposed to a network. Under Docker, publish the port on loopback:

```sh
docker run -d -p 127.0.0.1:8787:8787 -v babbl-state:/var/lib/babbl babbl
```

`docker compose up -d` does this for you.

## On-disk format

```
<dir>/topics/<topic>.jsonl   one JSON message per line; line number == message ID
<dir>/cursors.json           {"review-comments": {"codex": 3, "claude": 0}}
```

Both are plain text, so `cat` and `jq` work fine for debugging, and deleting a
topic file or editing a cursor by hand does what you would expect (after a
daemon restart).

## HTTP API

The CLI is a thin client over an HTTP API, so `curl` works too:

```
POST /topics/{topic}                              body is the raw payload
GET  /topics/{topic}/next?as=NAME&wait=30s        long-poll; 200 + message, or 204
GET  /topics/{topic}?from=0&limit=50              peek, no cursor change
GET  /topics                                      list topics and cursors
GET  /healthz
```

## Not implemented

Acks and redelivery, log compaction, wildcard subscriptions, competing-consumer
groups, and auth. The on-disk log plus cursor map is a reasonable substrate for
all of them if they turn out to be needed.

## Development

```sh
go test ./...
go build -o babbl .
```
