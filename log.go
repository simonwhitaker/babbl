package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// eventLog writes one line per interesting daemon event, in the spirit of a web
// server's request log. It deliberately does not log the long-poll requests
// that deliver nothing: the CLI re-issues those every minute for as long as a
// subscriber is blocked, so logging them would bury the real traffic.
//
// A nil *eventLog is a no-op, which is what `serve` without --verbose uses.
type eventLog struct {
	mu sync.Mutex
	w  io.Writer
}

func newEventLog(w io.Writer) *eventLog { return &eventLog{w: w} }

// previewLen bounds the quoted payload excerpt on a log line.
const previewLen = 60

// msg logs a message crossing the daemon. The details are padded to fixed
// widths so the payload previews line up in a scrolling terminal even when a
// message carries no publisher or the sizes differ in length.
func (l *eventLog) msg(kind string, m Message, field string) {
	l.line(kind, fmt.Sprintf("%s#%d", safe(m.Topic), m.ID),
		fmt.Sprintf("%-14s %7s  %s", field, byteCount(len(m.Payload)), preview(m.Payload)))
}

// event logs something that concerns a topic as a whole rather than one
// message: a peek, a topic listing, or a rejected request.
func (l *eventLog) event(kind, topic string, details ...string) {
	l.line(kind, safe(topic), details...)
}

// line prints one record: a timestamp, the kind of event, the subject it
// concerns, and any details. Empty details are dropped so columns don't drift.
// subject must already have been through safe.
func (l *eventLog) line(kind, subject string, details ...string) {
	if l == nil {
		return
	}
	var kept []string
	for _, d := range details {
		if d != "" {
			kept = append(kept, d)
		}
	}
	out := fmt.Sprintf("%s %-6s %-24s %s",
		time.Now().Format("15:04:05"), kind, subject, strings.Join(kept, "  "))

	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintln(l.w, strings.TrimRight(out, " "))
}

// safe quotes anything that isn't a legal topic or subscriber name. Names are
// logged even on the paths that go on to reject them, and they arrive verbatim
// from a request, so an unquoted one could smuggle control characters into the
// terminal watching the log.
func safe(name string) string {
	if name == "" || validName(name) {
		return name
	}
	return strconv.Quote(name)
}

// preview quotes a payload so newlines and control characters stay on one line,
// truncating anything long enough to wrap a terminal. The result is always
// well-formed quoted text: the cut lands between escape sequences, never inside
// one, so a payload holding a control character near the boundary can't leave a
// half-written \xNN behind.
func preview(payload string) string {
	q := strconv.Quote(payload)
	if utf8.RuneCountInString(q) <= previewLen {
		return q
	}
	// Walk the quoted body one token at a time. UnquoteChar consumes a whole
	// escape sequence where there is one, and a whole rune otherwise, so
	// stopping between tokens is always safe.
	body := q[1 : len(q)-1]
	var kept strings.Builder
	n := 0 // runes kept, leaving room for the two quotes and the ellipsis
	for body != "" {
		_, _, tail, err := strconv.UnquoteChar(body, '"')
		if err != nil {
			break // not reachable for output of Quote, but don't emit junk if it is
		}
		token := body[:len(body)-len(tail)]
		if n+utf8.RuneCountInString(token) > previewLen-3 {
			break
		}
		kept.WriteString(token)
		n += utf8.RuneCountInString(token)
		body = tail
	}
	return `"` + kept.String() + `…"`
}

func byteCount(n int) string {
	if n < 1024 {
		return strconv.Itoa(n) + "B"
	}
	return fmt.Sprintf("%.1fkB", float64(n)/1024)
}
