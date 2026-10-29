package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

// syncBuffer is a bytes.Buffer safe to read while the server writes to it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newLoggingServer(t *testing.T) (*Client, *syncBuffer) {
	t.Helper()
	var out syncBuffer
	srv := httptest.NewServer(NewServer(newTestStore(t), newEventLog(&out)))
	t.Cleanup(srv.Close)
	return NewClient(srv.URL), &out
}

func TestLogRecordsPublishAndDelivery(t *testing.T) {
	c, out := newLoggingServer(t)
	ctx := context.Background()

	if _, err := c.Publish(ctx, "review", "fix nil deref", "claude"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, ok, err := c.Next(ctx, "review", "codex", false); err != nil || !ok {
		t.Fatalf("Next: %v (ok=%v)", err, ok)
	}

	lines := logLines(out)
	if len(lines) != 2 {
		t.Fatalf("got %d log lines, want 2:\n%s", len(lines), out)
	}
	for i, want := range []string{
		`^\d\d:\d\d:\d\d pub \s+review#0\s+from=claude\s+13B\s+"fix nil deref"$`,
		`^\d\d:\d\d:\d\d sub \s+review#0\s+as=codex\s+13B\s+"fix nil deref"$`,
	} {
		if !regexp.MustCompile(want).MatchString(lines[i]) {
			t.Errorf("line %d is %q, want match for %s", i, lines[i], want)
		}
	}
}

// The CLI re-polls every minute for as long as a subscriber is blocked. Those
// empty responses must not reach the log.
func TestLogSkipsEmptyLongPolls(t *testing.T) {
	var out syncBuffer
	srv := httptest.NewServer(NewServer(newTestStore(t), newEventLog(&out)))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/topics/quiet/next?as=codex&wait=1ms", "", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if got := out.String(); got != "" {
		t.Fatalf("expected no output for an empty long poll, got:\n%s", got)
	}
}

func TestLogRecordsPeekAndTopicsAndErrors(t *testing.T) {
	c, out := newLoggingServer(t)
	ctx := context.Background()

	c.Publish(ctx, "review", "one", "")
	c.Peek(ctx, "review", 0, 50)
	c.Topics(ctx)
	http.Post(c.baseURL+"/topics/review/next", "", nil) // 400: no ?as=

	want := []string{
		"pub    review#0",
		"peek   review",
		"topics",
		"error  review",
	}
	lines := logLines(out)
	if len(lines) != len(want) {
		t.Fatalf("got %d log lines, want %d:\n%s", len(lines), len(want), out)
	}
	for i, w := range want {
		if !strings.Contains(lines[i], w) {
			t.Errorf("line %d is %q, want it to contain %q", i, lines[i], w)
		}
	}
}

func TestLogRecordsDelete(t *testing.T) {
	c, out := newLoggingServer(t)
	ctx := context.Background()

	c.Publish(ctx, "review", "one", "")
	c.Publish(ctx, "review", "two", "")
	c.Next(ctx, "review", "codex", false)
	if err := c.Delete(ctx, "review"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	lines := logLines(out)
	want := `^\d\d:\d\d:\d\d rm \s+review\s+2 messages, 1 subscriber$`
	if last := lines[len(lines)-1]; !regexp.MustCompile(want).MatchString(last) {
		t.Fatalf("last line is %q, want match for %s", last, want)
	}
}

// The rm line already explains why parked subscribers were dropped, so waking
// them must not add an error line apiece.
func TestLogSkipsSubscribersWokenByDelete(t *testing.T) {
	var out syncBuffer
	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store, newEventLog(&out)))
	t.Cleanup(srv.Close)
	c := NewClient(srv.URL)

	done := make(chan struct{})
	go func() {
		c.Next(context.Background(), "quiet", "codex", false)
		close(done)
	}()
	waitForCursor(t, store, "quiet", "codex")
	if err := c.Delete(context.Background(), "quiet"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	<-done

	lines := logLines(&out)
	if len(lines) != 1 || !strings.Contains(lines[0], "rm     quiet") {
		t.Fatalf("want only the rm line, got:\n%s", &out)
	}
}

// Names are logged before they are validated, so a topic or publisher carrying
// terminal escapes must reach the log quoted.
func TestLogQuotesHostileNames(t *testing.T) {
	c, out := newLoggingServer(t)
	ctx := context.Background()

	c.Publish(ctx, "review", "hi", "\x1b[31mclaude")
	http.Post(c.baseURL+"/topics/%1b%5b31mreview/next?as=codex", "", nil)

	got := out.String()
	if strings.ContainsRune(got, 0x1b) {
		t.Fatalf("escape character reached the log:\n%q", got)
	}
	for _, want := range []string{`from="\x1b[31mclaude"`, `"\x1b[31mreview"`} {
		if !strings.Contains(got, want) {
			t.Errorf("log is\n%s\nwant it to contain %s", got, want)
		}
	}
}

// A nil log is the no-verbose case and must not panic.
func TestNilLogIsNoOp(t *testing.T) {
	var l *eventLog
	l.event("pub", "review", "from=claude")
	l.msg("sub", Message{Topic: "review"}, "as=codex")
}

func TestPreview(t *testing.T) {
	const room = previewLen - 3 // what fits between the quotes and the ellipsis
	// nel is a control character that quotes as the six-rune \u0085, the
	// longest escape preview has to keep whole. Built from its code point so
	// the source file itself stays plain ASCII.
	nel := string(rune(0x85))
	cases := []struct{ in, want string }{
		{"short", `"short"`},
		{"two\nlines", `"two\nlines"`},
		{"", `""`},
		{strings.Repeat("a", 100), `"` + strings.Repeat("a", room) + `…"`},
		// An escape sequence straddling the cut is dropped whole, never split.
		{strings.Repeat("a", room) + "\nx", `"` + strings.Repeat("a", room) + `…"`},
		{strings.Repeat("a", room-1) + "\nx", `"` + strings.Repeat("a", room-1) + `…"`},
		{strings.Repeat("a", 55) + "\x01", `"` + strings.Repeat("a", 55) + `…"`},
		{strings.Repeat("a", 53) + nel, `"` + strings.Repeat("a", 53) + `…"`},
		// ...but one that fits is kept intact, ahead of the cut.
		{nel + strings.Repeat("a", 80), `"\u0085` + strings.Repeat("a", room-6) + `…"`},
		{"\x01" + strings.Repeat("a", 80), `"\x01` + strings.Repeat("a", room-4) + `…"`},
	}
	for _, tc := range cases {
		if got := preview(tc.in); got != tc.want {
			t.Errorf("preview(%q) =\n\t%s, want\n\t%s", tc.in, got, tc.want)
		}
	}
}

// Whatever preview returns must be valid quoted text and must fit the budget,
// wherever the cut happens to land. Sweeping the offset walks a control
// character, a multi-byte rune and a quote past every position near the
// boundary, which is where truncation can split a token.
func TestPreviewStaysWellFormedAtEveryOffset(t *testing.T) {
	nel := string(rune(0x85))
	for _, mid := range []string{"\x01", "\x7f", "\n", "\t", nel, "\\", `"`, "é", "😀"} {
		for n := range previewLen + 8 {
			in := strings.Repeat("a", n) + mid + strings.Repeat("b", 8)
			got := preview(in)

			// Put a closing quote back where the ellipsis is, then the whole
			// thing should parse as the Go quoted string it claims to be.
			quoted := got
			if trimmed, cut := strings.CutSuffix(got, `…"`); cut {
				quoted = trimmed + `"`
			}
			unquoted, err := strconv.Unquote(quoted)
			if err != nil {
				t.Fatalf("preview(%d a's + %q) = %s: not valid quoted text: %v", n, mid, got, err)
			}
			if !strings.HasPrefix(in, unquoted) {
				t.Fatalf("preview(%d a's + %q) = %s: %q is not a prefix of the payload", n, mid, got, unquoted)
			}
			if r := utf8.RuneCountInString(got); r > previewLen {
				t.Fatalf("preview(%d a's + %q) is %d runes, want at most %d", n, mid, r, previewLen)
			}
		}
	}
}

func TestByteCount(t *testing.T) {
	cases := []struct {
		in   int
		want string
	}{{0, "0B"}, {512, "512B"}, {1024, "1.0kB"}, {2560, "2.5kB"}}
	for _, tc := range cases {
		if got := byteCount(tc.in); got != tc.want {
			t.Errorf("byteCount(%d) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

func logLines(out *syncBuffer) []string {
	s := strings.TrimRight(out.String(), "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}
