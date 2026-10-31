package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

// next is a helper for the common "should return promptly" case.
func next(t *testing.T, s *Store, topic, sub string) (Message, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	m, ok, err := s.Next(ctx, topic, sub, false)
	if err != nil {
		t.Fatalf("Next(%s, %s): %v", topic, sub, err)
	}
	return m, ok
}

func TestAppendThenNext(t *testing.T) {
	s := newTestStore(t)

	if _, err := s.Append("review", "fix nil deref", "claude"); err != nil {
		t.Fatalf("Append: %v", err)
	}

	m, ok := next(t, s, "review", "codex")
	if !ok {
		t.Fatal("expected a message")
	}
	if m.Payload != "fix nil deref" || m.ID != 0 || m.From != "claude" {
		t.Fatalf("unexpected message: %+v", m)
	}

	// Cursor advanced, so a second read has nothing.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, ok, _ := s.Next(ctx, "review", "codex", false); ok {
		t.Fatal("expected no second message")
	}
}

// Each named subscriber has its own cursor, so both see every message.
func TestFanOutToMultipleSubscribers(t *testing.T) {
	s := newTestStore(t)
	s.Append("review", "one", "")
	s.Append("review", "two", "")

	for _, sub := range []string{"codex", "claude"} {
		for _, want := range []string{"one", "two"} {
			m, ok := next(t, s, "review", sub)
			if !ok || m.Payload != want {
				t.Fatalf("%s: got %q (ok=%v), want %q", sub, m.Payload, ok, want)
			}
		}
	}
}

// The motivating case: publish with nobody listening, subscribe later.
func TestBacklogDeliveredToLateSubscriber(t *testing.T) {
	s := newTestStore(t)
	s.Append("review", "published before anyone subscribed", "")

	m, ok := next(t, s, "review", "codex")
	if !ok || m.Payload != "published before anyone subscribed" {
		t.Fatalf("got %q (ok=%v)", m.Payload, ok)
	}
}

func TestFromLatestSkipsBacklog(t *testing.T) {
	s := newTestStore(t)
	s.Append("review", "old", "")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, ok, err := s.Next(ctx, "review", "codex", true); ok || err != nil {
		t.Fatalf("expected no backlog for --latest subscriber (ok=%v, err=%v)", ok, err)
	}
	// Waiting at the end of the log, not at the start of it.
	if pos, ok := cursorFor(s.Topics(), "review", "codex"); !ok || pos != 1 {
		t.Fatalf("--latest subscriber cursor is %d (present=%v), want 1", pos, ok)
	}

	s.Append("review", "new", "")
	if m, ok := next(t, s, "review", "codex"); !ok || m.Payload != "new" {
		t.Fatalf("got %q (ok=%v), want %q", m.Payload, ok, "new")
	}
}

// A subscriber blocked on a quiet topic has to be visible in `babbl topics`
// while it waits — that listing is the only way to see who is listening, since
// the daemon's request log deliberately says nothing about idle subscribers.
func TestParkedSubscriberIsVisibleInTopics(t *testing.T) {
	s := newTestStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go s.Next(ctx, "quiet", "codex", false)

	// The cursor is recorded just before the goroutine parks, so poll for it
	// rather than guessing how long it needs to get there.
	deadline := time.Now().Add(2 * time.Second)
	for {
		pos, ok := cursorFor(s.Topics(), "quiet", "codex")
		if ok {
			if pos != 0 {
				t.Fatalf("parked subscriber has cursor %d, want 0", pos)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("codex never showed up in Topics: %+v", s.Topics())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Registering a waiting subscriber must not consume anything on its behalf.
func TestParkedSubscriberStillGetsLaterMessages(t *testing.T) {
	s := newTestStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, ok, err := s.Next(ctx, "quiet", "codex", false); ok || err != nil {
		t.Fatalf("expected nothing to be delivered (ok=%v, err=%v)", ok, err)
	}

	s.Append("quiet", "arrived later", "")
	if m, ok := next(t, s, "quiet", "codex"); !ok || m.Payload != "arrived later" {
		t.Fatalf("got %q (ok=%v), want %q", m.Payload, ok, "arrived later")
	}
}

func cursorFor(infos []TopicInfo, topic, sub string) (int, bool) {
	for _, info := range infos {
		if info.Name == topic {
			pos, ok := info.Cursors[sub]
			return pos, ok
		}
	}
	return 0, false
}

// waitForCursor blocks until a subscriber parked in a goroutine has registered.
func waitForCursor(t *testing.T, s *Store, topic, sub string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := cursorFor(s.Topics(), topic, sub); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never registered on %s", sub, topic)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	s1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s1.Append("review", "one", "claude")
	s1.Append("review", "two", "claude")
	if m, ok := next(t, s1, "review", "codex"); !ok || m.Payload != "one" {
		t.Fatalf("setup read failed: %q ok=%v", m.Payload, ok)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	// Messages reloaded from disk...
	msgs, err := s2.Peek("review", 0, 0)
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	if len(msgs) != 2 || msgs[1].Payload != "two" || msgs[1].From != "claude" {
		t.Fatalf("unexpected reloaded messages: %+v", msgs)
	}

	// ...and the cursor picks up where it left off rather than replaying.
	if m, ok := next(t, s2, "review", "codex"); !ok || m.Payload != "two" {
		t.Fatalf("after reopen got %q (ok=%v), want %q", m.Payload, ok, "two")
	}
}

// A blocked Next must wake when a message arrives, not just on timeout.
func TestNextWakesOnAppend(t *testing.T) {
	s := newTestStore(t)

	type result struct {
		m  Message
		ok bool
	}
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m, ok, err := s.Next(ctx, "review", "codex", false)
		if err != nil {
			t.Errorf("Next: %v", err)
		}
		done <- result{m, ok}
	}()

	// Give the goroutine a moment to actually be parked on the notify channel.
	time.Sleep(50 * time.Millisecond)
	if _, err := s.Append("review", "late arrival", ""); err != nil {
		t.Fatalf("Append: %v", err)
	}

	select {
	case r := <-done:
		if !r.ok || r.m.Payload != "late arrival" {
			t.Fatalf("got %q (ok=%v)", r.m.Payload, r.ok)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Next did not wake on Append")
	}
}

func TestNextReturnsFalseOnContextExpiry(t *testing.T) {
	s := newTestStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	m, ok, err := s.Next(ctx, "quiet", "codex", false)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if ok {
		t.Fatalf("expected no message, got %+v", m)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("returned too early: %v", elapsed)
	}
}

func TestPeekDoesNotAdvanceCursor(t *testing.T) {
	s := newTestStore(t)
	s.Append("review", "one", "")
	s.Append("review", "two", "")

	if msgs, _ := s.Peek("review", 0, 0); len(msgs) != 2 {
		t.Fatalf("Peek returned %d messages, want 2", len(msgs))
	}
	if msgs, _ := s.Peek("review", 1, 0); len(msgs) != 1 || msgs[0].Payload != "two" {
		t.Fatalf("Peek(from=1) = %+v", msgs)
	}
	if msgs, _ := s.Peek("review", 0, 1); len(msgs) != 1 || msgs[0].Payload != "one" {
		t.Fatalf("Peek(limit=1) = %+v", msgs)
	}
	if msgs, _ := s.Peek("review", 99, 0); len(msgs) != 0 {
		t.Fatalf("Peek past end = %+v", msgs)
	}

	// Cursor untouched by all that peeking.
	if m, ok := next(t, s, "review", "codex"); !ok || m.Payload != "one" {
		t.Fatalf("got %q (ok=%v), want %q", m.Payload, ok, "one")
	}
}

func TestTopics(t *testing.T) {
	s := newTestStore(t)
	s.Append("review", "one", "")
	s.Append("review", "two", "")
	s.Append("alerts", "boom", "")
	next(t, s, "review", "codex")

	infos := s.Topics()
	if len(infos) != 2 {
		t.Fatalf("got %d topics, want 2", len(infos))
	}
	// Sorted by name: alerts before review.
	if infos[0].Name != "alerts" || infos[1].Name != "review" {
		t.Fatalf("unexpected order: %+v", infos)
	}
	if infos[1].Count != 2 || infos[1].Cursors["codex"] != 1 {
		t.Fatalf("unexpected review info: %+v", infos[1])
	}
}

func TestInvalidNamesRejected(t *testing.T) {
	s := newTestStore(t)

	// Bounded so a validation regression shows up as a failure rather than a
	// Next that blocks forever waiting for a message.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for _, bad := range []string{"", "../escape", "has/slash", "has space", ".", ".."} {
		if _, err := s.Append(bad, "x", ""); !errors.Is(err, ErrInvalidTopic) {
			t.Errorf("Append(%q): got %v, want ErrInvalidTopic", bad, err)
		}
		if _, _, err := s.Next(ctx, bad, "codex", false); !errors.Is(err, ErrInvalidTopic) {
			t.Errorf("Next(topic=%q): got %v, want ErrInvalidTopic", bad, err)
		}
		if _, _, err := s.Next(ctx, "review", bad, false); !errors.Is(err, ErrInvalidSubscriber) {
			t.Errorf("Next(sub=%q): got %v, want ErrInvalidSubscriber", bad, err)
		}
	}
}

func TestDeleteRemovesTopicAndCursors(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.Append("review", "one", "")
	s.Append("review", "two", "")
	s.Append("alerts", "boom", "")
	next(t, s, "review", "codex")
	next(t, s, "review", "claude")

	count, subs, err := s.Delete("review")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if count != 2 || subs != 2 {
		t.Fatalf("Delete reported %d messages, %d subscribers; want 2, 2", count, subs)
	}

	infos := s.Topics()
	if len(infos) != 1 || infos[0].Name != "alerts" {
		t.Fatalf("unexpected topics after delete: %+v", infos)
	}
	if _, err := os.Stat(filepath.Join(dir, "topics", "review.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("log file still present (stat err: %v)", err)
	}

	// The deletion is durable, not just in memory.
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if infos := s2.Topics(); len(infos) != 1 || infos[0].Name != "alerts" {
		t.Fatalf("unexpected topics after reopen: %+v", infos)
	}
}

// Reusing a deleted topic's name gives a brand-new topic.
func TestPublishAfterDeleteStartsFresh(t *testing.T) {
	s := newTestStore(t)
	s.Append("review", "old", "")
	next(t, s, "review", "codex")

	if _, _, err := s.Delete("review"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	m, err := s.Append("review", "new", "")
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if m.ID != 0 {
		t.Fatalf("first message after delete has ID %d, want 0", m.ID)
	}
	// codex's old cursor went with the topic, so it sees the new message.
	if got, ok := next(t, s, "review", "codex"); !ok || got.Payload != "new" {
		t.Fatalf("got %q (ok=%v), want %q", got.Payload, ok, "new")
	}
}

func TestDeleteUnknownTopic(t *testing.T) {
	s := newTestStore(t)
	if _, _, err := s.Delete("nope"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("got %v, want ErrTopicNotFound", err)
	}
	if _, _, err := s.Delete("has space"); !errors.Is(err, ErrInvalidTopic) {
		t.Fatalf("got %v, want ErrInvalidTopic", err)
	}
}

// A topic that has only ever been subscribed to shows up in Topics, so it must
// be deletable too.
func TestDeleteCursorOnlyTopic(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	s.Next(ctx, "quiet", "codex", false)

	count, subs, err := s.Delete("quiet")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if count != 0 || subs != 1 {
		t.Fatalf("Delete reported %d messages, %d subscribers; want 0, 1", count, subs)
	}
	if infos := s.Topics(); len(infos) != 0 {
		t.Fatalf("topic survived delete: %+v", infos)
	}
}

// A subscriber parked on a topic is woken by its deletion and told why, rather
// than waiting on — and resurrecting — a topic nobody cares about any more.
func TestDeleteWakesParkedSubscriber(t *testing.T) {
	s := newTestStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, _, err := s.Next(ctx, "quiet", "codex", false)
		errCh <- err
	}()

	// Wait for the subscriber to register before deleting out from under it.
	waitForCursor(t, s, "quiet", "codex")

	if _, _, err := s.Delete("quiet"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrTopicDeleted) {
			t.Fatalf("parked Next returned %v, want ErrTopicDeleted", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked subscriber was not woken by delete")
	}
	if infos := s.Topics(); len(infos) != 0 {
		t.Fatalf("deleted topic came back: %+v", infos)
	}
}

// Deleting one topic must not disturb subscribers parked on another.
func TestDeleteLeavesOtherSubscribersParked(t *testing.T) {
	s := newTestStore(t)
	s.Append("doomed", "x", "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := make(chan string, 1)
	go func() {
		m, _, err := s.Next(ctx, "quiet", "codex", false)
		if err != nil {
			got <- "error: " + err.Error()
			return
		}
		got <- m.Payload
	}()

	waitForCursor(t, s, "quiet", "codex")

	s.Delete("doomed")
	s.Append("quiet", "still listening", "")

	select {
	case p := <-got:
		if p != "still listening" {
			t.Fatalf("got %q, want %q", p, "still listening")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber on another topic never received its message")
	}
}

// Payloads are often diffs and review comments, so nothing babbl writes may
// be readable by other users on the machine.
func TestStoreStateIsPrivate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission bits don't govern access on Windows")
	}
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.Append("review", "secret diff", "")
	s.Next(context.Background(), "review", "codex", false)

	for _, p := range []string{dir, s.topicsDir(), s.topicPath("review"), s.cursorsPath()} {
		assertPrivate(t, p)
	}
}

// State from older releases was 0755/0644. Opening it tightens it in place and
// still loads it.
func TestOpenTightensExistingState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission bits don't govern access on Windows")
	}
	dir := filepath.Join(t.TempDir(), "state")
	topics := filepath.Join(dir, "topics")
	log := filepath.Join(topics, "review.jsonl")
	cursors := filepath.Join(dir, "cursors.json")
	os.MkdirAll(topics, 0o755)
	os.WriteFile(log, []byte(`{"topic":"review","id":0,"payload":"old"}`+"\n"), 0o644)
	os.WriteFile(cursors, []byte(`{"review":{"codex":0}}`), 0o644)
	for p, mode := range map[string]os.FileMode{dir: 0o755, topics: 0o755, log: 0o644, cursors: 0o644} {
		os.Chmod(p, mode) // defeat the umask, so the starting point is really wide
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, p := range []string{dir, topics, log, cursors} {
		assertPrivate(t, p)
	}
	if msgs, _ := s.Peek("review", 0, 0); len(msgs) != 1 || msgs[0].Payload != "old" {
		t.Fatalf("existing topic not loaded: %+v", msgs)
	}
	// Owner bits are kept, not reset: a 0644 file becomes 0600, not 0700.
	if fi, _ := os.Stat(log); fi.Mode().Perm() != 0o600 {
		t.Errorf("%s is %v, want 0600", log, fi.Mode().Perm())
	}
}

// A symlink in topics/ is left alone, not followed to chmod its target.
func TestOpenDoesNotChmodThroughSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission bits don't govern access on Windows")
	}
	dir := filepath.Join(t.TempDir(), "state")
	os.MkdirAll(filepath.Join(dir, "topics"), 0o700)
	elsewhere := filepath.Join(t.TempDir(), "shared.jsonl")
	os.WriteFile(elsewhere, nil, 0o644)
	os.Chmod(elsewhere, 0o644)
	os.Symlink(elsewhere, filepath.Join(dir, "topics", "linked.jsonl"))

	if _, err := Open(dir); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if fi, _ := os.Stat(elsewhere); fi.Mode().Perm() != 0o644 {
		t.Fatalf("symlink target changed to %v", fi.Mode().Perm())
	}
}

func assertPrivate(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		t.Errorf("%s is %v, want no group or other access", path, perm)
	}
}

func TestStoreMaxBytes(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	s.MaxBytes = 10

	if _, err := s.Append("review", "123456", "ab"); err != nil { // 8 bytes
		t.Fatalf("Append under the limit: %v", err)
	}
	if _, err := s.Append("review", "123", ""); !errors.Is(err, ErrLimit) { // would be 11
		t.Fatalf("Append over the limit: got %v, want ErrLimit", err)
	}
	if msgs, _ := s.Peek("review", 0, 0); len(msgs) != 1 {
		t.Fatalf("refused message was stored: %+v", msgs)
	}
	if _, err := s.Append("other", "12", ""); err != nil { // exactly 10
		t.Fatalf("Append up to the limit: %v", err)
	}

	// Usage survives a restart, so the limit can't be dodged by restarting.
	s2, _ := Open(dir)
	s2.MaxBytes = 10
	if _, err := s2.Append("review", "x", ""); !errors.Is(err, ErrLimit) {
		t.Fatalf("after reopen: got %v, want ErrLimit", err)
	}

	// Deleting a topic gives its bytes back.
	if _, _, err := s2.Delete("review"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s2.Append("review", "12345678", ""); err != nil {
		t.Fatalf("Append after delete: %v", err)
	}
}

func TestStoreNameLengthLimit(t *testing.T) {
	dir := t.TempDir()
	long := strings.Repeat("a", maxNameLen+1)

	// A long topic left by an older release still loads and still works.
	os.MkdirAll(filepath.Join(dir, "topics"), 0o700)
	os.WriteFile(filepath.Join(dir, "topics", long+".jsonl"), []byte(`{"topic":"`+long+`","id":0,"payload":"old"}`+"\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "cursors.json"), []byte(`{"review":{"`+long+`":0}}`), 0o600)
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	if _, err := s.Append(long, "new", ""); err != nil {
		t.Errorf("Append to existing long topic: %v", err)
	}
	s.Append("review", "hi", "")
	if _, ok, err := s.Next(ctx, "review", long, false); err != nil || !ok {
		t.Errorf("Next as existing long subscriber: ok=%v, err=%v", ok, err)
	}

	// New names are capped.
	if _, err := s.Append(strings.Repeat("b", maxNameLen), "x", ""); err != nil {
		t.Errorf("Append to a %d-character topic: %v", maxNameLen, err)
	}
	if _, err := s.Append(strings.Repeat("b", maxNameLen+1), "x", ""); !errors.Is(err, ErrInvalidTopic) {
		t.Errorf("Append to a new over-long topic: got %v, want ErrInvalidTopic", err)
	}
	expired, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := s.Next(expired, strings.Repeat("c", maxNameLen+1), "codex", false); !errors.Is(err, ErrInvalidTopic) {
		t.Errorf("Next on a new over-long topic: got %v, want ErrInvalidTopic", err)
	}
	if _, _, err := s.Next(expired, "review", strings.Repeat("d", maxNameLen+1), false); !errors.Is(err, ErrInvalidSubscriber) {
		t.Errorf("Next as a new over-long subscriber: got %v, want ErrInvalidSubscriber", err)
	}
	if infos := s.Topics(); len(infos) != 3 || len(infos[2].Cursors) != 1 {
		t.Errorf("refused requests left state behind: %+v", infos)
	}
}

func TestStoreSubscriberLimit(t *testing.T) {
	s := newTestStore(t)
	s.Append("review", "hi", "")
	full := map[string]int{}
	for i := range maxSubscriptions - 1 {
		full[fmt.Sprintf("s%d", i)] = 0
	}
	s.cursors["busy"] = full
	ctx := context.Background()

	if _, _, err := s.Next(ctx, "review", "last", false); err != nil {
		t.Fatalf("subscriber up to the limit: %v", err)
	}
	if _, _, err := s.Next(ctx, "review", "one-too-many", false); !errors.Is(err, ErrLimit) {
		t.Fatalf("subscriber over the limit: got %v, want ErrLimit", err)
	}
	// Subscribers that already have a cursor are unaffected.
	expired, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := s.Next(expired, "busy", "s0", false); err != nil {
		t.Fatalf("existing subscriber at the limit: %v", err)
	}
}

// On a case-insensitive filesystem "Review" and "review" would share a log, so
// a new topic may not differ from an existing one only in case.
func TestStoreRejectsCaseOnlyTopicConflicts(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	expired, cancel := context.WithCancel(ctx)
	cancel()

	s.Append("review", "one", "")
	s.Next(expired, "waiting", "codex", false) // a topic known only by its cursors

	for _, tc := range []struct{ topic, existing string }{
		{"Review", "review"},
		{"REVIEW", "review"},
		{"Waiting", "waiting"},
	} {
		_, err := s.Append(tc.topic, "x", "")
		if !errors.Is(err, ErrTopicConflict) || !strings.Contains(err.Error(), `"`+tc.existing+`"`) {
			t.Errorf("Append(%q): got %v, want a conflict naming %q", tc.topic, err, tc.existing)
		}
		if _, _, err := s.Next(expired, tc.topic, "codex", false); !errors.Is(err, ErrTopicConflict) {
			t.Errorf("Next(%q): got %v, want ErrTopicConflict", tc.topic, err)
		}
	}

	// The existing names themselves are fine, and so are unrelated ones.
	for _, topic := range []string{"review", "waiting", "reviews"} {
		if _, err := s.Append(topic, "x", ""); err != nil {
			t.Errorf("Append(%q): %v", topic, err)
		}
	}
	if n := len(s.Topics()); n != 3 {
		t.Errorf("got %d topics, want 3", n)
	}
}

func TestStoreRejectsWindowsReservedTopics(t *testing.T) {
	s := newTestStore(t)
	for _, bad := range []string{"con", "NUL", "Aux", "prn.txt", "com1", "lpt9.x.y"} {
		if _, err := s.Append(bad, "x", ""); !errors.Is(err, ErrInvalidTopic) {
			t.Errorf("Append(%q): got %v, want ErrInvalidTopic", bad, err)
		}
	}
	for _, ok := range []string{"console", "com10", "com0", "nul-results", "my.con"} {
		if _, err := s.Append(ok, "x", ""); err != nil {
			t.Errorf("Append(%q): %v", ok, err)
		}
	}
}

// A crash mid-write leaves a last line with no newline. It was never
// acknowledged, so Open drops it rather than refusing to start.
func TestOpenDropsTornLastLine(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "topics"), 0o700)
	log := filepath.Join(dir, "topics", "review.jsonl")
	good := `{"topic":"review","id":0,"payload":"kept"}` + "\n"
	os.WriteFile(log, []byte(good+`{"topic":"review","id":1,"pay`), 0o600)

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if len(s.Repairs) != 1 || !strings.Contains(s.Repairs[0], "dropped incomplete line 2") {
		t.Errorf("Repairs = %q", s.Repairs)
	}
	if data, _ := os.ReadFile(log); string(data) != good {
		t.Errorf("log after repair = %q, want %q", data, good)
	}

	// The next publish takes the dropped message's place and reloads cleanly.
	if m, err := s.Append("review", "next", ""); err != nil || m.ID != 1 {
		t.Fatalf("Append: %+v, %v", m, err)
	}
	s2, err := Open(dir)
	if err != nil || len(s2.Repairs) != 0 {
		t.Fatalf("reopen: %v, repairs %q", err, s2.Repairs)
	}
	if msgs, _ := s2.Peek("review", 0, 0); len(msgs) != 2 || msgs[1].Payload != "next" {
		t.Fatalf("after reopen: %+v", msgs)
	}
}

// A complete record that merely lacks its newline (a hand edit) is kept, and
// the newline added so the next append doesn't run into it.
func TestOpenKeepsCompleteLastLineWithoutNewline(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "topics"), 0o700)
	log := filepath.Join(dir, "topics", "review.jsonl")
	os.WriteFile(log, []byte(`{"topic":"review","id":0,"payload":"edited"}`), 0o600)

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if len(s.Repairs) != 1 || !strings.Contains(s.Repairs[0], "newline") {
		t.Errorf("Repairs = %q", s.Repairs)
	}
	s.Append("review", "next", "")
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if msgs, _ := s2.Peek("review", 0, 0); len(msgs) != 2 || msgs[0].Payload != "edited" {
		t.Fatalf("after reopen: %+v", msgs)
	}
}

// Damage before the last line isn't a torn write, so it still stops Open
// rather than being silently dropped.
func TestOpenRefusesCorruptionMidLog(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "topics"), 0o700)
	log := filepath.Join(dir, "topics", "review.jsonl")
	data := `{"topic":"review","id":0,"payload":"a"}` + "\n" + "garbage\n" + `{"topic":"review","id":2,"payload":"c"}` + "\n"
	os.WriteFile(log, []byte(data), 0o600)

	if _, err := Open(dir); err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("got %v, want an error at line 2", err)
	}
	if got, _ := os.ReadFile(log); string(got) != data {
		t.Fatalf("log was modified: %q", got)
	}
}
