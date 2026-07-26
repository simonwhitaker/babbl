package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestServer(t *testing.T) (*Client, *Store) {
	t.Helper()
	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store))
	t.Cleanup(srv.Close)
	return NewClient(srv.URL), store
}

func TestServerPublishThenNext(t *testing.T) {
	c, _ := newTestServer(t)
	ctx := context.Background()

	m, err := c.Publish(ctx, "review", "fix nil deref in foo.go:42", "claude")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if m.ID != 0 || m.Topic != "review" || m.From != "claude" {
		t.Fatalf("unexpected published message: %+v", m)
	}

	got, ok, err := c.Next(ctx, "review", "codex", false)
	if err != nil || !ok {
		t.Fatalf("Next: %v (ok=%v)", err, ok)
	}
	if got.Payload != "fix nil deref in foo.go:42" {
		t.Fatalf("got payload %q", got.Payload)
	}
}

// A brand-new subscriber gets everything published before it existed.
func TestServerBacklogForNewSubscriber(t *testing.T) {
	c, _ := newTestServer(t)
	ctx := context.Background()

	c.Publish(ctx, "review", "one", "")
	c.Publish(ctx, "review", "two", "")

	for _, want := range []string{"one", "two"} {
		m, ok, err := c.Next(ctx, "review", "codex", false)
		if err != nil || !ok || m.Payload != want {
			t.Fatalf("got %q (ok=%v, err=%v), want %q", m.Payload, ok, err, want)
		}
	}
}

// The raw endpoint returns 204 when the server-side wait elapses with nothing
// available; the client turns that into a retry loop.
func TestServerLongPollReturns204(t *testing.T) {
	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/topics/quiet/next?as=codex&wait=100ms")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("got %s, want 204", resp.Status)
	}
}

// The client keeps polling past individual 204s until its own context expires.
func TestClientNextTimesOut(t *testing.T) {
	c, _ := newTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_, ok, err := c.Next(ctx, "quiet", "codex", false)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if ok {
		t.Fatal("expected no message")
	}
}

// A subscriber blocked on the server is woken by a publish over HTTP.
func TestServerNextWakesOnPublish(t *testing.T) {
	c, _ := newTestServer(t)

	done := make(chan Message, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m, ok, err := c.Next(ctx, "review", "codex", false)
		if err != nil || !ok {
			t.Errorf("Next: %v (ok=%v)", err, ok)
			close(done)
			return
		}
		done <- m
	}()

	time.Sleep(100 * time.Millisecond)
	if _, err := c.Publish(context.Background(), "review", "late arrival", ""); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case m, open := <-done:
		if !open {
			t.Fatal("Next failed")
		}
		if m.Payload != "late arrival" {
			t.Fatalf("got %q", m.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Next did not wake on publish")
	}
}

func TestServerPeekDoesNotAdvanceCursor(t *testing.T) {
	c, _ := newTestServer(t)
	ctx := context.Background()

	c.Publish(ctx, "review", "one", "")
	c.Publish(ctx, "review", "two", "")

	msgs, err := c.Peek(ctx, "review", 0, 50)
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("Peek returned %d messages, want 2", len(msgs))
	}

	m, ok, err := c.Next(ctx, "review", "codex", false)
	if err != nil || !ok || m.Payload != "one" {
		t.Fatalf("got %q (ok=%v, err=%v), want %q", m.Payload, ok, err, "one")
	}
}

func TestServerTopics(t *testing.T) {
	c, _ := newTestServer(t)
	ctx := context.Background()

	c.Publish(ctx, "review", "one", "")
	c.Publish(ctx, "review", "two", "")
	c.Next(ctx, "review", "codex", false)

	infos, err := c.Topics(ctx)
	if err != nil {
		t.Fatalf("Topics: %v", err)
	}
	if len(infos) != 1 || infos[0].Count != 2 || infos[0].Cursors["codex"] != 1 {
		t.Fatalf("unexpected topics: %+v", infos)
	}
}

func TestServerRejectsBadRequests(t *testing.T) {
	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store))
	defer srv.Close()

	cases := []struct {
		name, url string
	}{
		{"missing subscriber", "/topics/review/next"},
		{"invalid topic", "/topics/has%20space/next?as=codex"},
		{"invalid subscriber", "/topics/review/next?as=has%20space"},
		{"invalid wait", "/topics/review/next?as=codex&wait=banana"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Get(srv.URL + tc.url)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("got %s, want 400", resp.Status)
			}
		})
	}
}
