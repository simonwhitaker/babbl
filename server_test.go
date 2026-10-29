package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestServer(t *testing.T) (*Client, *Store) {
	t.Helper()
	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store, nil))
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
	srv := httptest.NewServer(NewServer(store, nil))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/topics/quiet/next?as=codex&wait=100ms", "", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
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
	srv := httptest.NewServer(NewServer(store, nil))
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
			resp, err := http.Post(srv.URL+tc.url, "", nil)
			if err != nil {
				t.Fatalf("POST: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("got %s, want 400", resp.Status)
			}
		})
	}
}

func TestServerDelete(t *testing.T) {
	c, store := newTestServer(t)
	ctx := context.Background()

	c.Publish(ctx, "review", "one", "")
	if err := c.Delete(ctx, "review"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if infos := store.Topics(); len(infos) != 0 {
		t.Fatalf("topic survived delete: %+v", infos)
	}
}

func TestServerDeleteStatusCodes(t *testing.T) {
	store := newTestStore(t)
	store.Append("review", "one", "")
	srv := httptest.NewServer(NewServer(store, nil))
	defer srv.Close()

	cases := []struct {
		topic string
		want  int
	}{
		{"review", http.StatusNoContent},
		{"review", http.StatusNotFound}, // already gone
		{"has%20space", http.StatusBadRequest},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/topics/"+tc.topic, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("DELETE %s: %v", tc.topic, err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("DELETE %s: got %s, want %d", tc.topic, resp.Status, tc.want)
		}
	}
}

// A CLI subscriber blocked on a topic gets an error, not a silent re-poll, when
// that topic is deleted.
func TestServerDeleteWakesBlockedClient(t *testing.T) {
	c, store := newTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, _, err := c.Next(ctx, "quiet", "codex", false)
		errCh <- err
	}()
	waitForCursor(t, store, "quiet", "codex")

	if err := c.Delete(context.Background(), "quiet"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "deleted") {
			t.Fatalf("blocked Next returned %v, want a topic-deleted error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked client was not woken by delete")
	}
}

// A DNS-rebinding page reaches the daemon under its own hostname, so anything
// other than localhost or an IP literal is refused, on every route.
func TestServerRejectsForeignHost(t *testing.T) {
	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store, nil))
	defer srv.Close()

	for _, path := range []string{"/topics", "/topics/review", "/healthz"} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		req.Host = "rebind.evil.example:8787"
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("GET %s with foreign Host: got %s, want 403", path, resp.Status)
		}
	}
}

// Without --allow-host, only localhost and IP literals get through.
func TestServerHostCheck(t *testing.T) {
	cases := []struct {
		host  string
		allow []string
		want  int
	}{
		{"127.0.0.1:8787", nil, http.StatusOK},
		{"127.0.0.1", nil, http.StatusOK},
		{"localhost:8787", nil, http.StatusOK},
		{"LOCALHOST", nil, http.StatusOK},
		{"localhost.:8787", nil, http.StatusOK},
		{"[::1]:8787", nil, http.StatusOK},
		{"10.0.0.5:8787", nil, http.StatusOK}, // Docker or LAN binds; rebinding never yields an IP
		{"evil.example", nil, http.StatusForbidden},
		{"localhost.evil", nil, http.StatusForbidden},
		{"127.0.0.1.nip.io", nil, http.StatusForbidden},
		{"babbl:8787", nil, http.StatusForbidden},

		// --allow-host adds names; port, case and a trailing dot don't matter.
		{"babbl:8787", []string{"babbl"}, http.StatusOK},
		{"BABBL.:9000", []string{"babbl"}, http.StatusOK},
		{"babbl", []string{"Babbl:8787"}, http.StatusOK},
		{"babbl.internal", []string{"babbl", "babbl.internal"}, http.StatusOK},
		{"localhost", []string{"babbl"}, http.StatusOK},
		{"sub.babbl:8787", []string{"babbl"}, http.StatusForbidden}, // exact names only
		{"evil.example", []string{"babbl"}, http.StatusForbidden},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(NewServer(newTestStore(t), nil, tc.allow...))
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/healthz", nil)
		req.Host = tc.host
		resp, err := http.DefaultClient.Do(req)
		srv.Close()
		if err != nil {
			t.Fatalf("Host %q: %v", tc.host, err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("Host %q, allow %q: got %d, want %d", tc.host, tc.allow, resp.StatusCode, tc.want)
		}
	}
}

func TestValidHostName(t *testing.T) {
	for name, want := range map[string]bool{
		"babbl":          true,
		"babbl.internal": true,
		"Babbl:8787":     true,
		"my-babbl.":      true,
		"":               false,
		"*":              false,
		"*.internal":     false,
		"http://babbl":   false,
		"babbl/":         false,
		"-babbl":         false,
		"bab bl":         false,
	} {
		if got := validHostName(name); got != want {
			t.Errorf("validHostName(%q) = %v, want %v", name, got, want)
		}
	}
}

// A web page can POST text/plain or send a DELETE to localhost. Browsers mark
// those with Sec-Fetch-Site or Origin, and the server refuses them.
func TestServerRejectsCrossOriginWrites(t *testing.T) {
	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store, nil))
	defer srv.Close()

	store.Append("review", "keep me", "")

	for _, tc := range []struct {
		name, method, header, value string
	}{
		{"POST cross-site", http.MethodPost, "Sec-Fetch-Site", "cross-site"},
		{"POST foreign Origin", http.MethodPost, "Origin", "https://evil.example"},
		{"DELETE cross-site", http.MethodDelete, "Sec-Fetch-Site", "cross-site"},
	} {
		req, _ := http.NewRequest(tc.method, srv.URL+"/topics/review?from=claude", strings.NewReader("injected"))
		req.Header.Set("Content-Type", "text/plain;charset=UTF-8")
		req.Header.Set(tc.header, tc.value)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s: got %s, want 403", tc.name, resp.Status)
		}
	}

	msgs, _ := store.Peek("review", 0, 0)
	if len(msgs) != 1 || msgs[0].Payload != "keep me" {
		t.Fatalf("topic changed by rejected requests: %+v", msgs)
	}
}

// curl, the CLI and a page on the daemon's own origin are all still allowed.
func TestServerAllowsSameOriginAndNonBrowserWrites(t *testing.T) {
	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store, nil))
	defer srv.Close()

	for _, h := range []map[string]string{
		{},
		{"Sec-Fetch-Site": "same-origin"},
		{"Origin": srv.URL},
	} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/topics/review", strings.NewReader("hello"))
		for k, v := range h {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST with %v: %v", h, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("POST with %v: got %s, want 201", h, resp.Status)
		}
	}
}

// A page can't read a cross-origin response, but it doesn't need to: asking
// for the next message advances the cursor, and delivery is at-most-once.
// Neither method may be used to consume a message from another site.
func TestServerRejectsCrossOriginNext(t *testing.T) {
	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store, nil))
	defer srv.Close()

	store.Append("review", "keep me", "")

	for _, tc := range []struct {
		name, method, header, value string
	}{
		{"POST cross-site", http.MethodPost, "Sec-Fetch-Site", "cross-site"},
		{"POST foreign Origin", http.MethodPost, "Origin", "https://evil.example"},
		{"GET cross-site", http.MethodGet, "Sec-Fetch-Site", "cross-site"},
		{"GET same-site", http.MethodGet, "Sec-Fetch-Site", "same-site"},
		{"GET foreign Origin", http.MethodGet, "Origin", "https://evil.example"},
		{"GET opaque Origin", http.MethodGet, "Origin", "null"},
	} {
		req, _ := http.NewRequest(tc.method, srv.URL+"/topics/review/next?as=codex&wait=0s", nil)
		req.Header.Set(tc.header, tc.value)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s: got %s, want 403", tc.name, resp.Status)
		}
	}

	if cursors := store.Topics()[0].Cursors; len(cursors) != 0 {
		t.Fatalf("rejected requests registered a subscriber: %v", cursors)
	}
}

// The GET form survives for clients from before the switch to POST, and for
// curl. Without browser headers it still delivers.
func TestServerNextStillServesGET(t *testing.T) {
	store := newTestStore(t)
	srv := httptest.NewServer(NewServer(store, nil))
	defer srv.Close()

	store.Append("review", "one", "")
	store.Append("review", "two", "")

	for _, h := range []map[string]string{
		{},
		{"Sec-Fetch-Site": "none"},
	} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/topics/review/next?as=codex&wait=0s", nil)
		for k, v := range h {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET with %v: %v", h, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET with %v: got %s, want 200", h, resp.Status)
		}
	}
}

// A daemon left running from an older release only routes GET for next, so
// the client falls back to it when POST is refused.
func TestClientNextFallsBackToGET(t *testing.T) {
	var methods []string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /topics/{topic}/next", func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		writeJSON(w, http.StatusOK, Message{Topic: "review", Payload: "hello"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	m, ok, err := NewClient(srv.URL).Next(context.Background(), "review", "codex", false)
	if err != nil || !ok || m.Payload != "hello" {
		t.Fatalf("Next: %+v (ok=%v, err=%v)", m, ok, err)
	}
	if len(methods) != 1 {
		t.Fatalf("old server saw %v, want a single GET after the refused POST", methods)
	}
}

// The client uses POST against a current daemon, so it gets the stricter check.
func TestClientNextUsesPOST(t *testing.T) {
	var methods []string
	inner := NewServer(newTestStore(t), nil)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		inner.ServeHTTP(w, r)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	ctx := context.Background()

	c.Publish(ctx, "review", "hello", "")
	methods = nil
	if _, ok, err := c.Next(ctx, "review", "codex", false); err != nil || !ok {
		t.Fatalf("Next: ok=%v, err=%v", ok, err)
	}
	if len(methods) != 1 || methods[0] != http.MethodPost {
		t.Fatalf("client sent %v, want [POST]", methods)
	}
}
