package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// maxWait caps how long a single long-poll request is held open. The CLI
// re-issues the request until its own deadline expires, so a shorter cap here
// just means more round trips, not a shorter effective wait.
const maxWait = 60 * time.Second

const defaultWait = 30 * time.Second

// maxPayload bounds a single published message (1 MiB).
const maxPayload = 1 << 20

// NewServer builds the HTTP handler for a store. A nil log disables the
// request log; see eventLog.
func NewServer(s *Store, log *eventLog) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok\n")
	})

	mux.HandleFunc("GET /topics", func(w http.ResponseWriter, r *http.Request) {
		infos := s.Topics()
		log.event("topics", "", fmt.Sprintf("%d %s", len(infos), plural(len(infos), "topic")))
		writeJSON(w, http.StatusOK, infos)
	})

	mux.HandleFunc("POST /topics/{topic}", func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxPayload))
		if err != nil {
			log.event("error", topic, "payload too large")
			writeError(w, http.StatusRequestEntityTooLarge, err)
			return
		}
		from := r.URL.Query().Get("from")
		m, err := s.Append(topic, string(body), from)
		if err != nil {
			log.event("error", topic, err.Error())
			writeStoreError(w, err)
			return
		}
		log.msg("pub", m, field("from", from))
		writeJSON(w, http.StatusCreated, m)
	})

	// Next advances a cursor, so it is a POST: that puts it under the
	// cross-origin check in guard, and a page can't fire it from an <img> or
	// <link>. GET is still served for clients and daemons from before the
	// switch, but refuses anything a browser sent cross-site.
	next := func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		q := r.URL.Query()

		subscriber := q.Get("as")
		if subscriber == "" {
			log.event("error", topic, "missing subscriber name")
			writeError(w, http.StatusBadRequest, errors.New("missing required query parameter: as"))
			return
		}

		wait := defaultWait
		if v := q.Get("wait"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				log.event("error", topic, "invalid wait duration: "+safe(v))
				writeError(w, http.StatusBadRequest, errors.New("invalid wait duration: "+v))
				return
			}
			wait = d
		}
		if wait > maxWait {
			wait = maxWait
		}
		if wait < 0 {
			wait = 0
		}

		// Unlike the CLI, wait=0 here means "don't block at all", so use
		// WithTimeout directly rather than the CLI's 0-means-forever helper.
		ctx, cancel := context.WithTimeout(r.Context(), wait)
		defer cancel()

		m, ok, err := s.Next(ctx, topic, subscriber, q.Get("from") == "latest")
		if err != nil {
			// A subscriber dropped by a delete is accounted for by the rm
			// line, so don't log one error per waiter on top of it.
			if !errors.Is(err, ErrTopicDeleted) {
				log.event("error", topic, err.Error())
			}
			writeStoreError(w, err)
			return
		}
		if !ok {
			// Nothing delivered: the client will simply ask again. Not logged.
			w.WriteHeader(http.StatusNoContent)
			return
		}
		log.msg("sub", m, field("as", subscriber))
		writeJSON(w, http.StatusOK, m)
	}
	mux.HandleFunc("POST /topics/{topic}/next", next)
	mux.HandleFunc("GET /topics/{topic}/next", func(w http.ResponseWriter, r *http.Request) {
		if crossSiteBrowser(r) {
			log.event("error", r.PathValue("topic"), "cross-origin request rejected: "+safe(r.Header.Get("Origin")))
			writeError(w, http.StatusForbidden, errors.New("cross-origin requests are not allowed"))
			return
		}
		next(w, r)
	})

	mux.HandleFunc("GET /topics/{topic}", func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		q := r.URL.Query()
		from, _ := strconv.Atoi(q.Get("from"))
		limit := 50
		if v := q.Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				log.event("error", topic, "invalid limit: "+safe(v))
				writeError(w, http.StatusBadRequest, errors.New("invalid limit: "+v))
				return
			}
			limit = n
		}
		msgs, err := s.Peek(topic, from, limit)
		if err != nil {
			log.event("error", topic, err.Error())
			writeStoreError(w, err)
			return
		}
		log.event("peek", topic, fmt.Sprintf("%d %s", len(msgs), plural(len(msgs), "message")))
		writeJSON(w, http.StatusOK, msgs)
	})

	mux.HandleFunc("DELETE /topics/{topic}", func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		count, subs, err := s.Delete(topic)
		if err != nil {
			log.event("error", topic, err.Error())
			writeStoreError(w, err)
			return
		}
		log.event("rm", topic, fmt.Sprintf("%d %s, %d %s",
			count, plural(count, "message"), subs, plural(subs, "subscriber")))
		w.WriteHeader(http.StatusNoContent)
	})

	return guard(mux, log)
}

// guard keeps web pages out of the API. babbl has no auth, so without it any
// site open in a browser on the same machine could publish to (or, via DNS
// rebinding, read and delete) the topics agents act on.
//
// The Host check defeats DNS rebinding: a rebound page still sends its own
// hostname, so only loopback names and IP literals are accepted. The port is
// ignored, since Docker may publish the daemon on a different one. IP literals
// are allowed so the daemon stays reachable on whatever address it is bound to.
//
// CrossOriginProtection rejects cross-site POST and DELETE, which a page can
// otherwise send without a preflight. The CLI sends neither Origin nor
// Sec-Fetch-Site, so it is unaffected.
func guard(h http.Handler, log *eventLog) http.Handler {
	cop := http.NewCrossOriginProtection()
	cop.SetDenyHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.event("error", "", "cross-origin request rejected: "+safe(r.Header.Get("Origin")))
		writeError(w, http.StatusForbidden, errors.New("cross-origin requests are not allowed"))
	}))
	h = cop.Handler(h)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !loopbackHost(r.Host) {
			log.event("error", "", "unexpected Host header: "+safe(r.Host))
			writeError(w, http.StatusForbidden, errors.New("unexpected Host header: "+r.Host))
			return
		}
		h.ServeHTTP(w, r)
	})
}

// crossSiteBrowser reports whether a request came from a page on another
// origin. It is the GET counterpart of CrossOriginProtection, which exempts
// safe methods. Browsers send Sec-Fetch-Site on every request; "none" means the
// user typed the URL. Origin is the fallback for browsers that predate it.
func crossSiteBrowser(r *http.Request) bool {
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" {
		return site != "same-origin" && site != "none"
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	u, err := url.Parse(origin)
	return err != nil || u.Host != r.Host
}

// loopbackHost reports whether a Host header names localhost or an IP literal,
// with or without a port.
func loopbackHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.Trim(host, "[]"), ".")
	return strings.EqualFold(host, "localhost") || net.ParseIP(host) != nil
}

// field renders an optional key=value detail, or "" if the value is unset.
// `from` is arbitrary text off the query string, hence safe.
func field(key, value string) string {
	if value == "" {
		return ""
	}
	return key + "=" + safe(value)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrInvalidTopic) || errors.Is(err, ErrInvalidSubscriber):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, ErrTopicNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, ErrTopicDeleted):
		writeError(w, http.StatusGone, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}
