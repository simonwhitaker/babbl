package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
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

	mux.HandleFunc("GET /topics/{topic}/next", func(w http.ResponseWriter, r *http.Request) {
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
			log.event("error", topic, err.Error())
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

	return mux
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
	if errors.Is(err, ErrInvalidTopic) || errors.Is(err, ErrInvalidSubscriber) {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeError(w, http.StatusInternalServerError, err)
}
