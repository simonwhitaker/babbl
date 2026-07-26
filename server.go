package main

import (
	"context"
	"encoding/json"
	"errors"
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

// NewServer builds the HTTP handler for a store.
func NewServer(s *Store) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok\n")
	})

	mux.HandleFunc("GET /topics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.Topics())
	})

	mux.HandleFunc("POST /topics/{topic}", func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxPayload))
		if err != nil {
			writeError(w, http.StatusRequestEntityTooLarge, err)
			return
		}
		m, err := s.Append(topic, string(body), r.URL.Query().Get("from"))
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, m)
	})

	mux.HandleFunc("GET /topics/{topic}/next", func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		q := r.URL.Query()

		subscriber := q.Get("as")
		if subscriber == "" {
			writeError(w, http.StatusBadRequest, errors.New("missing required query parameter: as"))
			return
		}

		wait := defaultWait
		if v := q.Get("wait"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
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
			writeStoreError(w, err)
			return
		}
		if !ok {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, m)
	})

	mux.HandleFunc("GET /topics/{topic}", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		from, _ := strconv.Atoi(q.Get("from"))
		limit := 50
		if v := q.Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				writeError(w, http.StatusBadRequest, errors.New("invalid limit: "+v))
				return
			}
			limit = n
		}
		msgs, err := s.Peek(r.PathValue("topic"), from, limit)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, msgs)
	})

	return mux
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
