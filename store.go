package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Message is a single entry in a topic's log. ID is the message's 0-based
// offset within that log, so it doubles as the cursor position.
type Message struct {
	Topic     string    `json:"topic"`
	ID        int       `json:"id"`
	Payload   string    `json:"payload"`
	From      string    `json:"from,omitempty"`
	Published time.Time `json:"published_at"`
}

// TopicInfo summarises a topic for `babbl topics`.
type TopicInfo struct {
	Name    string         `json:"name"`
	Count   int            `json:"count"`
	Cursors map[string]int `json:"cursors"`
}

var topicNameRE = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// ErrInvalidTopic is returned for topic names that would not map safely to a
// filename. We reject rather than escape, so the on-disk layout stays readable.
var ErrInvalidTopic = errors.New("invalid topic name: must match [A-Za-z0-9._-]+")

// ErrInvalidSubscriber is returned for empty or malformed subscriber names.
var ErrInvalidSubscriber = errors.New("invalid subscriber name: must match [A-Za-z0-9._-]+")

// validName is shared by topics and subscribers. The regex allows "." and
// "..", which would be path traversal for a topic filename, so exclude them
// explicitly. Subscribers are only ever map keys, but the same rule keeps the
// two namespaces consistent and avoids surprising cursors.json entries.
func validName(name string) bool {
	return topicNameRE.MatchString(name) && name != "." && name != ".."
}

func validTopic(name string) error {
	if !validName(name) {
		return ErrInvalidTopic
	}
	return nil
}

func validSubscriber(name string) error {
	if !validName(name) {
		return ErrInvalidSubscriber
	}
	return nil
}

// Store is the durable message log. Everything is held in memory and mirrored
// to disk; a local agent tool never holds enough messages for that to matter.
type Store struct {
	dir string

	mu      sync.Mutex
	topics  map[string][]Message
	cursors map[string]map[string]int
	// notify is closed and replaced on every append, waking all long-poll
	// waiters at once. Waiters take a reference under the lock, release it,
	// then select on the channel.
	notify chan struct{}
}

// Open loads any existing state from dir, creating it if necessary.
func Open(dir string) (*Store, error) {
	s := &Store{
		dir:     dir,
		topics:  map[string][]Message{},
		cursors: map[string]map[string]int{},
		notify:  make(chan struct{}),
	}
	if err := os.MkdirAll(s.topicsDir(), 0o755); err != nil {
		return nil, fmt.Errorf("creating state dir: %w", err)
	}
	if err := s.loadTopics(); err != nil {
		return nil, err
	}
	if err := s.loadCursors(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) topicsDir() string   { return filepath.Join(s.dir, "topics") }
func (s *Store) cursorsPath() string { return filepath.Join(s.dir, "cursors.json") }
func (s *Store) topicPath(topic string) string {
	return filepath.Join(s.topicsDir(), topic+".jsonl")
}

func (s *Store) loadTopics() error {
	entries, err := os.ReadDir(s.topicsDir())
	if err != nil {
		return fmt.Errorf("reading topics dir: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		topic := strings.TrimSuffix(e.Name(), ".jsonl")
		if validTopic(topic) != nil {
			continue
		}
		msgs, err := readLog(filepath.Join(s.topicsDir(), e.Name()))
		if err != nil {
			return fmt.Errorf("loading topic %q: %w", topic, err)
		}
		s.topics[topic] = msgs
	}
	return nil
}

func readLog(path string) ([]Message, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var msgs []Message
	sc := bufio.NewScanner(f)
	// Review comments can be long; allow generously sized lines.
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var m Message
		if err := json.Unmarshal(line, &m); err != nil {
			return nil, fmt.Errorf("line %d: %w", len(msgs)+1, err)
		}
		msgs = append(msgs, m)
	}
	return msgs, sc.Err()
}

func (s *Store) loadCursors() error {
	data, err := os.ReadFile(s.cursorsPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading cursors: %w", err)
	}
	if err := json.Unmarshal(data, &s.cursors); err != nil {
		return fmt.Errorf("parsing cursors: %w", err)
	}
	if s.cursors == nil {
		s.cursors = map[string]map[string]int{}
	}
	return nil
}

// saveCursors writes cursors.json atomically. Caller must hold s.mu.
func (s *Store) saveCursors() error {
	data, err := json.MarshalIndent(s.cursors, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, "cursors-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), s.cursorsPath())
}

// Append adds a message to the end of a topic's log and wakes any waiters.
func (s *Store) Append(topic, payload, from string) (Message, error) {
	if err := validTopic(topic); err != nil {
		return Message{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	m := Message{
		Topic:     topic,
		ID:        len(s.topics[topic]),
		Payload:   payload,
		From:      from,
		Published: time.Now().UTC(),
	}
	if err := s.appendToLog(m); err != nil {
		return Message{}, err
	}
	s.topics[topic] = append(s.topics[topic], m)

	close(s.notify)
	s.notify = make(chan struct{})
	return m, nil
}

// appendToLog persists one message. Caller must hold s.mu.
func (s *Store) appendToLog(m Message) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.topicPath(m.Topic), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("opening topic log: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("writing topic log: %w", err)
	}
	return f.Sync()
}

// Next returns the next unread message for subscriber, blocking until one is
// available or ctx expires. The second return value is false on expiry.
//
// If fromLatest is set and the subscriber has no cursor yet, its cursor starts
// at the current end of the log rather than at 0, skipping the backlog.
//
// A subscriber's cursor is recorded the first time it asks, even if it then has
// to wait, so that Topics can report who is listening to a quiet topic.
//
// The cursor advances as soon as the message is returned; there is no ack.
func (s *Store) Next(ctx context.Context, topic, subscriber string, fromLatest bool) (Message, bool, error) {
	if err := validTopic(topic); err != nil {
		return Message{}, false, err
	}
	if err := validSubscriber(subscriber); err != nil {
		return Message{}, false, err
	}

	for {
		s.mu.Lock()
		msgs := s.topics[topic]
		subs, ok := s.cursors[topic]
		if !ok {
			subs = map[string]int{}
			s.cursors[topic] = subs
		}
		pos, seen := subs[subscriber]
		if !seen && fromLatest {
			pos = len(msgs)
		}
		if pos < len(msgs) {
			m := msgs[pos]
			subs[subscriber] = pos + 1
			err := s.saveCursors()
			s.mu.Unlock()
			if err != nil {
				return Message{}, false, err
			}
			return m, true, nil
		}
		// Nothing to read yet. Record where a first-time subscriber starts
		// before parking on it, so that `babbl topics` shows it waiting rather
		// than hiding it until something is finally delivered. This branch is
		// only reachable with pos already at the end of the log, so the stored
		// position is the one the subscriber would have resumed from anyway.
		if !seen {
			subs[subscriber] = pos
			if err := s.saveCursors(); err != nil {
				s.mu.Unlock()
				return Message{}, false, err
			}
		}
		// Grab the current notify channel before releasing the lock so an
		// append racing with us can't slip through unnoticed.
		wait := s.notify
		s.mu.Unlock()

		select {
		case <-wait:
			// Something was published; loop round and re-check.
		case <-ctx.Done():
			return Message{}, false, nil
		}
	}
}

// Peek returns up to limit messages starting at offset from, without touching
// any cursor.
func (s *Store) Peek(topic string, from, limit int) ([]Message, error) {
	if err := validTopic(topic); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := s.topics[topic]
	if from < 0 {
		from = 0
	}
	if from >= len(msgs) {
		return []Message{}, nil
	}
	end := len(msgs)
	if limit > 0 && from+limit < end {
		end = from + limit
	}
	out := make([]Message, end-from)
	copy(out, msgs[from:end])
	return out, nil
}

// Topics lists every known topic, sorted by name.
func (s *Store) Topics() []TopicInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	names := make([]string, 0, len(s.topics))
	for name := range s.topics {
		names = append(names, name)
	}
	// A topic can exist in cursors but not topics if it was only subscribed to.
	for name := range s.cursors {
		if _, ok := s.topics[name]; !ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	out := make([]TopicInfo, 0, len(names))
	for _, name := range names {
		cursors := map[string]int{}
		for sub, pos := range s.cursors[name] {
			cursors[sub] = pos
		}
		out = append(out, TopicInfo{
			Name:    name,
			Count:   len(s.topics[name]),
			Cursors: cursors,
		})
	}
	return out
}
