package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
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
var ErrInvalidTopic = errors.New("invalid topic name")

// ErrTopicNotFound is returned when deleting a topic that doesn't exist.
var ErrTopicNotFound = errors.New("no such topic")

// ErrTopicDeleted is returned to a subscriber whose topic was deleted while it
// was waiting on it.
var ErrTopicDeleted = errors.New("topic deleted")

// ErrInvalidSubscriber is returned for empty or malformed subscriber names.
var ErrInvalidSubscriber = errors.New("invalid subscriber name")

// ErrTopicConflict is returned for a new topic whose name differs from an
// existing one only in case. On a case-insensitive filesystem, macOS's default,
// the two would share one log file.
var ErrTopicConflict = errors.New("topic name conflict")

// windowsReserved are device names Windows won't use as a file name, whatever
// the extension, so a topic called "con" or "nul.x" couldn't have a log there.
var windowsReserved = map[string]bool{
	"con": true, "prn": true, "aux": true, "nul": true,
	"com1": true, "com2": true, "com3": true, "com4": true, "com5": true,
	"com6": true, "com7": true, "com8": true, "com9": true,
	"lpt1": true, "lpt2": true, "lpt3": true, "lpt4": true, "lpt5": true,
	"lpt6": true, "lpt7": true, "lpt8": true, "lpt9": true,
}

// ErrLimit is returned when a write would take the store past one of its
// limits. Everything is held in memory, so without them one runaway publisher
// or subscriber loop could grow the daemon without bound.
var ErrLimit = errors.New("limit reached")

// maxNameLen bounds new topic and subscriber names. It leaves a topic's
// filename, with ".jsonl", well inside the usual 255-byte limit.
const maxNameLen = 128

// maxSubscriptions bounds the (topic, subscriber) pairs in cursors.json, which
// is rewritten whole on every delivery.
const maxSubscriptions = 10_000

// validName is shared by topics and subscribers. The regex allows "." and
// "..", which would be path traversal for a topic filename, so exclude them
// explicitly. Subscribers are only ever map keys, but the same rule keeps the
// two namespaces consistent and avoids surprising cursors.json entries.
func validName(name string) bool {
	return topicNameRE.MatchString(name) && name != "." && name != ".."
}

func validTopic(name string) error {
	if !validName(name) {
		return fmt.Errorf("%w: must match [A-Za-z0-9._-]+", ErrInvalidTopic)
	}
	return nil
}

func validSubscriber(name string) error {
	if !validName(name) {
		return fmt.Errorf("%w: must match [A-Za-z0-9._-]+", ErrInvalidSubscriber)
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
	// deletions counts how many times each topic has been deleted. A waiter
	// notes the count before parking and gives up if it has moved on waking,
	// so deleting a topic stops its subscribers instead of leaving them to
	// re-register and bring it back.
	deletions map[string]int
	// notify is closed and replaced on every append, waking all long-poll
	// waiters at once. Waiters take a reference under the lock, release it,
	// then select on the channel.
	notify chan struct{}

	// MaxBytes caps the message bytes held across all topics; 0 means no cap.
	// Set it before the store is used. size is what is held now.
	MaxBytes int64
	size     int64

	// Repairs describes anything Open had to fix to load the state, for the
	// daemon to report.
	Repairs []string
}

// msgSize is what a message counts against MaxBytes: the parts a publisher
// controls, which dominate its footprint.
func msgSize(m Message) int64 { return int64(len(m.Payload) + len(m.From)) }

// Open loads any existing state from dir, creating it if necessary.
func Open(dir string) (*Store, error) {
	s := &Store{
		dir:       dir,
		topics:    map[string][]Message{},
		cursors:   map[string]map[string]int{},
		deletions: map[string]int{},
		notify:    make(chan struct{}),
	}
	if err := os.MkdirAll(s.topicsDir(), 0o700); err != nil {
		return nil, fmt.Errorf("creating state dir: %w", err)
	}
	if err := s.privatize(); err != nil {
		return nil, err
	}
	if err := s.loadTopics(); err != nil {
		return nil, err
	}
	if err := s.loadCursors(); err != nil {
		return nil, err
	}
	return s, nil
}

// privatize strips group and other permissions from the state dir and
// everything babbl keeps in it. Payloads are often diffs and review comments,
// so other users on the machine shouldn't be able to read them. MkdirAll and
// OpenFile only apply modes to what they create, so this is what tightens
// state left by older releases, which used 0755 and 0644. Symlinks are left
// alone rather than followed to whatever they point at.
func (s *Store) privatize() error {
	if runtime.GOOS == "windows" {
		return nil // permission bits don't govern access there
	}
	paths := []string{s.dir, s.topicsDir(), s.cursorsPath()}
	entries, err := os.ReadDir(s.topicsDir())
	if err != nil {
		return fmt.Errorf("reading topics dir: %w", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".jsonl") {
			paths = append(paths, filepath.Join(s.topicsDir(), e.Name()))
		}
	}
	for i, p := range paths {
		stat := os.Lstat
		if i == 0 {
			stat = os.Stat // the state dir itself may be a symlink, e.g. to a volume
		}
		fi, err := stat(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("checking permissions: %w", err)
		}
		if fi.Mode()&os.ModeSymlink != 0 || fi.Mode().Perm()&0o077 == 0 {
			continue
		}
		if err := os.Chmod(p, fi.Mode().Perm()&^0o077); err != nil {
			return fmt.Errorf("restricting permissions: %w", err)
		}
	}
	return nil
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
		msgs, repair, err := readLog(filepath.Join(s.topicsDir(), e.Name()))
		if err != nil {
			return fmt.Errorf("loading topic %q: %w", topic, err)
		}
		if repair != "" {
			s.Repairs = append(s.Repairs, fmt.Sprintf("topic %s: %s", topic, repair))
		}
		s.topics[topic] = msgs
		for _, m := range msgs {
			s.size += msgSize(m)
		}
	}
	return nil
}

// readLog loads a topic log. A last line with no newline is the mark of a
// write cut short by a crash: appendToLog writes each record and its newline
// in one call, and only acknowledges the publish once that is synced. Such a
// line is dropped, or, if it does parse (a hand edit, say), given the newline
// it lacks so the next append doesn't run into it. Either way repair says what
// was done. A bad line anywhere else is real corruption and stays an error.
func readLog(path string) (msgs []Message, repair string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 64*1024)
	var off int64 // end of the last complete line
	for n := 1; ; n++ {
		line, err := r.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, "", err
		}
		if err == io.EOF {
			if len(bytes.TrimSpace(line)) == 0 {
				return msgs, "", nil
			}
			var m Message
			if json.Unmarshal(line, &m) == nil {
				msgs = append(msgs, m)
				return msgs, "added a missing final newline", appendNewline(path)
			}
			if err := os.Truncate(path, off); err != nil {
				return nil, "", fmt.Errorf("dropping incomplete line %d: %w", n, err)
			}
			return msgs, fmt.Sprintf("dropped incomplete line %d (%d bytes), left by an interrupted write", n, len(line)), nil
		}
		off += int64(len(line))
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var m Message
		if err := json.Unmarshal(line, &m); err != nil {
			return nil, "", fmt.Errorf("line %d: %w", n, err)
		}
		msgs = append(msgs, m)
	}
}

func appendNewline(path string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte{'\n'}); err != nil {
		f.Close()
		return err
	}
	return f.Close()
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

	if !s.knownTopic(topic) {
		if err := s.checkNewTopic(topic); err != nil {
			return Message{}, err
		}
	}
	m := Message{
		Topic:     topic,
		ID:        len(s.topics[topic]),
		Payload:   payload,
		From:      from,
		Published: time.Now().UTC(),
	}
	if s.MaxBytes > 0 && s.size+msgSize(m) > s.MaxBytes {
		return Message{}, fmt.Errorf("%w: the store holds %.1f of the %.1f MB allowed by serve --max-store-mb; free space with babbl topics rm",
			ErrLimit, float64(s.size)/(1<<20), float64(s.MaxBytes)/(1<<20))
	}
	if err := s.appendToLog(m); err != nil {
		return Message{}, err
	}
	s.topics[topic] = append(s.topics[topic], m)
	s.size += msgSize(m)

	s.wake()
	return m, nil
}

// knownTopic reports whether a topic has a log or any cursors. Caller must hold
// s.mu.
func (s *Store) knownTopic(topic string) bool {
	_, hasLog := s.topics[topic]
	_, hasCursors := s.cursors[topic]
	return hasLog || hasCursors
}

// checkNewTopic applies the rules for creating a topic, which are stricter than
// those for using one: names already on disk keep working. Caller must hold
// s.mu.
//
// The rules apply on every platform, so a state directory stays portable
// between them.
func (s *Store) checkNewTopic(topic string) error {
	if len(topic) > maxNameLen {
		return fmt.Errorf("%w: at most %d characters", ErrInvalidTopic, maxNameLen)
	}
	base, _, _ := strings.Cut(strings.ToLower(topic), ".")
	if windowsReserved[base] {
		return fmt.Errorf("%w: %q is a reserved device name on Windows", ErrInvalidTopic, topic)
	}
	for name := range s.topics {
		if strings.EqualFold(name, topic) {
			return caseConflict(topic, name)
		}
	}
	for name := range s.cursors {
		if strings.EqualFold(name, topic) {
			return caseConflict(topic, name)
		}
	}
	return nil
}

func caseConflict(topic, existing string) error {
	return fmt.Errorf("%w: %q differs from existing topic %q only in case, which some filesystems can't tell apart",
		ErrTopicConflict, topic, existing)
}

// checkNewSubscriber applies the rules for a subscriber's first request on a
// topic. Caller must hold s.mu.
func (s *Store) checkNewSubscriber(topic, subscriber string) error {
	if !s.knownTopic(topic) {
		if err := s.checkNewTopic(topic); err != nil {
			return err
		}
	}
	if len(subscriber) > maxNameLen {
		return fmt.Errorf("%w: at most %d characters", ErrInvalidSubscriber, maxNameLen)
	}
	n := 0
	for _, subs := range s.cursors {
		n += len(subs)
	}
	if n >= maxSubscriptions {
		return fmt.Errorf("%w: %d subscribers across all topics; free some with babbl topics rm", ErrLimit, n)
	}
	return nil
}

// wake releases every long-poll waiter so it re-checks its topic. Caller must
// hold s.mu.
func (s *Store) wake() {
	close(s.notify)
	s.notify = make(chan struct{})
}

// appendToLog persists one message. Caller must hold s.mu.
func (s *Store) appendToLog(m Message) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.topicPath(m.Topic), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
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

	s.mu.Lock()
	deletions := s.deletions[topic]
	s.mu.Unlock()

	for {
		s.mu.Lock()
		if s.deletions[topic] != deletions {
			s.mu.Unlock()
			return Message{}, false, ErrTopicDeleted
		}
		msgs := s.topics[topic]
		subs, ok := s.cursors[topic]
		pos, seen := subs[subscriber]
		if !seen {
			if err := s.checkNewSubscriber(topic, subscriber); err != nil {
				s.mu.Unlock()
				return Message{}, false, err
			}
		}
		if !ok {
			subs = map[string]int{}
			s.cursors[topic] = subs
		}
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

// Delete removes a topic's log and every subscriber cursor on it, reporting how
// many messages and subscribers it had. Subscribers waiting on the topic are
// woken with ErrTopicDeleted. Publishing to the name afterwards starts a new
// topic from ID 0.
//
// Cursors are removed before the log, so a crash in between leaves the topic's
// messages without cursors — still listed, and still deletable — rather than
// leaving cursors that point past the end of a topic that is later recreated.
func (s *Store) Delete(topic string) (messages, subscribers int, err error) {
	if err := validTopic(topic); err != nil {
		return 0, 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs, hasLog := s.topics[topic]
	subs, hasCursors := s.cursors[topic]
	if !hasLog && !hasCursors {
		return 0, 0, ErrTopicNotFound
	}

	if hasCursors {
		delete(s.cursors, topic)
		if err := s.saveCursors(); err != nil {
			s.cursors[topic] = subs
			return 0, 0, err
		}
	}
	// Bump the deletion count before anything else can fail, so waiters stop
	// even if the log can't be removed: their cursors are already gone.
	s.deletions[topic]++
	s.wake()

	if err := os.Remove(s.topicPath(topic)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, 0, fmt.Errorf("removing topic log: %w", err)
	}
	for _, m := range msgs {
		s.size -= msgSize(m)
	}
	delete(s.topics, topic)
	return len(msgs), len(subs), nil
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
		maps.Copy(cursors, s.cursors[name])
		out = append(out, TopicInfo{
			Name:    name,
			Count:   len(s.topics[name]),
			Cursors: cursors,
		})
	}
	return out
}
