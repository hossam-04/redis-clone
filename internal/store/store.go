// Package store holds the key-value data every client shares.
//
// It knows nothing about RESP, connections, or command names. Keeping it
// ignorant of the protocol is what will let milestone 3 add persistence and
// milestone 2 add expiry without either one having to care how a client
// phrased the request.
package store

import (
	"sync"
	"time"
)

// entry is one stored value plus its metadata.
//
// A zero expiresAt means "never expires", which is the common case -- most
// keys have no TTL, so the zero value has to be the cheap one.
//
// time.Time rather than an int64 of Unix nanos because time.Time carries Go's
// monotonic clock reading. Expiry compares two instants, and a wall-clock
// comparison would break if NTP stepped the clock backwards or a user changed
// the system time: keys would suddenly live longer or die early. The cost is
// 24 bytes per entry instead of 8.
type entry struct {
	value     string
	expiresAt time.Time
}

// expired reports whether e has an expiry that has already passed.
func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// Store is the key-value map every client shares.
//
// The mutex is not optional. Go maps are not safe for concurrent use, and two
// goroutines writing one at the same time does not merely lose an update --
// the runtime detects it and kills the whole process with "concurrent map
// writes". One racing client would take down every other client with it.
//
// RWMutex rather than Mutex because cache workloads are read-dominated:
// RLock lets any number of readers run at once, and only writers exclude
// everyone. Milestone 2 revisits whether a single lock over the entire map is
// the right granularity, since it serialises every writer in the server.
type Store struct {
	mu sync.RWMutex
	m  map[string]entry

	// now is swappable so tests can control the passage of time instead of
	// sleeping. Same idea as the parser taking an io.Reader rather than a
	// net.Conn: depend on the capability, not the concrete thing, and the
	// hard-to-produce condition becomes trivial to produce.
	now func() time.Time
}

// New returns an empty Store ready for concurrent use.
func New() *Store {
	return &Store{
		m:   make(map[string]entry),
		now: time.Now,
	}
}

// Get reports the value and whether the key exists and has not expired. That
// second return is what lets the caller distinguish a missing key from a key
// holding the empty string -- the distinction RESP's null bulk string exists
// for.
//
// Expired keys are deleted here, on access. This is the lazy half of expiry;
// the sweeper handles keys nobody ever asks for.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	e, ok := s.m[key]
	s.mu.RUnlock()

	if !ok {
		return "", false
	}
	if !e.expired(s.now()) {
		return e.value, true
	}

	// The key is expired and must be deleted, which needs the write lock --
	// but we hold the read lock, and Go's RWMutex cannot upgrade one to the
	// other. There is no atomic way to do it: we must drop the read lock and
	// take the write lock, and in that gap another goroutine can do anything
	// it likes to this key.
	//
	// So everything has to be re-checked from scratch. Acting on what we read
	// before the gap would mean deleting a value some other client set in it.
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok = s.m[key]
	switch {
	case !ok:
		// Someone else deleted it, or the sweeper got there first.
		return "", false
	case e.expired(s.now()):
		delete(s.m, key)
		return "", false
	default:
		// Replaced with a fresh value during the gap. It is not ours to
		// delete, and the client asking now deserves the current value.
		return e.value, true
	}
}

// Set stores value under key with no expiry, discarding any TTL the key
// previously had. That matches Redis: a plain SET clears an existing TTL.
func (s *Store) Set(key, value string) {
	s.put(key, entry{value: value})
}

// SetWithTTL stores value under key and expires it after ttl.
func (s *Store) SetWithTTL(key, value string, ttl time.Duration) {
	s.put(key, entry{value: value, expiresAt: s.now().Add(ttl)})
}

func (s *Store) put(key string, e entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = e
}
