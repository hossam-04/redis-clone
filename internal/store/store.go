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

	// volatile indexes exactly the keys that carry a deadline. The sweeper
	// samples from here rather than from m.
	//
	// Without it, sampling is useless at realistic ratios: a cache with a
	// million keys of which ten thousand have TTLs would spend roughly 199
	// samples in 200 looking at keys that can never expire. The deadline
	// itself stays in the entry so Get still needs only one lookup -- this
	// holds no timestamps, only names.
	volatile map[string]struct{}

	// now is swappable so tests can control the passage of time instead of
	// sleeping. Same idea as the parser taking an io.Reader rather than a
	// net.Conn: depend on the capability, not the concrete thing, and the
	// hard-to-produce condition becomes trivial to produce.
	now func() time.Time
}

// New returns an empty Store ready for concurrent use.
func New() *Store {
	return &Store{
		m:        make(map[string]entry),
		volatile: make(map[string]struct{}),
		now:      time.Now,
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
		s.remove(key)
		return "", false
	default:
		// Replaced with a fresh value during the gap. It is not ours to
		// delete, and the client asking now deserves the current value.
		return e.value, true
	}
}

// remove deletes a key from both the map and the volatile index. Callers must
// already hold the write lock.
//
// Both deletions always happen together: an entry in volatile with no entry in
// m would be a slot the sweeper wastes a sample on forever.
func (s *Store) remove(key string) {
	delete(s.m, key)
	delete(s.volatile, key)
}

// Len reports how many entries are resident, counting any that have expired
// but not yet been reclaimed.
//
// Counting the not-yet-reclaimed ones is what makes the sweeper observable
// from outside: the number falls on its own, without anybody reading a key.
// A count that hid them would be indistinguishable from lazy expiry.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

// TTL reports how long key has left before it expires.
//
// Three outcomes, because a client genuinely has to tell them apart:
//
//	exists == false     no such key (missing outright, or expired)
//	hasTTL == false     the key is there and never expires
//	otherwise           d is the time remaining
//
// Collapsing the first two would leave a caller unable to distinguish "this
// key is permanent" from "this key is gone", which is the same mistake as
// answering both a miss and a stored empty string with a null.
//
// Expired keys are reported gone but not deleted here. Reclaiming would mean
// taking the write lock on every TTL call; Get and the sweeper both free it
// soon enough.
func (s *Store) TTL(key string) (d time.Duration, hasTTL, exists bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.m[key]
	now := s.now()
	switch {
	case !ok || e.expired(now):
		return 0, false, false
	case e.expiresAt.IsZero():
		return 0, false, true
	default:
		return e.expiresAt.Sub(now), true, true
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
	// Keep the index honest in both directions. A plain SET over a key that
	// had a TTL must drop it out of volatile, or the sweeper keeps sampling a
	// key that can no longer expire.
	if e.expiresAt.IsZero() {
		delete(s.volatile, key)
	} else {
		s.volatile[key] = struct{}{}
	}
}

// Tuning for the sweeper. These mirror real Redis, which samples 20 keys per
// cycle and goes around again while more than a quarter turn out to be
// expired.
const (
	// sweepSample is how many volatile keys one round inspects. Small enough
	// that the write lock is held for microseconds.
	sweepSample = 20
	// sweepRepeatRatio is the expired fraction above which a round repeats
	// immediately instead of waiting for the next tick.
	sweepRepeatRatio = 0.25
	// sweepMaxRounds caps a single sweep. Without it, a keyspace that is
	// mostly expired would keep clearing the threshold and hold the lock for
	// an unbounded stretch -- the server-wide stall that active-only expiry
	// is supposed to avoid.
	sweepMaxRounds = 16
)

// SweepExpired removes expired keys that nobody has asked for, and reports how
// many it deleted. This is the active half of expiry; Get is the lazy half.
//
// Neither half is sufficient alone. Lazy-only leaks: a key written with a TTL
// and never read again is never noticed, so a session cache whose users do not
// return grows without bound. Active-only cannot afford to be exhaustive:
// scanning ten million keys means holding the write lock across all of them,
// stalling every client in the server.
//
// So this is deliberately probabilistic. It never looks at the whole keyspace,
// only a sample, and repeats while the sample keeps coming back mostly
// expired. That bounds the expired-but-resident population without ever
// promising to eliminate it -- affordable, rather than complete.
//
// The caller decides how often to run it; the Store owns no goroutines.
func (s *Store) SweepExpired() int {
	deleted := 0
	for range sweepMaxRounds {
		sampled, removed := s.sweepRound()
		deleted += removed
		// Stop when the sample comes back mostly live, or when there was
		// nothing volatile left to look at.
		if sampled == 0 || float64(removed)/float64(sampled) <= sweepRepeatRatio {
			break
		}
	}
	return deleted
}

// sweepRound inspects up to sweepSample volatile keys and deletes the expired
// ones, holding the write lock for exactly one round.
func (s *Store) sweepRound() (sampled, removed int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	// Go randomizes both the starting bucket and the offset within it on every
	// range, so successive rounds look at different keys. That is a randomized
	// start rather than a uniform random sample -- weaker than what Redis gets
	// from picking random dict entries, but it costs nothing and is fair
	// enough for a population estimate.
	for key := range s.volatile {
		if sampled == sweepSample {
			break
		}
		sampled++

		e, ok := s.m[key]
		if !ok {
			// Indexed but absent: nothing to reclaim, so drop the stale name
			// rather than sampling it again forever.
			delete(s.volatile, key)
			continue
		}
		if e.expired(now) {
			s.remove(key)
			removed++
		}
	}
	return sampled, removed
}
