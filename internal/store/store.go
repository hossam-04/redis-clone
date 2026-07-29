// Package store holds the key-value data every client shares.
//
// It knows nothing about RESP, connections, or command names. Keeping it
// ignorant of the protocol is what will let milestone 3 add persistence and
// milestone 2 add expiry without either one having to care how a client
// phrased the request.
package store

import (
	"sync"
	"sync/atomic"
	"time"
)

// entry is one stored value plus its metadata.
//
// Entries live in the map as pointers, and are never copied. That is not a
// style choice: atime below cannot be copied, and more importantly the
// pointer is what lets a reader update an entry without writing to the map.
// See Get.
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

	// atime is the value of the store's clock when this entry was last read
	// or written. Eviction prefers the smallest.
	//
	// Atomic because it is written by readers holding only the read lock,
	// while eviction reads it under the write lock. Any non-atomic field
	// would be a data race the moment a GET overlapped an eviction.
	atime atomic.Uint64
}

// expired reports whether e has an expiry that has already passed.
//
// Pointer receiver: a value receiver would copy the struct, and copying an
// atomic is both meaningless and a vet error.
func (e *entry) expired(now time.Time) bool {
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
	m  map[string]*entry

	// clock is a counter bumped on every access, and stamped onto the entry
	// that was touched. It is a logical clock, not a wall clock -- eviction
	// only ever compares two stamps, so their ordering is all that matters
	// and no real time is involved.
	//
	// Atomic so readers can bump it without the write lock. Note this is a
	// single contended cache line: at very high throughput every core is
	// fighting over it. Redis sidesteps that with a coarse clock updated
	// periodically rather than per access. Worth measuring at milestone 4
	// before deciding it matters.
	clock atomic.Uint64

	// volatile indexes exactly the keys that carry a deadline. The sweeper
	// samples from here rather than from m.
	//
	// Without it, sampling is useless at realistic ratios: a cache with a
	// million keys of which ten thousand have TTLs would spend roughly 199
	// samples in 200 looking at keys that can never expire. The deadline
	// itself stays in the entry so Get still needs only one lookup -- this
	// holds no timestamps, only names.
	volatile map[string]struct{}

	// used is the estimated bytes held; maxBytes is the ceiling above which
	// writes evict. Zero maxBytes means no limit. Both are guarded by mu
	// rather than atomic, since every path that changes them already holds
	// the write lock.
	used     int64
	maxBytes int64

	// evictSample is how many keys each eviction examines. Larger is a better
	// approximation of true LRU and costs proportionally more time under the
	// write lock, which is held during eviction.
	evictSample int

	// now is swappable so tests can control the passage of time instead of
	// sleeping. Same idea as the parser taking an io.Reader rather than a
	// net.Conn: depend on the capability, not the concrete thing, and the
	// hard-to-produce condition becomes trivial to produce.
	now func() time.Time
}

// Option configures a Store.
type Option func(*Store)

// WithMaxMemory caps the store's estimated size. Once a write pushes it over,
// approximately-least-recently-used keys are evicted until it fits again.
// Zero, the default, means no limit and no eviction.
func WithMaxMemory(bytes int64) Option {
	return func(s *Store) { s.maxBytes = bytes }
}

// WithEvictSample sets how many keys each eviction examines before choosing a
// victim. Higher values approximate true LRU more closely at proportionally
// more time under the write lock. Values below 1 are ignored.
func WithEvictSample(n int) Option {
	return func(s *Store) {
		if n >= 1 {
			s.evictSample = n
		}
	}
}

// New returns an empty Store ready for concurrent use.
func New(opts ...Option) *Store {
	s := &Store{
		m:           make(map[string]*entry),
		volatile:    make(map[string]struct{}),
		now:         time.Now,
		evictSample: defaultEvictSample,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// entryOverhead stands in for everything an entry costs beyond its key and
// value bytes: the map bucket slot, two string headers, the *entry pointer,
// the entry struct, and allocator rounding.
//
// Memory accounting here is deliberately an estimate. Go offers no cheap way
// to ask what a map entry really occupies -- bucket layout, allocator size
// classes and GC timing all contribute, and reading the truth means
// runtime.ReadMemStats, which stops the world and so cannot run on every SET.
//
// Real RSS will therefore be higher than this number. That is fine: eviction
// needs a figure that moves in proportion to actual usage, not an accurate
// one. A limit that ignored value size, by contrast, would not measure the
// thing eviction exists to control at all.
const entryOverhead = 96

func entryCost(key, value string) int64 {
	return int64(len(key) + len(value) + entryOverhead)
}

// defaultEvictSample is how many keys a single eviction looks at before
// picking a victim. Redis defaults to 5 too, and exposes it as
// maxmemory-samples for the same reason WithEvictSample exists: it is the
// dial between eviction accuracy and eviction cost.
const defaultEvictSample = 5

// UsedMemory reports the estimated bytes currently held.
func (s *Store) UsedMemory() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.used
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
		// Stamp the access so eviction knows this key is recently used.
		//
		// This is a write, on the read-lock path -- and it is safe because it
		// is not a write to the *map*. The map still holds the same pointer
		// to the same entry; only a field inside that entry changes, and it
		// changes atomically. Mutating the contents is not mutating the
		// container.
		//
		// With map[string]entry this would be impossible: updating a field
		// would mean assigning the slot back, which is a map write, which
		// needs the write lock, which would serialise every read in the
		// server and cost us the RWMutex entirely.
		e.atime.Store(s.clock.Add(1))
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
	if e, ok := s.m[key]; ok {
		s.used -= entryCost(key, e.value)
	}
	delete(s.m, key)
	delete(s.volatile, key)
}

// evictToLimit removes keys until the store fits under maxBytes, never
// touching keep -- the key whose write triggered this. Callers must hold the
// write lock.
//
// Protecting keep matters for the degenerate case of a value larger than the
// whole limit. Without it, the write would be accepted and then immediately
// evicted as the only candidate, so SET would report OK and the following GET
// would return nil. Silently discarding what a client just stored is worse
// than sitting over the limit, so the store stays over instead.
func (s *Store) evictToLimit(keep string) {
	if s.maxBytes <= 0 {
		return
	}
	for s.used > s.maxBytes {
		if !s.evictOne(keep) {
			return // nothing left that may be evicted
		}
	}
}

// evictOne removes the least recently used key among a small random sample and
// reports whether anything went. Callers must hold the write lock.
//
// Sampling rather than a true LRU order, for two reasons that compound.
//
// Finding the genuine minimum means scanning every key, and eviction fires
// under memory pressure -- precisely when the server can least afford a full
// scan holding the write lock.
//
// Keeping an exact order instead, via the textbook hash-map-plus-linked-list,
// would mean moving a node on every read. That is a mutation of shared
// structure, so every GET would need the write lock, and reads across the
// whole server would serialise. LRU would cost all the read concurrency the
// RWMutex exists to provide.
//
// So this gives up exactness and buys both back. The victim is the oldest of
// five, which is usually old but is not guaranteed to be the oldest overall.
func (s *Store) evictOne(keep string) bool {
	var (
		victim string
		oldest uint64
		found  bool
	)
	sampled := 0
	// Go's randomized range start supplies the sample, same as in sweepRound.
	for key, e := range s.m {
		if sampled == s.evictSample {
			break
		}
		if key == keep {
			continue
		}
		sampled++
		if a := e.atime.Load(); !found || a < oldest {
			victim, oldest, found = key, a, true
		}
	}
	if !found {
		return false
	}
	s.remove(victim)
	return true
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
	s.put(key, &entry{value: value})
}

// SetWithTTL stores value under key and expires it after ttl.
func (s *Store) SetWithTTL(key, value string, ttl time.Duration) {
	s.put(key, &entry{value: value, expiresAt: s.now().Add(ttl)})
}

func (s *Store) put(key string, e *entry) {
	// A write counts as an access, so a freshly written key is the most
	// recently used one and will not be the next thing evicted.
	e.atime.Store(s.clock.Add(1))

	s.mu.Lock()
	defer s.mu.Unlock()

	// An overwrite replaces the old value, so its bytes stop counting.
	if old, ok := s.m[key]; ok {
		s.used -= entryCost(key, old.value)
	}
	s.m[key] = e
	s.used += entryCost(key, e.value)

	// Keep the index honest in both directions. A plain SET over a key that
	// had a TTL must drop it out of volatile, or the sweeper keeps sampling a
	// key that can no longer expire.
	if e.expiresAt.IsZero() {
		delete(s.volatile, key)
	} else {
		s.volatile[key] = struct{}{}
	}

	// Evict only on write. Growth is the only thing that breaches the limit,
	// so this is the one place that needs to check.
	s.evictToLimit(key)
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
