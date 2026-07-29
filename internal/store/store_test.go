package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock moves time forward on demand instead of by sleeping.
//
// Sleeping in tests is slow and flaky: a 10ms TTL test either wastes 10ms or
// fails on a loaded CI box. Controlling the clock makes "an hour passed" cost
// nothing and be exactly true. Same reasoning as the parser taking an
// io.Reader -- depend on the capability, and the awkward condition becomes
// trivial to produce.
//
// The mutex is needed because the store calls Now from every client goroutine.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestStore returns a Store whose sense of time the test controls.
func newTestStore() (*Store, *fakeClock) {
	c := newFakeClock()
	s := New()
	s.now = c.Now
	return s, c
}

// size reports how many entries are physically present, expired or not. Tests
// use it to tell "hidden from readers" apart from "actually reclaimed".
func (s *Store) size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

func TestGetMissingKey(t *testing.T) {
	s := New()
	v, ok := s.Get("nothing")
	if ok {
		t.Errorf("ok = true for a key never set, want false")
	}
	if v != "" {
		t.Errorf("value = %q for a missing key, want empty", v)
	}
}

// TestEmptyValueIsNotMissing is the storage-side half of the null/empty
// distinction. Get must report ok=true for a key explicitly set to "",
// otherwise the server has no way to tell a stored empty value from a miss
// and would answer both with a null.
func TestEmptyValueIsNotMissing(t *testing.T) {
	s := New()
	s.Set("empty", "")

	v, ok := s.Get("empty")
	if !ok {
		t.Fatal("ok = false for a key set to the empty string, want true")
	}
	if v != "" {
		t.Errorf("value = %q, want empty string", v)
	}
}

func TestSetOverwrites(t *testing.T) {
	s := New()
	s.Set("k", "first")
	s.Set("k", "second")

	if v, _ := s.Get("k"); v != "second" {
		t.Errorf("value = %q after overwrite, want %q", v, "second")
	}
}

func TestValuesAreBinarySafe(t *testing.T) {
	s := New()
	// Nothing in the store inspects value bytes, so protocol delimiters and
	// NUL bytes are ordinary data here.
	want := "a\r\nb\x00c"
	s.Set("k", want)

	if got, _ := s.Get("k"); got != want {
		t.Errorf("value = %q, want %q", got, want)
	}
}

func TestTTLAliveBeforeDeadline(t *testing.T) {
	s, clock := newTestStore()
	s.SetWithTTL("k", "v", 10*time.Second)

	clock.Advance(9 * time.Second)
	if v, ok := s.Get("k"); !ok || v != "v" {
		t.Errorf("Get = %q, %v after 9s of a 10s TTL; want %q, true", v, ok, "v")
	}
}

// TestTTLAliveExactlyAtDeadline pins the boundary. Expiry triggers strictly
// after the deadline, so at the instant it is reached the key is still live.
// Untested boundaries are where off-by-ones live.
func TestTTLAliveExactlyAtDeadline(t *testing.T) {
	s, clock := newTestStore()
	s.SetWithTTL("k", "v", 10*time.Second)

	clock.Advance(10 * time.Second)
	if _, ok := s.Get("k"); !ok {
		t.Error("key expired at exactly its deadline; expiry should be strictly after")
	}
}

func TestTTLGoneAfterDeadline(t *testing.T) {
	s, clock := newTestStore()
	s.SetWithTTL("k", "v", 10*time.Second)

	clock.Advance(10*time.Second + time.Nanosecond)
	if v, ok := s.Get("k"); ok {
		t.Errorf("Get = %q, true past the TTL; want a miss", v)
	}
}

// TestExpiredKeyIsReclaimedNotHidden is the point of lazy expiry. Reporting a
// miss is not enough -- if the entry stayed in the map, memory would grow
// forever with keys that are invisible but still resident.
func TestExpiredKeyIsReclaimedNotHidden(t *testing.T) {
	s, clock := newTestStore()
	s.SetWithTTL("k", "v", time.Second)
	clock.Advance(2 * time.Second)

	if s.size() != 1 {
		t.Fatalf("size = %d before the expired key is touched, want 1", s.size())
	}
	s.Get("k")
	if got := s.size(); got != 0 {
		t.Errorf("size = %d after reading an expired key, want 0 (memory not reclaimed)", got)
	}
}

// TestSetClearsExistingTTL matches Redis: a plain SET replaces the whole
// entry, TTL included, so the key stops being volatile.
func TestSetClearsExistingTTL(t *testing.T) {
	s, clock := newTestStore()
	s.SetWithTTL("k", "old", time.Second)
	s.Set("k", "new")

	clock.Advance(time.Hour)
	if v, ok := s.Get("k"); !ok || v != "new" {
		t.Errorf("Get = %q, %v an hour after a plain SET; want %q, true", v, ok, "new")
	}
}

func TestSetWithTTLReplacesExistingTTL(t *testing.T) {
	s, clock := newTestStore()
	s.SetWithTTL("k", "v", time.Hour)
	s.SetWithTTL("k", "v", time.Second)

	clock.Advance(2 * time.Second)
	if _, ok := s.Get("k"); ok {
		t.Error("key survived its new, shorter TTL; the second SetWithTTL did not replace the first")
	}
}

// TestConcurrentAccess is meaningful only under -race, where it proves the
// RWMutex actually guards the map. Without the lock, concurrent writes do not
// corrupt quietly -- the Go runtime kills the process outright.
func TestConcurrentAccess(t *testing.T) {
	s := New()
	const goroutines, ops = 16, 200

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range ops {
				key := fmt.Sprintf("key:%d:%d", g, i)
				s.Set(key, fmt.Sprintf("value:%d", i))
				s.Get(key)
				s.Get("contended") // every goroutine reads one shared key
			}
		}(g)
	}
	wg.Wait()

	// Spot-check that writes from a few different goroutines survived.
	for _, g := range []int{0, 7, 15} {
		key := fmt.Sprintf("key:%d:%d", g, ops-1)
		if v, ok := s.Get(key); !ok || v != fmt.Sprintf("value:%d", ops-1) {
			t.Errorf("Get(%q) = %q, %v; want the last write from goroutine %d", key, v, ok, g)
		}
	}
}

// TestConcurrentExpiredReads exercises the read-lock-to-write-lock gap in Get.
// Every goroutine reads the same expired key at once, so they all race to
// delete it and all but one find it already gone. Under -race this is what
// would catch acting on state read before the gap.
func TestConcurrentExpiredReads(t *testing.T) {
	s, clock := newTestStore()
	for i := range 50 {
		s.SetWithTTL(fmt.Sprintf("k%d", i), "v", time.Second)
	}
	clock.Advance(2 * time.Second)

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				if _, ok := s.Get(fmt.Sprintf("k%d", i)); ok {
					t.Errorf("k%d readable after expiry", i)
				}
			}
		}()
	}
	wg.Wait()

	if got := s.size(); got != 0 {
		t.Errorf("size = %d after all expired keys were read, want 0", got)
	}
}

// TestConcurrentExpiryAndOverwrite covers the nastiest case in Get: a key
// expires at the same moment another client overwrites it. The overwritten
// value must never be deleted by the expiry path, because by then it is a
// live value that simply shares a name with an expired one.
func TestConcurrentExpiryAndOverwrite(t *testing.T) {
	s, clock := newTestStore()

	var wg sync.WaitGroup
	for i := range 200 {
		key := fmt.Sprintf("k%d", i)
		s.SetWithTTL(key, "old", time.Second)
		clock.Advance(2 * time.Second)

		wg.Add(2)
		go func() { defer wg.Done(); s.Get(key) }()        // sees it expired, wants to delete
		go func() { defer wg.Done(); s.Set(key, "new") }() // replaces it with a live value
		wg.Wait()

		// Whoever won the race, a key holding "new" must never be deleted.
		if v, ok := s.Get(key); ok && v != "new" {
			t.Fatalf("key %s = %q after overwrite, want %q", key, v, "new")
		}
	}
}
