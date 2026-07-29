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

	if s.Len() != 1 {
		t.Fatalf("size = %d before the expired key is touched, want 1", s.Len())
	}
	s.Get("k")
	if got := s.Len(); got != 0 {
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

	if got := s.Len(); got != 0 {
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

// volatileSize reports how many keys the sweeper considers worth sampling.
func (s *Store) volatileSize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.volatile)
}

// TestSweepReclaimsKeysNobodyReads is the whole reason active expiry exists.
// These keys are never passed to Get, so lazy expiry would never notice them
// and they would occupy memory forever.
func TestSweepReclaimsKeysNobodyReads(t *testing.T) {
	s, clock := newTestStore()
	for i := range 10 {
		s.SetWithTTL(fmt.Sprintf("k%d", i), "v", time.Second)
	}
	clock.Advance(2 * time.Second)

	if got := s.SweepExpired(); got != 10 {
		t.Errorf("swept %d keys, want 10", got)
	}
	if got := s.Len(); got != 0 {
		t.Errorf("size = %d after sweep, want 0", got)
	}
	if got := s.volatileSize(); got != 0 {
		t.Errorf("volatile index size = %d after sweep, want 0", got)
	}
}

func TestSweepLeavesLiveKeys(t *testing.T) {
	s, clock := newTestStore()
	s.SetWithTTL("soon", "v", time.Second)
	s.SetWithTTL("later", "v", time.Hour)

	clock.Advance(2 * time.Second)
	s.SweepExpired()

	if _, ok := s.Get("later"); !ok {
		t.Error("a key well inside its TTL was swept")
	}
	if _, ok := s.Get("soon"); ok {
		t.Error("an expired key survived the sweep")
	}
}

// TestSweepIgnoresKeysWithoutTTL is what the volatile index buys. A key with
// no deadline must never be sampled at all, however long it sits there.
func TestSweepIgnoresKeysWithoutTTL(t *testing.T) {
	s, clock := newTestStore()
	for i := range 100 {
		s.Set(fmt.Sprintf("permanent%d", i), "v")
	}
	if got := s.volatileSize(); got != 0 {
		t.Fatalf("volatile index size = %d with no TTLs set, want 0", got)
	}

	clock.Advance(365 * 24 * time.Hour)
	if got := s.SweepExpired(); got != 0 {
		t.Errorf("swept %d keys that have no TTL, want 0", got)
	}
	if got := s.Len(); got != 100 {
		t.Errorf("size = %d, want 100 -- permanent keys were deleted", got)
	}
}

// TestPlainSetLeavesVolatileIndex covers the bookkeeping half of Redis's rule
// that a plain SET clears an existing TTL. If the name stayed in the index,
// the sweeper would sample a key that can no longer expire, forever.
func TestPlainSetLeavesVolatileIndex(t *testing.T) {
	s, _ := newTestStore()
	s.SetWithTTL("k", "v", time.Hour)
	if got := s.volatileSize(); got != 1 {
		t.Fatalf("volatile index size = %d after SetWithTTL, want 1", got)
	}

	s.Set("k", "v")
	if got := s.volatileSize(); got != 0 {
		t.Errorf("volatile index size = %d after a plain Set, want 0", got)
	}
}

// TestSweepRoundIsBounded is the guarantee that keeps the write lock short.
// One round must never inspect more than sweepSample keys no matter how many
// are expired.
func TestSweepRoundIsBounded(t *testing.T) {
	s, clock := newTestStore()
	for i := range 500 {
		s.SetWithTTL(fmt.Sprintf("k%d", i), "v", time.Second)
	}
	clock.Advance(2 * time.Second)

	sampled, removed := s.sweepRound()
	if sampled != sweepSample {
		t.Errorf("one round sampled %d keys, want exactly %d", sampled, sweepSample)
	}
	if removed != sweepSample {
		t.Errorf("one round removed %d of %d sampled expired keys", removed, sampled)
	}
}

// TestSweepRepeatsWhileMostlyExpired shows the adaptive half: a keyspace full
// of expired keys is cleaned harder than one round's worth, without anyone
// asking for a bigger sample.
func TestSweepRepeatsWhileMostlyExpired(t *testing.T) {
	s, clock := newTestStore()
	for i := range 200 {
		s.SetWithTTL(fmt.Sprintf("k%d", i), "v", time.Second)
	}
	clock.Advance(2 * time.Second)

	deleted := s.SweepExpired()
	if deleted <= sweepSample {
		t.Errorf("swept %d keys in one call; expected repeats to exceed one round of %d",
			deleted, sweepSample)
	}
}

// TestSweepStopsAtMaxRounds is the other side of the same coin: however
// expired the keyspace is, one call must return rather than holding the lock
// indefinitely. That cap is what stops active expiry from causing the
// server-wide stall it exists to prevent.
func TestSweepStopsAtMaxRounds(t *testing.T) {
	s, clock := newTestStore()
	const keys = 2000
	for i := range keys {
		s.SetWithTTL(fmt.Sprintf("k%d", i), "v", time.Second)
	}
	clock.Advance(2 * time.Second)

	deleted := s.SweepExpired()
	if max := sweepSample * sweepMaxRounds; deleted > max {
		t.Errorf("swept %d keys in one call, want at most %d", deleted, max)
	}
	if deleted == keys {
		t.Error("one call cleared the entire keyspace; the round cap is not bounding anything")
	}
}

// TestSweepEventuallyClearsEverything checks that the bound above costs
// completeness only per call, not overall -- repeated ticks still converge.
func TestSweepEventuallyClearsEverything(t *testing.T) {
	s, clock := newTestStore()
	for i := range 300 {
		s.SetWithTTL(fmt.Sprintf("k%d", i), "v", time.Second)
	}
	clock.Advance(2 * time.Second)

	for tick := 0; tick < 100 && s.Len() > 0; tick++ {
		s.SweepExpired()
	}
	if got := s.Len(); got != 0 {
		t.Errorf("size = %d after 100 sweep ticks, want 0", got)
	}
}

// TestSweepConcurrentWithClients runs the sweeper against live traffic, which
// is how it will actually run. Under -race this is what would catch the
// sweeper touching the map outside the lock.
func TestSweepConcurrentWithClients(t *testing.T) {
	s, clock := newTestStore()

	// The sweeper gets its own stop channel rather than joining the client
	// WaitGroup: it runs until told to stop, so waiting on it alongside the
	// clients would wait forever.
	stop := make(chan struct{})
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		for {
			select {
			case <-stop:
				return
			default:
				s.SweepExpired()
			}
		}
	}()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 300 {
				key := fmt.Sprintf("k%d:%d", g, i)
				s.SetWithTTL(key, "v", time.Second)
				s.Get(key)
				s.Set(key, "permanent")
				clock.Advance(time.Millisecond)
			}
		}(g)
	}
	wg.Wait()

	close(stop)
	<-swept
}

// entryCostOfTestKey is the cost of the "kNNN"/"value" pairs the eviction
// tests write, so limits can be expressed in whole entries.
var entryCostOfTestKey = entryCost("k000", "value")

func TestNoEvictionWithoutALimit(t *testing.T) {
	s := New()
	for i := range 1000 {
		s.Set(fmt.Sprintf("k%d", i), "value")
	}
	if got := s.Len(); got != 1000 {
		t.Errorf("Len = %d with no limit configured, want 1000", got)
	}
}

func TestUsedMemoryTracksWrites(t *testing.T) {
	s := New()
	if got := s.UsedMemory(); got != 0 {
		t.Fatalf("UsedMemory = %d on an empty store, want 0", got)
	}

	s.Set("k", "value")
	want := entryCost("k", "value")
	if got := s.UsedMemory(); got != want {
		t.Errorf("UsedMemory = %d after one Set, want %d", got, want)
	}

	// An overwrite must replace the old cost, not add to it.
	s.Set("k", "a much longer value than before")
	want = entryCost("k", "a much longer value than before")
	if got := s.UsedMemory(); got != want {
		t.Errorf("UsedMemory = %d after overwrite, want %d (old bytes not released?)", got, want)
	}
}

func TestUsedMemoryReleasedOnExpiry(t *testing.T) {
	s, clock := newTestStore()
	s.SetWithTTL("k", "v", time.Second)
	clock.Advance(2 * time.Second)

	s.SweepExpired()
	if got := s.UsedMemory(); got != 0 {
		t.Errorf("UsedMemory = %d after the entry was swept, want 0", got)
	}
}

func TestEvictionKeepsStoreUnderLimit(t *testing.T) {
	limit := 10 * entryCostOfTestKey
	s := New(WithMaxMemory(limit))

	for i := range 500 {
		s.Set(fmt.Sprintf("k%03d", i), "value")
	}
	if got := s.UsedMemory(); got > limit {
		t.Errorf("UsedMemory = %d after 500 writes, want <= %d", got, limit)
	}
	if got := s.Len(); got > 10 {
		t.Errorf("Len = %d, want at most 10 given the limit", got)
	}
}

// TestEvictionPrefersLeastRecentlyUsed is deterministic despite sampling:
// there are only four keys and the sample size is five, so every key is
// examined and the choice is exact.
func TestEvictionPrefersLeastRecentlyUsed(t *testing.T) {
	// Three entries fit exactly; the fourth forces one eviction.
	limit := 3 * entryCost("a", "x")
	s := New(WithMaxMemory(limit))

	s.Set("a", "x")
	s.Set("b", "x")
	s.Set("c", "x")

	// Touch a and b, leaving c as the least recently used.
	s.Get("a")
	s.Get("b")

	s.Set("d", "x")

	if _, ok := s.Get("c"); ok {
		t.Error("c was the least recently used key but survived eviction")
	}
	for _, key := range []string{"a", "b", "d"} {
		if _, ok := s.Get(key); !ok {
			t.Errorf("%s was recently used but was evicted", key)
		}
	}
}

// TestWriteLargerThanLimitIsKept covers the degenerate case. Accepting a write
// and then immediately evicting it would make SET report OK while the very
// next GET returned nil.
func TestWriteLargerThanLimitIsKept(t *testing.T) {
	s := New(WithMaxMemory(10)) // smaller than a single entry's overhead
	s.Set("k", "value")

	if v, ok := s.Get("k"); !ok || v != "value" {
		t.Errorf("Get = %q, %v; a value larger than the whole limit must still be stored", v, ok)
	}
	if got := s.UsedMemory(); got <= 10 {
		t.Errorf("UsedMemory = %d, expected the store to sit over its limit here", got)
	}
}

// TestEvictionUnderConcurrency runs eviction against live readers and writers.
// Under -race this is what would catch atime being read during eviction while
// a reader stamps it.
func TestEvictionUnderConcurrency(t *testing.T) {
	s := New(WithMaxMemory(50 * entryCostOfTestKey))

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 400 {
				key := fmt.Sprintf("k%d:%03d", g, i)
				s.Set(key, "value")
				s.Get(key)
				s.Get(fmt.Sprintf("k%d:%03d", g, i/2)) // re-read something older
			}
		}(g)
	}
	wg.Wait()

	if got, limit := s.UsedMemory(), int64(50*entryCostOfTestKey); got > limit {
		t.Errorf("UsedMemory = %d after concurrent load, want <= %d", got, limit)
	}
}

func TestWithEvictSampleRejectsNonsense(t *testing.T) {
	// A sample size below 1 would make evictOne examine nothing and never
	// find a victim, so the limit would silently stop being enforced.
	for _, n := range []int{0, -1} {
		s := New(WithEvictSample(n))
		if s.evictSample != defaultEvictSample {
			t.Errorf("WithEvictSample(%d) set sample to %d, want the default %d",
				n, s.evictSample, defaultEvictSample)
		}
	}
}

// TestLimitHoldsAtAnySampleSize checks the property that must not depend on
// sample size. Accuracy degrades as the sample shrinks -- a sample of 1 is
// random eviction -- but the memory ceiling is not an approximation and has
// to hold regardless.
func TestLimitHoldsAtAnySampleSize(t *testing.T) {
	for _, n := range []int{1, 2, 5, 50} {
		t.Run(fmt.Sprintf("sample=%d", n), func(t *testing.T) {
			limit := 20 * entryCostOfTestKey
			s := New(WithMaxMemory(limit), WithEvictSample(n))

			for i := range 500 {
				s.Set(fmt.Sprintf("k%03d", i), "value")
			}
			if got := s.UsedMemory(); got > limit {
				t.Errorf("UsedMemory = %d, want <= %d", got, limit)
			}
		})
	}
}
