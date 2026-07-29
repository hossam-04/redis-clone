package store

import (
	"fmt"
	"sync"
	"testing"
)

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
