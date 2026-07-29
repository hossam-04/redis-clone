// Package store holds the key-value data every client shares.
//
// It knows nothing about RESP, connections, or command names. Keeping it
// ignorant of the protocol is what will let milestone 3 add persistence and
// milestone 2 add expiry without either one having to care how a client
// phrased the request.
package store

import "sync"

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
	m  map[string]string
}

// New returns an empty Store ready for concurrent use.
func New() *Store {
	return &Store{m: make(map[string]string)}
}

// Get reports the value and whether the key existed at all. That second
// return is what lets the caller distinguish a missing key from a key holding
// the empty string -- the distinction RESP's null bulk string exists for.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	return v, ok
}

// Set stores value under key, replacing any previous value.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
}
