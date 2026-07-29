// Command redis-clone is a Redis-compatible server built from scratch to
// learn systems programming. Real redis-cli connects to it unmodified.
//
// This file does as little as possible on purpose: parse flags, bind the
// port, wire the pieces together. Everything interesting lives under
// internal/ -- resp for the wire protocol, store for the data, server for
// the loop that joins them.
package main

import (
	"flag"
	"log"
	"net"
	"time"

	"github.com/hossam-04/redis-clone/internal/server"
	"github.com/hossam-04/redis-clone/internal/store"
)

// sweepInterval is how often expired keys nobody has read are swept. Ten
// times a second, matching Redis. The Store owns no goroutines of its own, so
// choosing the schedule is the caller's job -- which is also what keeps it
// testable without one running.
const sweepInterval = 100 * time.Millisecond

func main() {
	port := flag.String("port", "6379", "TCP port to listen on")
	maxMemory := flag.Int64("maxmemory", 0,
		"evict approximately-least-recently-used keys above this many estimated bytes (0 = unlimited)")
	evictSamples := flag.Int("maxmemory-samples", 5,
		"keys examined per eviction; higher approximates true LRU more closely at more cost")
	flag.Parse()

	// net.Listen binds the port and has the kernel start queueing inbound
	// connections for us. It does not accept any of them yet.
	ln, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen on :%s: %v", *port, err)
	}
	defer ln.Close()
	log.Printf("listening on :%s", *port)

	st := store.New(
		store.WithMaxMemory(*maxMemory),
		store.WithEvictSample(*evictSamples),
	)
	if *maxMemory > 0 {
		log.Printf("memory limit: %d estimated bytes, approximate LRU over %d samples",
			*maxMemory, *evictSamples)
	}
	go sweep(st, sweepInterval)

	log.Fatal(server.New(st).Serve(ln))
}

// sweep runs active expiry forever. Each call is internally bounded, so this
// never holds the store's write lock for long no matter how much has expired.
func sweep(st *store.Store, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for range tick.C {
		st.SweepExpired()
	}
}
