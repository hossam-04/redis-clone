// Command redis-clone is a Redis-compatible server built from scratch to
// learn systems programming. Real redis-cli connects to it unmodified.
//
// This file does as little as possible on purpose: parse flags, bind the
// port, wire the pieces together. Everything interesting lives under
// internal/ -- resp for the wire protocol, store for the data, aof for
// persistence, server for the loop that joins them.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hossam-04/redis-clone/internal/aof"
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
	appendOnly := flag.Bool("appendonly", false,
		"persist state-changing commands to an append-only log and replay it on startup")
	appendFilename := flag.String("appendfilename", "redis-clone.aof",
		"path to the append-only log")
	appendFsync := flag.String("appendfsync", "everysec",
		"when to fsync the log: always, everysec, or no")
	flag.Parse()

	st := store.New(
		store.WithMaxMemory(*maxMemory),
		store.WithEvictSample(*evictSamples),
	)
	if *maxMemory > 0 {
		log.Printf("memory limit: %d estimated bytes, approximate LRU over %d samples",
			*maxMemory, *evictSamples)
	}

	var opts []server.Option
	var logFile *aof.Log
	if *appendOnly {
		var err error
		if logFile, err = startPersistence(st, *appendFilename, *appendFsync); err != nil {
			log.Fatal(err)
		}
		opts = append(opts, server.WithLog(logFile))
	}

	// net.Listen binds the port and has the kernel start queueing inbound
	// connections for us. It does not accept any of them yet.
	ln, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen on :%s: %v", *port, err)
	}
	defer ln.Close()
	log.Printf("listening on :%s", *port)

	go shutdownOnSignal(ln, logFile)
	go sweep(st, sweepInterval)

	log.Fatal(server.New(st, opts...).Serve(ln))
}

// startPersistence replays any existing log into st, then opens it for
// appending.
//
// The order matters. Replay uses a server with no log attached, so rebuilding
// the data does not append every recovered command straight back into the file
// it was just read from -- which would double the log on every restart.
func startPersistence(st *store.Store, path, fsync string) (*aof.Log, error) {
	policy, err := fsyncPolicy(fsync)
	if err != nil {
		return nil, err
	}

	res, err := aof.Replay(path, server.New(st).Apply)
	if err != nil {
		// Refuse to start rather than serve data we know is incomplete.
		return nil, fmt.Errorf("replay %s: %w", path, err)
	}
	if res.Truncated > 0 {
		log.Printf("append-only log ended mid-command: discarded %d trailing bytes "+
			"(the previous run was killed during a write)", res.Truncated)
	}
	log.Printf("replayed %d commands from %s", res.Commands, path)

	l, err := aof.Open(path, policy)
	if err != nil {
		return nil, err
	}
	log.Printf("persistence on: %s, fsync %s", path, strings.ToLower(fsync))
	return l, nil
}

func fsyncPolicy(name string) (aof.Policy, error) {
	switch strings.ToLower(name) {
	case "always":
		return aof.Always, nil
	case "everysec":
		return aof.EverySecond, nil
	case "no":
		return aof.Never, nil
	}
	return 0, fmt.Errorf("unknown -appendfsync %q: want always, everysec, or no", name)
}

// shutdownOnSignal closes the log cleanly on Ctrl-C or SIGTERM.
//
// Without this, a deliberate shutdown would lose whatever was still buffered
// or unsynced -- and a shutdown the operator asked for is the one case where
// losing data is inexcusable. Close flushes and fsyncs whatever the policy is,
// because there is no throughput left to protect on the way out.
//
// Note this does nothing for kill -9, which is the point of the log rather
// than a gap in it: SIGKILL cannot be handled, and surviving it is exactly
// what flushing on every batch already guarantees.
func shutdownOnSignal(ln net.Listener, l *aof.Log) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	log.Print("shutting down")
	if l != nil {
		if err := l.Close(); err != nil {
			log.Printf("closing append-only log: %v", err)
		}
	}
	ln.Close()
	os.Exit(0)
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
