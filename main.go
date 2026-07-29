package main

import (
	"bufio"
	"errors"
	"flag"
	"log"
	"net"
)

func main() {
	port := flag.String("port", "6379", "TCP port to listen on")
	flag.Parse()

	// net.Listen binds the port and starts the kernel queueing inbound
	// connections for us. It does not accept any of them yet.
	ln, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen on :%s: %v", *port, err)
	}
	defer ln.Close()
	log.Printf("listening on :%s", *port)

	// One store, shared by every connection -- that sharing is the entire
	// point of a key-value server, and the reason Store carries a mutex.
	store := NewStore()

	for {
		// Accept blocks until a client connects, then returns a conn
		// representing that one client. The listener stays open.
		conn, err := ln.Accept()
		if err != nil {
			// One failed accept should not kill the server -- log and
			// keep serving everyone else.
			log.Printf("accept: %v", err)
			continue
		}
		// One goroutine per client. This is the whole concurrency model
		// (ADR-001): handleConn may block freely without stalling others.
		go handleConn(conn, store)
	}
}

// handleConn serves one client until it disconnects or breaks the protocol.
func handleConn(conn net.Conn, store *Store) {
	defer conn.Close()

	// Both buffers exist to amortise syscalls (ADR-004). The reader also
	// carries any leftover bytes of a partially-arrived command between
	// iterations of this loop.
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	for {
		cmd, err := ReadCommand(r)
		if err != nil {
			// A protocol error means framing is broken, so every byte after
			// it is untrustworthy and no recovery point exists -- say so
			// once, then hang up. An I/O error means the client is already
			// gone and there is nobody left to tell.
			if errors.Is(err, ErrProtocol) {
				_ = writeError(w, "ERR "+err.Error())
				_ = w.Flush()
			}
			return
		}

		if err := dispatch(w, store, cmd); err != nil {
			return
		}

		// Flush only when nothing is left to process. While more commands sit
		// in the read buffer we keep batching replies, so a pipelined burst
		// costs one write syscall instead of one per command. An empty buffer
		// means the next read would block -- and blocking without flushing
		// first deadlocks a client waiting on a reply still sitting in w.
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}
