// Package server joins the protocol to the data.
//
// It is the only package that imports both resp and store, and the dependency
// runs one way: resp and store do not know this package exists, and do not
// know about each other. That is what keeps the parser testable without a
// server and the store testable without a socket.
package server

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"

	"github.com/hossam-04/redis-clone/internal/aof"
	"github.com/hossam-04/redis-clone/internal/resp"
	"github.com/hossam-04/redis-clone/internal/store"
)

// Server serves RESP clients from one shared Store.
type Server struct {
	store *store.Store

	// log is nil when persistence is off, which is the only difference
	// between a durable server and an in-memory one.
	log *aof.Log

	// replaySink absorbs replies during replay, where there is no client to
	// send them to.
	replaySink *bufio.Writer
}

// Option configures a Server.
type Option func(*Server)

// WithLog makes the server append state-changing commands to l.
func WithLog(l *aof.Log) Option {
	return func(s *Server) { s.log = l }
}

// New returns a Server backed by st. Every connection it accepts shares that
// single store -- the sharing is the entire point of a key-value server, and
// the reason Store carries a mutex.
func New(st *store.Store, opts ...Option) *Server {
	s := &Server{
		store:      st,
		replaySink: bufio.NewWriter(io.Discard),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Apply executes cmd during log replay, discarding whatever it would reply.
//
// Reusing dispatch is the point: a replayed command takes exactly the same
// path a client command takes, so there is no second execution path to keep in
// step with the first. The server used for replay has no log attached, so
// rebuilding the data does not append every command straight back into the
// file it came from.
func (s *Server) Apply(cmd resp.Command) error {
	return s.dispatch(s.replaySink, cmd)
}

// Serve accepts connections on ln until the listener is closed, handing each
// client its own goroutine. It returns only when ln stops working.
func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			// A closed listener is a shutdown, not a failure -- stop.
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			// Anything else: one client failing to connect is no reason to
			// drop everyone else, so log and keep serving.
			//
			// Known soft spot: if this is EMFILE (out of file descriptors)
			// the next Accept fails instantly too, and this becomes a hot
			// loop pinning a core. A backoff belongs here.
			log.Printf("accept: %v", err)
			continue
		}
		// One goroutine per client. This is the whole concurrency model
		// (ADR-001): handleConn may block freely without stalling others.
		go s.handleConn(conn)
	}
}

// handleConn serves one client until it disconnects or breaks the protocol.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	// Both buffers exist to amortise syscalls (ADR-004). The reader also
	// carries any leftover bytes of a partially-arrived command between
	// iterations of this loop.
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	for {
		cmd, err := resp.ReadCommand(r)
		if err != nil {
			// A protocol error means framing is broken, so every byte after
			// it is untrustworthy and no recovery point exists -- say so
			// once, then hang up. An I/O error means the client is already
			// gone and there is nobody left to tell.
			if errors.Is(err, resp.ErrProtocol) {
				_ = resp.WriteError(w, "ERR "+err.Error())
				_ = w.Flush()
			}
			return
		}

		if err := s.dispatch(w, cmd); err != nil {
			return
		}

		// Flush only when nothing is left to process. While more commands sit
		// in the read buffer we keep batching replies, so a pipelined burst
		// costs one write syscall instead of one per command. An empty buffer
		// means the next read would block -- and blocking without flushing
		// first deadlocks a client waiting on a reply still sitting in w.
		if r.Buffered() == 0 {
			// Durability before acknowledgement. The log has to reach the
			// kernel before the reply reaches the client, or a kill -9 would
			// lose a write we already confirmed. Note this rides the same
			// batching rule (ADR-005), so a pipelined burst costs one log
			// write rather than one per command.
			if s.log != nil {
				if err := s.log.Flush(); err != nil {
					// Never answer OK for a write we could not record. Dropping
					// the connection tells the client something went wrong,
					// which a false acknowledgement would not.
					log.Printf("append-only log flush for %s: %v", conn.RemoteAddr(), err)
					return
				}
			}
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}
