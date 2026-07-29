// Package aof implements append-only persistence: every command that changes
// state is appended to a log, and the log is replayed on startup to rebuild
// the data.
//
// Commands are stored in the same RESP form clients send, so replay reuses the
// existing parser rather than inventing a second format that could drift away
// from the first.
package aof

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/hossam-04/redis-clone/internal/resp"
)

// Policy decides how hard the log works to survive a crash.
//
// Three options exist because "we wrote it to the file" and "the data is on
// the disk" are different claims. A write syscall hands bytes to the kernel,
// which acknowledges them and passes them to the drive whenever convenient --
// possibly many seconds later. fsync is what forces them out and waits for the
// device to confirm, and it costs milliseconds against microseconds for the
// write.
//
// That difference produces two failure modes rather than one, and they are not
// equally severe:
//
//	kill -9      the process dies; the kernel does not. Anything already
//	             written survives with no fsync at all.
//	power loss   the kernel dies too, taking whatever it had not yet handed
//	             to the drive. Only fsync protects against this.
type Policy int

const (
	// EverySecond fsyncs on a one-second tick. Survives kill -9 entirely, and
	// loses at most a second of writes to power loss. Redis's default and
	// ours -- but note what it means: a client can receive +OK for a write
	// that a power failure then erases.
	EverySecond Policy = iota

	// Always fsyncs before any reply goes out, so an acknowledged write is
	// genuinely durable. Costs a millisecond-scale disk round trip per batch,
	// which is why it is not the default.
	Always

	// Never leaves flushing to the kernel's own schedule. Survives kill -9;
	// against power loss it promises nothing at all.
	Never
)

// Log is an append-only command log.
type Log struct {
	policy Policy

	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
	// dirty means bytes have reached the kernel but not necessarily the disk,
	// so the ticker can skip a pointless fsync when nothing has changed.
	dirty bool

	stop chan struct{}
	done chan struct{}
}

// Open opens or creates the log at path for appending.
func Open(path string, policy Policy) (*Log, error) {
	// O_APPEND makes every write land at the current end of file as one
	// operation, so the kernel -- not us -- serialises concurrent appends. It
	// also means we never seek, which is what keeps a torn write confined to
	// the tail of the file rather than the middle of it.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open append-only log %s: %w", path, err)
	}

	l := &Log{
		policy: policy,
		f:      f,
		w:      bufio.NewWriter(f),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	if policy == EverySecond {
		go l.syncPeriodically(time.Second)
	} else {
		close(l.done) // nothing to wait for at Close
	}
	return l, nil
}

// Append buffers cmd. It has not reached the kernel until Flush.
func (l *Log) Append(cmd resp.Command) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return resp.WriteCommand(l.w, cmd)
}

// Flush pushes buffered commands into the kernel, and fsyncs if the policy
// demands it.
//
// Callers must do this BEFORE replying to the client, and the ordering is the
// whole point of a write-ahead log. A reply is a promise that the command was
// accepted; sending it while the command still sits in this process's own
// buffer means a kill -9 loses a write we already acknowledged. Flushing first
// makes every reply the client ever saw correspond to bytes the kernel holds.
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.w.Flush(); err != nil {
		return err
	}
	l.dirty = true
	if l.policy == Always {
		return l.sync()
	}
	return nil
}

// sync fsyncs if there is anything to fsync. Callers must hold mu.
func (l *Log) sync() error {
	if !l.dirty {
		return nil
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.dirty = false
	return nil
}

// syncPeriodically fsyncs on a tick for the EverySecond policy.
func (l *Log) syncPeriodically(every time.Duration) {
	defer close(l.done)

	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-tick.C:
			l.mu.Lock()
			// Flush before syncing: bytes still in our buffer are not yet the
			// kernel's problem, so an fsync would not cover them.
			if err := l.w.Flush(); err == nil {
				_ = l.sync()
			}
			l.mu.Unlock()
		}
	}
}

// Close stops the background sync, flushes, fsyncs, and closes the file.
//
// It fsyncs whatever the policy is. A clean shutdown has no throughput left to
// protect, so there is no reason to leave writes at the kernel's mercy.
func (l *Log) Close() error {
	close(l.stop)
	<-l.done

	l.mu.Lock()
	defer l.mu.Unlock()

	flushErr := l.w.Flush()
	syncErr := l.f.Sync()
	closeErr := l.f.Close()

	// Report the earliest failure: a flush error explains a sync error, and
	// both explain more than a close error does.
	for _, err := range []error{flushErr, syncErr, closeErr} {
		if err != nil {
			return err
		}
	}
	return nil
}
