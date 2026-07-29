package server

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hossam-04/redis-clone/internal/aof"
	"github.com/hossam-04/redis-clone/internal/resp"
	"github.com/hossam-04/redis-clone/internal/store"
)

func openLog(t *testing.T) (*aof.Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.aof")
	l, err := aof.Open(path, aof.Never)
	if err != nil {
		t.Fatalf("aof.Open: %v", err)
	}
	return l, path
}

func logContents(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// TestRelativeExpiryIsLoggedAsAbsolute is the fix for the trap a verbatim log
// falls into. "EX 3600" recorded literally and replayed three hours later
// would grant a fresh hour to a key that should already be gone, and enough
// restarts would make TTL'd keys immortal.
func TestRelativeExpiryIsLoggedAsAbsolute(t *testing.T) {
	l, path := openLog(t)
	s := New(store.New(), WithLog(l))

	run(t, s, resp.Command{"SET", "k", "v", "EX", "3600"})
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := logContents(t, path)
	if !strings.Contains(got, "PXAT") {
		t.Errorf("log does not record an absolute deadline\nlog = %q", got)
	}
	if strings.Contains(got, "$2\r\nEX\r\n") {
		t.Errorf("log recorded the relative form verbatim; replay would extend the TTL\nlog = %q", got)
	}
}

// TestDeadlinePassedDuringDowntimeStaysDead is the whole point of storing
// absolute deadlines: a key that should have expired while the server was down
// must not come back alive.
func TestDeadlinePassedDuringDowntimeStaysDead(t *testing.T) {
	l, path := openLog(t)

	// A log written before a long outage looks exactly like this: a deadline
	// that has since passed, alongside a key with no deadline at all.
	past := time.Now().Add(-time.Hour).UnixMilli()
	for _, cmd := range []resp.Command{
		{"SET", "lapsed", "v", "PXAT", strconv.FormatInt(past, 10)},
		{"SET", "permanent", "v"},
	} {
		if err := l.Append(cmd); err != nil {
			t.Fatalf("Append(%q): %v", cmd, err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Replay into a fresh store, as a restart does.
	fresh := store.New()
	res, err := aof.Replay(path, New(fresh).Apply)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Commands != 2 {
		t.Fatalf("replayed %d commands, want 2", res.Commands)
	}

	s := New(fresh)
	if got := run(t, s, resp.Command{"GET", "lapsed"}); got != "$-1\r\n" {
		t.Errorf("GET lapsed = %q, want null -- its deadline passed during the outage", got)
	}
	if got := run(t, s, resp.Command{"GET", "permanent"}); got == "$-1\r\n" {
		t.Error("a key with no TTL did not survive replay")
	}
}

// TestWritesSurviveReplay is the plain recovery case.
func TestWritesSurviveReplay(t *testing.T) {
	l, path := openLog(t)
	s := New(store.New(), WithLog(l))

	run(t, s, resp.Command{"SET", "name", "hossam"})
	run(t, s, resp.Command{"SET", "counter", "1"})
	run(t, s, resp.Command{"SET", "counter", "2"}) // later write must win
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fresh := store.New()
	if _, err := aof.Replay(path, New(fresh).Apply); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	recovered := New(fresh)
	if got, want := run(t, recovered, resp.Command{"GET", "name"}), "$6\r\nhossam\r\n"; got != want {
		t.Errorf("GET name = %q, want %q", got, want)
	}
	// Replay applies in order, so the last write to a key is the one that
	// stands. Out-of-order replay would resurrect an overwritten value.
	if got, want := run(t, recovered, resp.Command{"GET", "counter"}), "$1\r\n2\r\n"; got != want {
		t.Errorf("GET counter = %q, want %q", got, want)
	}
}

// TestReadsAreNotLogged keeps the log proportional to writes. On a read-heavy
// cache -- which is most of them -- logging reads would make the file grow with
// traffic that changes nothing.
func TestReadsAreNotLogged(t *testing.T) {
	l, path := openLog(t)
	s := New(store.New(), WithLog(l))

	run(t, s, resp.Command{"SET", "k", "v"})
	for range 50 {
		run(t, s, resp.Command{"GET", "k"})
		run(t, s, resp.Command{"TTL", "k"})
		run(t, s, resp.Command{"PING"})
		run(t, s, resp.Command{"DBSIZE"})
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := logContents(t, path)
	if n := strings.Count(got, "*"); n != 1 {
		t.Errorf("log holds %d commands after 1 write and 200 reads, want 1\nlog = %q", n, got)
	}
}

// TestRejectedWriteIsNotLogged: a command that failed validation never
// happened, so recording it would make replay apply something the client was
// told was invalid.
func TestRejectedWriteIsNotLogged(t *testing.T) {
	l, path := openLog(t)
	s := New(store.New(), WithLog(l))

	run(t, s, resp.Command{"SET", "k", "v", "EX", "0"})    // invalid expire time
	run(t, s, resp.Command{"SET", "k", "v", "ZZ", "10"})   // syntax error
	run(t, s, resp.Command{"SET", "k", "v", "EX", "soon"}) // not an integer
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := logContents(t, path); got != "" {
		t.Errorf("log = %q after three rejected writes, want empty", got)
	}
}

// TestNoLogConfiguredIsFine covers the in-memory case: the only difference
// between a durable server and an ephemeral one should be whether a log is
// attached.
func TestNoLogConfiguredIsFine(t *testing.T) {
	s := New(store.New())
	if got, want := run(t, s, resp.Command{"SET", "k", "v"}), "+OK\r\n"; got != want {
		t.Errorf("SET without a log = %q, want %q", got, want)
	}
	if got, want := run(t, s, resp.Command{"GET", "k"}), "$1\r\nv\r\n"; got != want {
		t.Errorf("GET without a log = %q, want %q", got, want)
	}
}
