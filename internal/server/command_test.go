package server

import (
	"bufio"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hossam-04/redis-clone/internal/resp"
	"github.com/hossam-04/redis-clone/internal/store"
)

// run executes one command against s and returns the exact reply bytes.
//
// No socket, no listener, no goroutines: dispatch writes to a bufio.Writer,
// and a strings.Builder is one. Same trick as the parser tests, where the
// reader was a strings.Reader rather than a net.Conn.
func run(t *testing.T, s *Server, cmd resp.Command) string {
	t.Helper()
	var sb strings.Builder
	w := bufio.NewWriter(&sb)
	if err := s.dispatch(w, cmd); err != nil {
		t.Fatalf("dispatch(%q): %v", cmd, err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return sb.String()
}

func TestDispatch(t *testing.T) {
	tests := []struct {
		name string
		cmd  resp.Command
		want string
	}{
		{"PING", resp.Command{"PING"}, "+PONG\r\n"},
		// Command names are case-insensitive; keys and values are not.
		{"lowercase ping", resp.Command{"ping"}, "+PONG\r\n"},
		{"PING with message", resp.Command{"PING", "hi"}, "$2\r\nhi\r\n"},
		{"ECHO", resp.Command{"ECHO", "hello"}, "$5\r\nhello\r\n"},
		{"ECHO preserves CRLF", resp.Command{"ECHO", "a\r\nb"}, "$4\r\na\r\nb\r\n"},
		{"SET", resp.Command{"SET", "k", "v"}, "+OK\r\n"},
		{"GET missing key", resp.Command{"GET", "absent"}, "$-1\r\n"},

		{"unknown command", resp.Command{"FOO"}, "-ERR unknown command 'FOO'\r\n"},
		{"PING too many args", resp.Command{"PING", "a", "b"},
			"-ERR wrong number of arguments for 'ping' command\r\n"},
		{"GET without a key", resp.Command{"GET"},
			"-ERR wrong number of arguments for 'get' command\r\n"},
		{"SET without a value", resp.Command{"SET", "k"},
			"-ERR wrong number of arguments for 'set' command\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(store.New())
			if got := run(t, s, tt.cmd); got != tt.want {
				t.Errorf("dispatch(%q) = %q, want %q", tt.cmd, got, tt.want)
			}
		})
	}
}

func TestSetThenGet(t *testing.T) {
	s := New(store.New())

	if got := run(t, s, resp.Command{"SET", "name", "hossam"}); got != "+OK\r\n" {
		t.Fatalf("SET = %q, want %q", got, "+OK\r\n")
	}
	if got, want := run(t, s, resp.Command{"GET", "name"}), "$6\r\nhossam\r\n"; got != want {
		t.Errorf("GET = %q, want %q", got, want)
	}
}

// TestEmptyValueRepliesEmptyNotNull is the end-to-end version of the
// distinction: a key explicitly set to "" must come back as a zero-length
// bulk string, never as null, or a cache built on this server could not tell
// a stored empty value from a miss.
func TestEmptyValueRepliesEmptyNotNull(t *testing.T) {
	s := New(store.New())
	run(t, s, resp.Command{"SET", "empty", ""})

	got := run(t, s, resp.Command{"GET", "empty"})
	if got == "$-1\r\n" {
		t.Fatal("GET on a key set to \"\" replied null; empty and missing have collapsed")
	}
	if want := "$0\r\n\r\n"; got != want {
		t.Errorf("GET = %q, want %q", got, want)
	}
}

func TestSetOverwrites(t *testing.T) {
	s := New(store.New())
	run(t, s, resp.Command{"SET", "k", "first"})
	run(t, s, resp.Command{"SET", "k", "second"})

	if got, want := run(t, s, resp.Command{"GET", "k"}), "$6\r\nsecond\r\n"; got != want {
		t.Errorf("GET after overwrite = %q, want %q", got, want)
	}
}

func TestSetWithExpiry(t *testing.T) {
	tests := []struct {
		name string
		cmd  resp.Command
		want string
	}{
		{"EX seconds", resp.Command{"SET", "k", "v", "EX", "10"}, "+OK\r\n"},
		{"PX milliseconds", resp.Command{"SET", "k", "v", "PX", "5000"}, "+OK\r\n"},
		// Option names are case-insensitive, like command names.
		{"lowercase ex", resp.Command{"SET", "k", "v", "ex", "10"}, "+OK\r\n"},

		{"unknown option", resp.Command{"SET", "k", "v", "ZZ", "10"},
			"-ERR syntax error\r\n"},
		{"non-numeric expiry", resp.Command{"SET", "k", "v", "EX", "soon"},
			"-ERR value is not an integer or out of range\r\n"},
		{"zero expiry", resp.Command{"SET", "k", "v", "EX", "0"},
			"-ERR invalid expire time in 'set' command\r\n"},
		{"negative expiry", resp.Command{"SET", "k", "v", "EX", "-5"},
			"-ERR invalid expire time in 'set' command\r\n"},
		// Would overflow time.Duration's int64 nanoseconds and wrap negative,
		// producing an already-expired key from a request for a very long one.
		{"expiry that would overflow", resp.Command{"SET", "k", "v", "EX", "999999999999"},
			"-ERR invalid expire time in 'set' command\r\n"},

		{"option without a value", resp.Command{"SET", "k", "v", "EX"},
			"-ERR wrong number of arguments for 'set' command\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(store.New())
			if got := run(t, s, tt.cmd); got != tt.want {
				t.Errorf("dispatch(%q) = %q, want %q", tt.cmd, got, tt.want)
			}
		})
	}
}

// TestTTLSentinels covers the three states one integer reply has to carry.
// Collapsing any two would leave a client unable to tell a permanent key from
// a missing one -- the same failure as answering both a miss and a stored
// empty string with a null.
func TestTTLSentinels(t *testing.T) {
	tests := []struct {
		name  string
		setup resp.Command // empty means set nothing up
		want  string
	}{
		{"key with a TTL reports seconds left", resp.Command{"SET", "k", "v", "EX", "10"}, ":10\r\n"},
		{"key with no TTL reports -1", resp.Command{"SET", "k", "v"}, ":-1\r\n"},
		{"missing key reports -2", nil, ":-2\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(store.New())
			if tt.setup != nil {
				run(t, s, tt.setup)
			}
			if got := run(t, s, resp.Command{"TTL", "k"}); got != tt.want {
				t.Errorf("TTL = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestTTLRoundsUp pins Redis's rounding: a key with most of a second left
// must not report 0, which would read as "expiring right now".
func TestTTLRoundsUp(t *testing.T) {
	s := New(store.New())
	run(t, s, resp.Command{"SET", "k", "v", "PX", "1500"})

	if got, want := run(t, s, resp.Command{"TTL", "k"}), ":2\r\n"; got != want {
		t.Errorf("TTL of a 1500ms key = %q, want %q", got, want)
	}
}

func TestTTLWrongArity(t *testing.T) {
	s := New(store.New())
	want := "-ERR wrong number of arguments for 'ttl' command\r\n"
	if got := run(t, s, resp.Command{"TTL"}); got != want {
		t.Errorf("TTL with no key = %q, want %q", got, want)
	}
}

// TestPlainSetClearsTTL is the command-level half of Redis's rule that a
// plain SET discards any existing expiry.
func TestPlainSetClearsTTL(t *testing.T) {
	s := New(store.New())
	run(t, s, resp.Command{"SET", "k", "v", "EX", "100"})
	run(t, s, resp.Command{"SET", "k", "v"})

	if got, want := run(t, s, resp.Command{"TTL", "k"}), ":-1\r\n"; got != want {
		t.Errorf("TTL after a plain SET = %q, want %q (the TTL survived)", got, want)
	}
}

// TestSetWithAbsoluteExpiry covers the EXAT/PXAT forms. These exist so the
// append-only log can record a deadline that means the same thing whenever it
// is replayed, rather than a duration that restarts on every restart.
func TestSetWithAbsoluteExpiry(t *testing.T) {
	future := time.Now().Add(time.Hour)

	tests := []struct {
		name    string
		cmd     resp.Command
		wantSet string
		wantGet string // "" means "expect the key to be readable"
	}{
		{
			"PXAT in the future",
			resp.Command{"SET", "k", "v", "PXAT", strconv.FormatInt(future.UnixMilli(), 10)},
			"+OK\r\n", "",
		},
		{
			"EXAT in the future",
			resp.Command{"SET", "k", "v", "EXAT", strconv.FormatInt(future.Unix(), 10)},
			"+OK\r\n", "",
		},
		{
			"lowercase pxat",
			resp.Command{"SET", "k", "v", "pxat", strconv.FormatInt(future.UnixMilli(), 10)},
			"+OK\r\n", "",
		},
		{
			// A deadline in the past is accepted, not rejected. Replaying a
			// log produces exactly this whenever a TTL lapsed during the
			// outage, and the key must come back already dead.
			"PXAT in the past",
			resp.Command{"SET", "k", "v", "PXAT", "1000"},
			"+OK\r\n", "$-1\r\n",
		},
		{
			"EXAT in the past",
			resp.Command{"SET", "k", "v", "EXAT", "1"},
			"+OK\r\n", "$-1\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(store.New())
			if got := run(t, s, tt.cmd); got != tt.wantSet {
				t.Fatalf("SET = %q, want %q", got, tt.wantSet)
			}
			got := run(t, s, resp.Command{"GET", "k"})
			if tt.wantGet == "" {
				if got == "$-1\r\n" {
					t.Errorf("GET = null, but the deadline is in the future")
				}
			} else if got != tt.wantGet {
				t.Errorf("GET = %q, want %q", got, tt.wantGet)
			}
		})
	}
}

// TestRelativeExpiryIsResolvedAtSetTime is the property that makes the log
// replay-safe. Two SETs of the same relative TTL issued at different moments
// must produce different deadlines, so that recording the deadline (rather
// than the duration) captures when the key actually dies.
func TestRelativeExpiryIsResolvedAtSetTime(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	early, errMsg := parseExpiry("EX", "3600", base)
	if errMsg != "" {
		t.Fatalf("parseExpiry: %s", errMsg)
	}
	late, errMsg := parseExpiry("EX", "3600", base.Add(3*time.Hour))
	if errMsg != "" {
		t.Fatalf("parseExpiry: %s", errMsg)
	}

	if !early.Before(late) {
		t.Errorf("EX 3600 resolved to %v at 12:00 and %v at 15:00; the same instant means replay would extend TTLs",
			early, late)
	}
	if want := base.Add(time.Hour); !early.Equal(want) {
		t.Errorf("EX 3600 at %v = %v, want %v", base, early, want)
	}
}
