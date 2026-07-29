package server

import (
	"bufio"
	"strings"
	"testing"

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
