package resp

import (
	"bufio"
	"errors"
	"strings"
	"testing"
)

// encode runs one writer against a buffer and returns the exact bytes produced.
func encode(t *testing.T, write func(*bufio.Writer) error) string {
	t.Helper()
	var sb strings.Builder
	w := bufio.NewWriter(&sb)
	if err := write(w); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Without Flush the bytes sit in bufio's buffer and never reach the
	// destination -- the same mistake that would hang a real client.
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return sb.String()
}

func TestWriteReplies(t *testing.T) {
	tests := []struct {
		name  string
		write func(*bufio.Writer) error
		want  string
	}{
		{
			"simple string",
			func(w *bufio.Writer) error { return WriteSimpleString(w, "OK") },
			"+OK\r\n",
		},
		{
			"error",
			func(w *bufio.Writer) error { return WriteError(w, "ERR unknown command 'FOO'") },
			"-ERR unknown command 'FOO'\r\n",
		},
		{
			"bulk string",
			func(w *bufio.Writer) error { return WriteBulkString(w, "hossam") },
			"$6\r\nhossam\r\n",
		},
		{
			// The length prefix means payload content is never inspected, so
			// a value containing the delimiter encodes without escaping.
			"bulk string containing CRLF",
			func(w *bufio.Writer) error { return WriteBulkString(w, "a\r\nb") },
			"$4\r\na\r\nb\r\n",
		},
		{
			"empty bulk string",
			func(w *bufio.Writer) error { return WriteBulkString(w, "") },
			"$0\r\n\r\n",
		},
		{
			"null",
			WriteNull,
			"$-1\r\n",
		},
		{
			"integer",
			func(w *bufio.Writer) error { return WriteInteger(w, 42) },
			":42\r\n",
		},
		{
			// Negative integers are how commands smuggle extra meaning into
			// an integer reply -- TTL's -1 and -2 -- so they must encode
			// plainly rather than being treated as an error.
			"negative integer",
			func(w *bufio.Writer) error { return WriteInteger(w, -2) },
			":-2\r\n",
		},
		{
			"zero",
			func(w *bufio.Writer) error { return WriteInteger(w, 0) },
			":0\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := encode(t, tt.write); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TestNullIsNotEmptyString is the entire reason RESP has a null bulk string.
// A cache must be able to tell "I have never seen this key" from "I looked it
// up and the answer was empty"; if these two encodings ever collide, a cached
// empty value becomes indistinguishable from a miss.
func TestNullIsNotEmptyString(t *testing.T) {
	null := encode(t, WriteNull)
	empty := encode(t, func(w *bufio.Writer) error { return WriteBulkString(w, "") })

	if null == empty {
		t.Fatalf("null and empty string encode identically as %q", null)
	}
	t.Logf("null=%q empty=%q — distinguishable, as required", null, empty)
}

// TestSimpleStringRejectsCRLF covers the one input that would silently
// desynchronize a client rather than produce a visible error.
func TestSimpleStringRejectsCRLF(t *testing.T) {
	for _, s := range []string{"bad\r\nvalue", "trailing\r", "trailing\n"} {
		var sb strings.Builder
		w := bufio.NewWriter(&sb)
		if err := WriteSimpleString(w, s); !errors.Is(err, errUnwritableSimple) {
			t.Errorf("WriteSimpleString(%q) error = %v, want errUnwritableSimple", s, err)
		}
	}
}
