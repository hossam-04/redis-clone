package aof

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hossam-04/redis-clone/internal/resp"
)

// readLog returns the bytes currently on disk, which is not the same as the
// bytes appended -- that gap is what Flush exists to close.
func readLog(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func TestAppendedCommandsAreStoredAsRESP(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	l, err := Open(path, Never)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	if err := l.Append(resp.Command{"SET", "name", "hossam"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Byte-identical to what a client sends, which is what lets ReadCommand
	// replay the file with no second parser.
	want := "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$6\r\nhossam\r\n"
	if got := readLog(t, path); got != want {
		t.Errorf("log = %q, want %q", got, want)
	}
}

// TestAppendDoesNotReachDiskUntilFlush pins the property the whole write-ahead
// ordering depends on. If Append alone were enough, there would be nothing for
// callers to sequence against the reply.
func TestAppendDoesNotReachDiskUntilFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	l, err := Open(path, Never)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	if err := l.Append(resp.Command{"SET", "k", "v"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := readLog(t, path); got != "" {
		t.Errorf("log = %q before Flush, want empty", got)
	}

	if err := l.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := readLog(t, path); got == "" {
		t.Error("log still empty after Flush")
	}
}

func TestAppendsAccumulateInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	l, err := Open(path, Never)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	for _, cmd := range []resp.Command{
		{"SET", "a", "1"},
		{"SET", "b", "2"},
		{"SET", "a", "3"},
	} {
		if err := l.Append(cmd); err != nil {
			t.Fatalf("Append(%q): %v", cmd, err)
		}
	}
	if err := l.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got := readLog(t, path)
	// Order is the entire correctness requirement for a replay log: the last
	// write to a key has to be the one that wins.
	if a, b := strings.Index(got, "$1\r\n1\r\n"), strings.Index(got, "$1\r\n3\r\n"); a > b {
		t.Errorf("the earlier write to 'a' appears after the later one\nlog = %q", got)
	}
	if n := strings.Count(got, "*3\r\n"); n != 3 {
		t.Errorf("found %d commands in the log, want 3\nlog = %q", n, got)
	}
}

// TestReopenAppends checks that restarting does not truncate the log. O_TRUNC
// instead of O_APPEND here would silently discard every prior write on every
// restart -- and the server would look like it was working.
func TestReopenAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")

	first, err := Open(path, Never)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := first.Append(resp.Command{"SET", "before", "restart"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := Open(path, Never)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := second.Append(resp.Command{"SET", "after", "restart"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := readLog(t, path)
	for _, want := range []string{"before", "after"} {
		if !strings.Contains(got, want) {
			t.Errorf("log is missing %q after reopen; earlier writes were discarded\nlog = %q", want, got)
		}
	}
}

func TestCloseFlushesWithoutAnExplicitFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	l, err := Open(path, Never)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := l.Append(resp.Command{"SET", "k", "v"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := readLog(t, path); !strings.Contains(got, "$1\r\nk\r\n") {
		t.Errorf("Close did not flush buffered commands; log = %q", got)
	}
}

// TestPoliciesAllPersist checks the property common to every policy. They
// differ only in when fsync happens; all three must get bytes to the kernel,
// because that is what kill -9 survival requires.
func TestPoliciesAllPersist(t *testing.T) {
	for name, policy := range map[string]Policy{
		"EverySecond": EverySecond,
		"Always":      Always,
		"Never":       Never,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "test.aof")
			l, err := Open(path, policy)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := l.Append(resp.Command{"SET", "k", "v"}); err != nil {
				t.Fatalf("Append: %v", err)
			}
			if err := l.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if got := readLog(t, path); !strings.Contains(got, "$1\r\nv\r\n") {
				t.Errorf("log = %q after Flush, want the command present", got)
			}
			if err := l.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

// TestConcurrentAppends covers many client goroutines logging at once. The
// mutex has to make each command atomic in the file: interleaved bytes from
// two commands would produce a log that cannot be parsed at all.
func TestConcurrentAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	l, err := Open(path, EverySecond)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	done := make(chan struct{})
	for g := range 8 {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			for i := range 100 {
				_ = l.Append(resp.Command{"SET", "k", "v"})
				if i%10 == 0 {
					_ = l.Flush()
				}
			}
		}(g)
	}
	for range 8 {
		<-done
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 800 well-formed commands, and no torn ones.
	got := readLog(t, path)
	if n := strings.Count(got, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"); n != 800 {
		t.Errorf("found %d intact commands, want 800 (bytes interleaved?)", n)
	}
}
