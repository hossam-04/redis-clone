package aof

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/hossam-04/redis-clone/internal/resp"
)

// writeLog builds a log file holding cmds and returns its path.
func writeLog(t *testing.T, cmds ...resp.Command) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.aof")
	l, err := Open(path, Never)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, c := range cmds {
		if err := l.Append(c); err != nil {
			t.Fatalf("Append(%q): %v", c, err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path
}

// chop removes n bytes from the end, which is what a crash during an append
// leaves behind.
func chop(t *testing.T, path string, n int64) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-n); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func size(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	return info.Size()
}

// collector returns an apply func that records what it was given.
func collector(got *[]resp.Command) func(resp.Command) error {
	return func(c resp.Command) error {
		*got = append(*got, slices.Clone(c))
		return nil
	}
}

func TestReplayMissingFileIsNotAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.aof")

	var got []resp.Command
	res, err := Replay(path, collector(&got))
	if err != nil {
		t.Fatalf("Replay on a missing file: %v (the first run has no log yet)", err)
	}
	if res.Commands != 0 || len(got) != 0 {
		t.Errorf("replayed %d commands from a missing file, want 0", res.Commands)
	}
}

func TestReplayAppliesEveryCommandInOrder(t *testing.T) {
	want := []resp.Command{
		{"SET", "a", "1"},
		{"SET", "b", "2"},
		{"SET", "a", "3"},
	}
	path := writeLog(t, want...)

	var got []resp.Command
	res, err := Replay(path, collector(&got))
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Commands != len(want) {
		t.Errorf("Commands = %d, want %d", res.Commands, len(want))
	}
	if res.Truncated != 0 {
		t.Errorf("Truncated = %d on an intact log, want 0", res.Truncated)
	}
	for i := range want {
		if i >= len(got) || !slices.Equal(got[i], want[i]) {
			t.Fatalf("command %d = %q, want %q\nall: %q", i, got[i], want[i], got)
		}
	}
}

// TestReplayTruncatedMidValue is the ordinary crash case: the last record was
// cut off partway through a value's bytes.
func TestReplayTruncatedMidValue(t *testing.T) {
	path := writeLog(t, resp.Command{"SET", "a", "1"}, resp.Command{"SET", "b", "2"})
	chop(t, path, 2) // clips the trailing CRLF off the last value

	var got []resp.Command
	res, err := Replay(path, collector(&got))
	if err != nil {
		t.Fatalf("Replay: %v (a torn tail is a crash, not corruption)", err)
	}
	if res.Commands != 1 {
		t.Errorf("Commands = %d, want 1 (the intact command before the fragment)", res.Commands)
	}
	if res.Truncated == 0 {
		t.Error("Truncated = 0, but the tail was a fragment")
	}
}

// TestReplayTruncatedMidHeader is the case that a naive implementation gets
// wrong. A record cut off inside a header line surfaces as plain io.EOF, which
// looks exactly like a clean end of file -- so trusting the error kind rather
// than the byte offset would leave the fragment on disk, and the next append
// would land after it and corrupt the middle of the file.
func TestReplayTruncatedMidHeader(t *testing.T) {
	path := writeLog(t, resp.Command{"SET", "a", "1"}, resp.Command{"SET", "b", "2"})
	chop(t, path, 5) // leaves the last element's "$1" with no CRLF

	var got []resp.Command
	res, err := Replay(path, collector(&got))
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Commands != 1 {
		t.Errorf("Commands = %d, want 1", res.Commands)
	}
	if res.Truncated == 0 {
		t.Fatal("Truncated = 0: the fragment was mistaken for a clean end of file")
	}
	// 27 bytes is one "SET x y" command; the file must now hold exactly that.
	if got, want := size(t, path), int64(27); got != want {
		t.Errorf("file is %d bytes after truncation, want %d", got, want)
	}
}

// TestTruncatedLogIsReusable is why truncation is necessary rather than merely
// tolerant: the file has to be safe to append to afterwards.
func TestTruncatedLogIsReusable(t *testing.T) {
	path := writeLog(t, resp.Command{"SET", "a", "1"}, resp.Command{"SET", "b", "2"})
	chop(t, path, 5)

	var first []resp.Command
	if _, err := Replay(path, collector(&first)); err != nil {
		t.Fatalf("first Replay: %v", err)
	}

	// Append after recovery, as a restarted server would.
	l, err := Open(path, Never)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := l.Append(resp.Command{"SET", "c", "3"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The second replay must be completely clean -- no leftover fragment.
	var second []resp.Command
	res, err := Replay(path, collector(&second))
	if err != nil {
		t.Fatalf("second Replay: %v (the fragment was not removed)", err)
	}
	if res.Truncated != 0 {
		t.Errorf("Truncated = %d on the second pass, want 0", res.Truncated)
	}
	want := []resp.Command{{"SET", "a", "1"}, {"SET", "c", "3"}}
	for i := range want {
		if i >= len(second) || !slices.Equal(second[i], want[i]) {
			t.Fatalf("after recovery, command %d = %q, want %q\nall: %q",
				i, second[i], want[i], second)
		}
	}
}

// TestReplayRefusesCorruptionInTheMiddle covers the case a crash cannot
// explain. Appends are sequential, so damage anywhere but the tail means
// something unknown altered the file, and serving that data silently would be
// worse than not starting.
func TestReplayRefusesCorruptionInTheMiddle(t *testing.T) {
	path := writeLog(t,
		resp.Command{"SET", "a", "1"},
		resp.Command{"SET", "b", "2"},
		resp.Command{"SET", "c", "3"},
	)

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	b[27] = 'X' // where the second command's '*' should be
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	var got []resp.Command
	if _, err := Replay(path, collector(&got)); err == nil {
		t.Fatal("Replay accepted a log corrupted in the middle; it must refuse")
	}
	if len(got) != 1 {
		t.Errorf("applied %d commands before failing, want 1", len(got))
	}
	// The file must be left alone: we do not understand the damage, so
	// truncating it would destroy evidence and possibly good data.
	if got, want := size(t, path), int64(81); got != want {
		t.Errorf("file is %d bytes, want %d unchanged", got, want)
	}
}

func TestReplayPropagatesApplyErrors(t *testing.T) {
	path := writeLog(t, resp.Command{"SET", "a", "1"}, resp.Command{"SET", "b", "2"})
	sentinel := errors.New("boom")

	calls := 0
	_, err := Replay(path, func(resp.Command) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("error = %v, want it to wrap %v", err, sentinel)
	}
	if calls != 1 {
		t.Errorf("apply called %d times, want 1 (replay should stop at the failure)", calls)
	}
}
