package aof

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/hossam-04/redis-clone/internal/resp"
)

// Result describes what a Replay did.
type Result struct {
	// Commands is how many were applied.
	Commands int
	// Truncated is how many bytes were cut from a partial trailing command.
	// Non-zero means the previous run was killed mid-append.
	Truncated int64
}

// Replay reads the log at path and hands every command to apply, in order.
// A missing file is not an error -- that is simply the first run.
//
// A partial command at the end of the file is treated as a crash, not as
// corruption, because that is what it is. A process killed during an append
// leaves a prefix of the last record, every time. So the file is truncated
// back to the last complete command and replay continues. Refusing to start
// would mean never recovering from precisely the event this log exists to
// survive.
//
// Truncating is not merely tolerant, it is necessary: leaving the fragment in
// place would put the next append after garbage, turning a recoverable tail
// into corruption in the middle of the file.
//
// Bytes that are malformed rather than merely short are a different matter.
// Appends are sequential, so a crash can only ever damage the tail; nonsense
// anywhere else means something we do not understand has altered the file, and
// starting anyway would silently serve wrong data. That is an error, and the
// server should refuse to come up.
func Replay(path string, apply func(resp.Command) error) (Result, error) {
	var res Result

	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return res, nil
	}
	if err != nil {
		return res, fmt.Errorf("open append-only log %s: %w", path, err)
	}
	defer f.Close()

	counter := &countingReader{r: f}
	br := bufio.NewReader(counter)

	// good is the offset just past the last command that parsed and applied
	// cleanly -- the point the file is cut back to if the tail is a fragment.
	var good int64

	for {
		cmd, err := resp.ReadCommand(br)
		if err == nil {
			if applyErr := apply(cmd); applyErr != nil {
				return res, fmt.Errorf("replaying command %d %q: %w",
					res.Commands+1, cmd, applyErr)
			}
			res.Commands++
			good = consumed(counter, br)
			continue
		}

		// Not a short read, so a crash cannot account for it.
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return res, fmt.Errorf("append-only log %s is corrupt after byte %d: %w",
				path, good, err)
		}

		// A short read exactly on a command boundary is just the end of the
		// file, with every record intact.
		//
		// Checking the offset matters rather than trusting the error kind: a
		// record cut off inside a header line surfaces as plain io.EOF, and
		// treating that as a clean end would leave the fragment on disk.
		if consumed(counter, br) == good {
			return res, nil
		}

		info, statErr := os.Stat(path)
		if statErr != nil {
			return res, fmt.Errorf("stat %s: %w", path, statErr)
		}
		res.Truncated = info.Size() - good
		if truncErr := os.Truncate(path, good); truncErr != nil {
			return res, fmt.Errorf("truncate partial record from %s: %w", path, truncErr)
		}
		return res, nil
	}
}

// countingReader tracks how many bytes have been pulled out of the file.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// consumed is the file offset the parser has actually reached: bytes pulled
// from the file, less the ones bufio read ahead but has not handed over yet.
//
// bufio.Reader deliberately reads more than it is asked for, so its own
// position is always ahead of the parser's. Truncating to the reader's
// position instead of the parser's would discard whole valid commands.
func consumed(c *countingReader, br *bufio.Reader) int64 {
	return c.n - int64(br.Buffered())
}
