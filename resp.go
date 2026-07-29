package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// ErrProtocol marks a malformed command, as distinct from a dead connection.
// The difference matters to the caller: a protocol error deserves an error
// reply on a still-usable connection, whereas an I/O error means the client
// is gone and there is nobody left to reply to.
var ErrProtocol = errors.New("protocol error")

// Both limits exist because the numbers they guard arrive from the client and
// are used as allocation sizes. Without a ceiling, "$999999999999" is a
// one-line denial of service: we would try to allocate a terabyte on request
// and die. Values match real Redis.
const (
	// MaxBulkSize caps the byte length of a single argument.
	MaxBulkSize = 512 * 1024 * 1024
	// MaxArgs caps how many arguments one command may declare.
	MaxArgs = 1024 * 1024
)

// Command is one client request, already split into its parts:
// ["SET", "name", "hossam"]. Argument 0 is the command name.
type Command []string

// readLine consumes bytes through the next CRLF and returns what preceded it.
//
// This is used ONLY for RESP's short header lines ("*3", "$16"), never for
// payloads. Scanning for a delimiter is exactly the mistake that breaks on
// binary data -- a stored JPEG will contain \n somewhere. Payload bytes are
// read by length instead, in readBulkString below.
func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", fmt.Errorf("%w: line not CRLF-terminated", ErrProtocol)
	}
	return line[:len(line)-2], nil
}

// readInt reads one header line and parses it as an integer -- the element
// count in "*3", or the byte length in "$16".
func readInt(r *bufio.Reader) (int, error) {
	line, err := readLine(r)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(line)
	if err != nil {
		return 0, fmt.Errorf("%w: %q is not an integer", ErrProtocol, line)
	}
	return n, nil
}

// readBulkString reads one $<len>\r\n<bytes>\r\n value.
//
// This is where framing actually happens. io.ReadFull loops internally until
// it has all n bytes or hits a real error, so a value split across any number
// of TCP packets is reassembled here rather than silently truncated. Nothing
// above this function has to know that packets exist.
func readBulkString(r *bufio.Reader) (string, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	if prefix != '$' {
		return "", fmt.Errorf("%w: expected '$', got %q", ErrProtocol, prefix)
	}
	n, err := readInt(r)
	if err != nil {
		return "", err
	}
	// RESP does have a null bulk string ("$-1"), but only in server replies --
	// a client command containing one is malformed.
	if n < 0 || n > MaxBulkSize {
		return "", fmt.Errorf("%w: bulk length %d out of range", ErrProtocol, n)
	}
	// n+2 so the trailing CRLF is consumed by the same call.
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return "", fmt.Errorf("%w: bulk string not CRLF-terminated", ErrProtocol)
	}
	return string(buf[:n]), nil
}

// ReadCommand reads exactly one complete command and leaves the reader
// positioned at the first byte of the next one. It blocks until the whole
// command has arrived, however many packets that takes.
//
// A client command is always a RESP array of bulk strings, so this is the
// only shape accepted. Server replies use the richer RESP grammar; the
// client-facing subset is deliberately this narrow.
func ReadCommand(r *bufio.Reader) (Command, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if prefix != '*' {
		return nil, fmt.Errorf("%w: expected '*', got %q", ErrProtocol, prefix)
	}
	n, err := readInt(r)
	if err != nil {
		return nil, err
	}
	if n <= 0 || n > MaxArgs {
		return nil, fmt.Errorf("%w: argument count %d out of range", ErrProtocol, n)
	}

	args := make(Command, n)
	for i := range args {
		if args[i], err = readBulkString(r); err != nil {
			return nil, err
		}
	}
	return args, nil
}
