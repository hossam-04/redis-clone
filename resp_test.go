package main

import (
	"bufio"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
)

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name string
		wire string
		want Command
	}{
		{
			"no arguments",
			"*1\r\n$4\r\nPING\r\n",
			Command{"PING"},
		},
		{
			"typical command",
			"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$6\r\nhossam\r\n",
			Command{"SET", "name", "hossam"},
		},
		{
			// Spaces are ordinary payload bytes. A parser that split the
			// wire on whitespace would see five arguments here.
			"spaces inside a value",
			"*3\r\n$3\r\nSET\r\n$6\r\navatar\r\n$16\r\nmulti word value\r\n",
			Command{"SET", "avatar", "multi word value"},
		},
		{
			// The binary-safety case: a 4-byte value that is literally
			// "a\r\nb". Anything that scans for CRLF instead of counting
			// bytes truncates here and desynchronizes every command after.
			"CRLF inside a value",
			"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n",
			Command{"SET", "k", "a\r\nb"},
		},
		{
			"empty value",
			"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n",
			Command{"SET", "k", ""},
		},
	}

	// Each case runs under two delivery patterns. "whole" is the easy path
	// that manual redis-cli testing always produces. "one byte per Read" is
	// the most fragmented delivery possible -- strictly worse than any real
	// network -- and it is the one that would catch a broken parser.
	deliveries := []struct {
		mode string
		wrap func(io.Reader) io.Reader
	}{
		{"whole", func(r io.Reader) io.Reader { return r }},
		{"one byte per Read", iotest.OneByteReader},
	}

	for _, tt := range tests {
		for _, d := range deliveries {
			t.Run(tt.name+"/"+d.mode, func(t *testing.T) {
				r := bufio.NewReader(d.wrap(strings.NewReader(tt.wire)))
				got, err := ReadCommand(r)
				if err != nil {
					t.Fatalf("ReadCommand() error = %v, want nil", err)
				}
				if !slices.Equal(got, tt.want) {
					t.Errorf("ReadCommand() = %q, want %q", got, tt.want)
				}
			})
		}
	}
}

// TestReadCommandPipelined covers the leftover-bytes problem: two commands
// arrive in one read, and the second must survive until it is asked for.
// redis-benchmark pipelines by default, so this is the normal case under load.
func TestReadCommandPipelined(t *testing.T) {
	wire := "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"
	r := bufio.NewReader(iotest.OneByteReader(strings.NewReader(wire)))

	want := []Command{{"PING"}, {"ECHO", "hi"}}
	for i, w := range want {
		got, err := ReadCommand(r)
		if err != nil {
			t.Fatalf("command %d: error = %v, want nil", i, err)
		}
		if !slices.Equal(got, w) {
			t.Errorf("command %d = %q, want %q", i, got, w)
		}
	}
	if _, err := ReadCommand(r); !errors.Is(err, io.EOF) {
		t.Errorf("after last command: error = %v, want io.EOF", err)
	}
}

// TestReadCommandTruncated pins down a distinction that matters to the caller:
// a half-arrived command is an I/O condition, not a malformed one. Reporting
// it as a protocol error would blame the client for the network's timing.
func TestReadCommandTruncated(t *testing.T) {
	wire := "*3\r\n$3\r\nSET\r\n$4\r\nna" // cut off mid-value
	r := bufio.NewReader(strings.NewReader(wire))

	_, err := ReadCommand(r)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("error = %v, want io.ErrUnexpectedEOF", err)
	}
	if errors.Is(err, ErrProtocol) {
		t.Error("truncated input reported as a protocol error; it is an I/O condition")
	}
}

func TestReadCommandMalformed(t *testing.T) {
	tests := []struct {
		name string
		wire string
	}{
		{"not an array", "+PING\r\n"},
		{"zero arguments", "*0\r\n"},
		{"negative argument count", "*-1\r\n"},
		{"argument count not a number", "*abc\r\n"},
		{"argument is not a bulk string", "*1\r\n+PING\r\n"},
		{"bulk length not a number", "*1\r\n$abc\r\n"},
		// The denial-of-service case: without a ceiling this asks us to
		// allocate roughly a terabyte before we ever look at the payload.
		{"bulk length absurdly large", "*2\r\n$3\r\nGET\r\n$999999999999\r\n"},
		{"argument count absurdly large", "*999999999\r\n"},
		{"bulk not CRLF-terminated", "*1\r\n$4\r\nPINGXX"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.wire))
			if _, err := ReadCommand(r); !errors.Is(err, ErrProtocol) {
				t.Errorf("error = %v, want ErrProtocol", err)
			}
		})
	}
}
