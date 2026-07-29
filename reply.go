package main

import (
	"bufio"
	"errors"
	"strconv"
	"strings"
)

// The reply grammar is wider than the request grammar. A client only ever
// sends arrays of bulk strings; a server picks whichever type carries the most
// meaning for the answer:
//
//	+OK\r\n             simple string -- short, server-chosen, no CRLF allowed
//	-ERR bad thing\r\n  error -- the client raises this rather than returning it
//	$6\r\nhossam\r\n    bulk string -- arbitrary bytes, length-prefixed
//	$0\r\n\r\n          empty string -- length 0
//	$-1\r\n             null -- "no value", which is NOT the empty string

// errUnwritableSimple flags a bug in this server rather than bad client input.
// A CRLF inside a simple string would end the reply early and desynchronize
// the client, so it is caught here instead of corrupting the connection.
var errUnwritableSimple = errors.New("simple string contains CR or LF")

// writeSimpleString writes +<s>\r\n.
//
// Simple strings are framed by the CRLF that terminates them -- the same
// delimiter-scanning arrangement, with the same restriction, as readLine on
// the request side. That makes them safe only for fixed answers this server
// chooses itself, like OK and PONG. Anything derived from user data must go
// through writeBulkString, where the length prefix makes content irrelevant.
func writeSimpleString(w *bufio.Writer, s string) error {
	if strings.ContainsAny(s, "\r\n") {
		return errUnwritableSimple
	}
	w.WriteByte('+')
	w.WriteString(s)
	_, err := w.WriteString("\r\n")
	return err
}

// writeError writes -<msg>\r\n. Redis convention is a leading uppercase code,
// e.g. "ERR unknown command 'FOO'"; redis-cli renders these as errors rather
// than values.
func writeError(w *bufio.Writer, msg string) error {
	if strings.ContainsAny(msg, "\r\n") {
		return errUnwritableSimple
	}
	w.WriteByte('-')
	w.WriteString(msg)
	_, err := w.WriteString("\r\n")
	return err
}

// writeBulkString writes $<len>\r\n<s>\r\n, and is safe for any bytes at all.
//
// The intermediate writes go unchecked on purpose: bufio.Writer holds a sticky
// error, so after the first failure every later write is a no-op that returns
// that same error. One check at the end catches anything that went wrong,
// without five error branches obscuring a five-line function.
func writeBulkString(w *bufio.Writer, s string) error {
	w.WriteByte('$')
	w.WriteString(strconv.Itoa(len(s)))
	w.WriteString("\r\n")
	w.WriteString(s)
	_, err := w.WriteString("\r\n")
	return err
}

// writeNullBulkString writes $-1\r\n -- RESP's "there is no value here".
//
// This is what lets GET on a missing key differ from GET on a key holding "".
// A length of -1 is impossible, which is precisely why it is free to mean
// something other than a length -- no extra flag byte on every reply, no
// ambiguity. Collapsing null into "" would break any cache built on this
// server, since a miss and a cached empty value would be indistinguishable.
func writeNullBulkString(w *bufio.Writer) error {
	_, err := w.WriteString("$-1\r\n")
	return err
}
