package main

import (
	"bufio"
	"fmt"
	"strings"
)

// dispatch executes one command and writes its reply.
//
// The returned error means an I/O failure and nothing else. A command the
// client got wrong -- unknown name, wrong argument count -- produces an error
// reply and a nil return, because the connection is still perfectly usable
// and the client is entitled to keep going.
func dispatch(w *bufio.Writer, store *Store, cmd Command) error {
	// Command names are case-insensitive in Redis: redis-cli sends "PING",
	// but "ping" is equally valid. Keys and values stay case-sensitive.
	switch strings.ToUpper(cmd[0]) {
	case "PING":
		// PING takes an optional message, echoed back instead of PONG.
		switch len(cmd) {
		case 1:
			return writeSimpleString(w, "PONG")
		case 2:
			return writeBulkString(w, cmd[1])
		}

	case "ECHO":
		if len(cmd) == 2 {
			return writeBulkString(w, cmd[1])
		}

	case "SET":
		// EX / PX and the other options arrive in week 2.
		if len(cmd) == 3 {
			store.Set(cmd[1], cmd[2])
			return writeSimpleString(w, "OK")
		}

	case "GET":
		if len(cmd) == 2 {
			v, ok := store.Get(cmd[1])
			if !ok {
				// The missing-key case that the null bulk string exists for.
				return writeNullBulkString(w)
			}
			return writeBulkString(w, v)
		}

	default:
		return writeError(w, fmt.Sprintf("ERR unknown command '%s'", cmd[0]))
	}

	// Every known command above returns on its valid arities, so reaching
	// here means the name was recognised but the argument count was not.
	return writeError(w, fmt.Sprintf(
		"ERR wrong number of arguments for '%s' command", strings.ToLower(cmd[0])))
}
