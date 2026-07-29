package server

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/hossam-04/redis-clone/internal/resp"
)

// dispatch executes one command and writes its reply.
//
// The returned error means an I/O failure and nothing else. A command the
// client got wrong -- unknown name, wrong argument count -- produces an error
// reply and a nil return, because the connection is still perfectly usable
// and the client is entitled to keep going.
func (s *Server) dispatch(w *bufio.Writer, cmd resp.Command) error {
	// Command names are case-insensitive in Redis: redis-cli sends "PING",
	// but "ping" is equally valid. Keys and values stay case-sensitive.
	switch strings.ToUpper(cmd[0]) {
	case "PING":
		// PING takes an optional message, echoed back instead of PONG.
		switch len(cmd) {
		case 1:
			return resp.WriteSimpleString(w, "PONG")
		case 2:
			return resp.WriteBulkString(w, cmd[1])
		}

	case "ECHO":
		if len(cmd) == 2 {
			return resp.WriteBulkString(w, cmd[1])
		}

	case "SET":
		// EX / PX and the other options arrive in milestone 2.
		if len(cmd) == 3 {
			s.store.Set(cmd[1], cmd[2])
			return resp.WriteSimpleString(w, "OK")
		}

	case "GET":
		if len(cmd) == 2 {
			v, ok := s.store.Get(cmd[1])
			if !ok {
				// The missing-key case that the null bulk string exists for.
				return resp.WriteNull(w)
			}
			return resp.WriteBulkString(w, v)
		}

	default:
		return resp.WriteError(w, fmt.Sprintf("ERR unknown command '%s'", cmd[0]))
	}

	// Every known command above returns on its valid arities, so reaching
	// here means the name was recognised but the argument count was not.
	return resp.WriteError(w, fmt.Sprintf(
		"ERR wrong number of arguments for '%s' command", strings.ToLower(cmd[0])))
}
