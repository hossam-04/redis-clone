package server

import (
	"bufio"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

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
		// NX / XX / KEEPTTL and friends are still unsupported.
		switch len(cmd) {
		case 3:
			s.store.Set(cmd[1], cmd[2])
			return resp.WriteSimpleString(w, "OK")
		case 5:
			ttl, errMsg := parseExpiry(cmd[3], cmd[4])
			if errMsg != "" {
				return resp.WriteError(w, errMsg)
			}
			s.store.SetWithTTL(cmd[1], cmd[2], ttl)
			return resp.WriteSimpleString(w, "OK")
		}

	case "TTL":
		if len(cmd) == 2 {
			d, hasTTL, exists := s.store.TTL(cmd[1])
			switch {
			case !exists:
				return resp.WriteInteger(w, -2)
			case !hasTTL:
				return resp.WriteInteger(w, -1)
			default:
				// Redis rounds up, so a key with 900ms left reports 1 rather
				// than 0. Reporting 0 would read as "expires this instant"
				// for a key with most of a second to live.
				return resp.WriteInteger(w, int64(math.Ceil(d.Seconds())))
			}
		}

	case "DBSIZE":
		if len(cmd) == 1 {
			return resp.WriteInteger(w, int64(s.store.Len()))
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

// parseExpiry turns a SET option pair like ("EX", "10") into a duration.
//
// It returns a RESP error message rather than a Go error because these strings
// are part of the wire contract: ADR-003 makes real redis-cli the correctness
// bar, and clients do match on Redis's exact error text. An empty string means
// the parse succeeded.
func parseExpiry(unit, value string) (time.Duration, string) {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, "ERR value is not an integer or out of range"
	}

	var scale time.Duration
	switch strings.ToUpper(unit) {
	case "EX":
		scale = time.Second
	case "PX":
		scale = time.Millisecond
	default:
		return 0, "ERR syntax error"
	}

	// Zero and negative expiries are rejected outright rather than treated as
	// "delete immediately", which is what Redis does.
	//
	// The upper bound is not paranoia: time.Duration is an int64 count of
	// nanoseconds, so it tops out around 292 years. "EX 999999999999" would
	// overflow the multiplication below and wrap to a negative duration --
	// producing a key that is already expired, from a client asking for a
	// very long life. Silently doing the opposite of what was asked is worse
	// than refusing.
	if n <= 0 || n > int64(math.MaxInt64)/int64(scale) {
		return 0, "ERR invalid expire time in 'set' command"
	}
	return time.Duration(n) * scale, ""
}
