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
			if err := s.record(cmd); err != nil {
				return err
			}
			return resp.WriteSimpleString(w, "OK")
		case 5:
			deadline, errMsg := parseExpiry(cmd[3], cmd[4], time.Now())
			if errMsg != "" {
				return resp.WriteError(w, errMsg)
			}
			s.store.SetWithDeadline(cmd[1], cmd[2], deadline)
			// Record the resolved deadline, never the relative form the client
			// used. "EX 3600" replayed three hours later would grant a fresh
			// hour to a key that should already be gone, and enough restarts
			// would make TTL'd keys immortal. PXAT means the same instant
			// whenever it is read.
			if err := s.record(resp.Command{
				"SET", cmd[1], cmd[2],
				"PXAT", strconv.FormatInt(deadline.UnixMilli(), 10),
			}); err != nil {
				return err
			}
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

// record appends a state-changing command to the log, if one is configured.
//
// Only state-changing commands go in. Logging a GET would be harmless on
// replay but would grow the file with reads, which on a read-heavy cache is
// nearly all of the traffic.
//
// The command recorded is not always the one the client sent -- see the SET
// case, which rewrites a relative expiry into an absolute one.
func (s *Server) record(cmd resp.Command) error {
	if s.log == nil {
		return nil
	}
	return s.log.Append(cmd)
}

const errInvalidExpiry = "ERR invalid expire time in 'set' command"

// parseExpiry turns a SET option pair like ("EX", "10") into the absolute
// instant the key should die.
//
// It returns an instant rather than a duration on purpose. A relative expiry
// only means anything at the moment it was issued: "EX 3600" recorded in the
// append-only log and replayed three hours later would grant a fresh hour to a
// key that should already be gone, and enough restarts would make TTL'd keys
// immortal. Resolving to an absolute deadline here means the value written to
// the log means the same thing whenever it is read.
//
// The EXAT and PXAT forms exist for exactly that: they carry a Unix timestamp,
// and they are what the log stores.
//
// The returned string is a RESP error message rather than a Go error because
// these strings are part of the wire contract -- ADR-003 makes real redis-cli
// the correctness bar, and clients do match on Redis's exact error text. An
// empty string means the parse succeeded.
func parseExpiry(unit, value string, now time.Time) (time.Time, string) {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, "ERR value is not an integer or out of range"
	}

	var (
		scale    time.Duration
		absolute bool
	)
	switch strings.ToUpper(unit) {
	case "EX":
		scale = time.Second
	case "PX":
		scale = time.Millisecond
	case "EXAT":
		scale, absolute = time.Second, true
	case "PXAT":
		scale, absolute = time.Millisecond, true
	default:
		return time.Time{}, "ERR syntax error"
	}

	// Both forms multiply by scale to reach nanoseconds, and time.Duration is
	// an int64 count of those -- roughly 292 years' worth. Past that the
	// multiplication wraps negative, which for a relative expiry would hand
	// back an already-dead key to a client that asked for a very long-lived
	// one. Silently doing the opposite of the request is worse than refusing.
	if n > int64(math.MaxInt64)/int64(scale) || n < int64(math.MinInt64)/int64(scale) {
		return time.Time{}, errInvalidExpiry
	}

	if absolute {
		// A timestamp in the past is legal and means the key is already
		// expired -- which is precisely the case a replayed log produces when
		// a deadline passed during the outage.
		//
		// Note this instant carries no monotonic reading, so comparisons
		// against it use the wall clock. That is inherent: an absolute
		// timestamp is a wall-clock idea. Relative expiries below keep their
		// monotonic reading and so stay immune to the clock being stepped.
		return time.Unix(0, 0).Add(time.Duration(n) * scale), ""
	}
	if n <= 0 {
		// Zero and negative relative expiries are rejected rather than treated
		// as "delete immediately", matching Redis.
		return time.Time{}, errInvalidExpiry
	}
	return now.Add(time.Duration(n) * scale), ""
}
