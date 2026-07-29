# redis-clone

A Redis-compatible server written from scratch in Go — no third-party
dependencies. The official `redis-cli` and `redis-benchmark` tools connect to it
unmodified.

> **Status:** in development. See [PROGRESS.md](PROGRESS.md) for the current
> milestone and [DECISIONS.md](DECISIONS.md) for the design rationale.

## Why

Redis is small enough to understand end to end but touches most of the systems
concepts that matter: protocol design, TCP framing, concurrency, memory
management, and crash-safe persistence. Building it against the real client
means correctness is judged by an independent tool rather than by my own tests.

## Features

- [x] RESP request parsing — length-prefixed framing that survives a command
      split across any number of TCP packets, verified byte-at-a-time in tests
- [x] RESP reply encoding — including the null/empty-string distinction that
      lets a cache tell a miss from a stored empty value
- [x] `PING`, `ECHO`, `SET`, `GET`, `TTL`, `DBSIZE`
- [x] Concurrent client handling — one goroutine per connection, shared store
      behind an `RWMutex`, verified under `-race`
- [x] Key expiry (`EX` / `PX`) — lazy deletion on read, plus a bounded
      sampling sweeper for keys nobody ever reads
- [x] LRU eviction under memory pressure — approximate, by sampling, so reads
      stay concurrent and eviction stays O(1)
- [ ] Append-only persistence with crash recovery

## Running

```bash
go run . --port 6379

# with a memory ceiling, above which approximately-LRU keys are evicted
go run . --port 6379 --maxmemory 64000000 --maxmemory-samples 10
```

Then, in another terminal:

```bash
redis-cli -p 6379 PING
# PONG
```

## Testing

```bash
go test ./...
```

## Benchmarks

Preliminary smoke numbers only — `redis-benchmark -t set,get -n 20000` on an
M-series Mac, client and server on loopback. **Not yet a real result:** there is
no comparison against real Redis, no p99, and no persistence in the write path,
all of which land in Milestone 4.

| Pipeline depth | Throughput |
|----------------|------------|
| none           | ~90–97k ops/sec |
| 16             | ~1.5M ops/sec   |
| 64             | ~2.2–4.0M ops/sec |

The gap between the first row and the rest is one `if` statement: replies are
flushed only once the read buffer is drained, so a pipelined burst costs a
single `write` syscall rather than one per command. See ADR-005.

### Eviction accuracy

Eviction approximates LRU by sampling rather than tracking an exact order, so
it is worth knowing what that costs. A 20-key working set is read every round
while 800 cold keys stream past a budget of roughly 100 keys — the store turns
over about eight times. A perfect LRU keeps all 20.

| `maxmemory-samples` | hot keys retained |
|---------------------|-------------------|
| 1                   | 0 / 20            |
| 2                   | 0 / 20            |
| 5 (default)         | 10 / 20           |
| 10                  | 18 / 20           |
| 20                  | 20 / 20           |
| 50                  | 20 / 20           |

A sample of 1 is random eviction and performs like it. The curve is steep early
and flat past about 20 — which is why the knob is exposed rather than hardcoded.
See ADR-008 for why exact LRU was rejected.

## Layout

```
main.go                  flags, listener, wiring — and nothing else
internal/
  resp/                  the wire protocol; knows nothing about storage
    reader.go            ReadCommand + unexported framing helpers
    writer.go            reply encoding, including null vs empty string
  store/                 the data; knows nothing about the protocol
    store.go
  server/                the only package that imports both
    server.go            accept loop, connection handling, the flush rule
    command.go           command dispatch
```

Dependencies run one way — `server → resp` and `server → store`, with `resp`
and `store` unaware of each other:

```
        server
        ╱     ╲
     resp     store
```

That is why the parser can be tested against a `strings.Reader` with no server
running, and the store against no socket at all. See ADR-006.

## Design notes

*Architecture walkthrough and tradeoffs to follow as the implementation lands.
Decision history lives in [DECISIONS.md](DECISIONS.md).*
