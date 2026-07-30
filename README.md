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
- [x] Append-only persistence with crash recovery — `kill -9` loses nothing,
      a torn log tail is repaired on startup, TTLs keep running while down

## Running

```bash
go run . --port 6379

# with a memory ceiling, above which approximately-LRU keys are evicted
go run . --port 6379 --maxmemory 64000000 --maxmemory-samples 10

# with persistence: writes are logged and replayed on startup
go run . --port 6379 --appendonly --appendfsync everysec
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

Apple M1 Pro, 8 cores, 16 GB · Go 1.26.5 · Redis 8.8.1 · both servers on the
same machine, `-c 50 -r 10000`, median of 3 runs after a discarded warm-up.

**Caveats first,** because a benchmark without them is marketing. The client and
both servers share one laptop and compete for the same cores. `redis-benchmark`
is closed-loop, so it stops sending when the server stalls, which systematically
*understates* tail latency. Run-to-run variance on an identical configuration is
around 10%. Real Redis is single-threaded C with its own allocator and no GC —
the goal here is to understand the gap, not to win.

### In memory, no persistence

| | ours | Redis | ours/Redis |
|---|---|---|---|
| GET, no pipelining | 95,420/s | 108,225/s | 88% |
| GET, pipeline 16 | 1,470,588/s | 1,515,152/s | 97% |
| SET, no pipelining | 95,969/s | 108,460/s | 88% |
| SET, pipeline 16 | **1,190,476/s** | 1,063,830/s | **112%** |

We beat Redis on pipelined `SET`, and the reason is ADR-001 paying off three
milestones later: Redis uses one core, our goroutine-per-connection model uses
eight. Under enough concurrent pipelined load, parallelism beats C.

It is not free. On the same workload:

```
p99, SET pipeline 16:    ours 2.159ms    Redis 0.967ms
```

**Higher throughput, 2.2× worse tail.** That trade is why these are measured at
p99 and not on the average, which would have hidden it entirely.

### Cost of durability (`SET`, no pipelining)

| | ours | Redis |
|---|---|---|
| `appendonly` off | 95,602/s | 122,549/s |
| `appendfsync everysec` | 88,968/s | 122,249/s |
| `appendfsync always` | 4,327/s | 4,926/s |

`fsync` is a ~10ms round trip on this hardware, and `always` pays it per batch.
That single row is the entire reason durability is a dial.

### What benchmarking actually found

Two things, and only one of them was the thing we predicted.

**Group commit — a real flaw, found and fixed.** `appendfsync always` first
measured at **12% of Redis unpipelined and 5% pipelined**. Fifty clients meant
fifty `fsync` calls, where single-threaded Redis necessarily batches all fifty
into one. Making clients wait on a shared sync rather than issue their own:

```
SET, always, no pipelining:     557/s  →   4,327/s     (7.8×)
SET, always, pipeline 16:     3,855/s  →  61,069/s    (15.8×)
```

See ADR-012.

**A refuted hypothesis, kept on the record.** We predicted the `everysec` tail
came from holding the log mutex across `fsync`. The fix was correct on its own
terms and changed nothing (2.887ms → 2.951ms). A concurrency scan then showed
the tail is contention, not a periodic stall — identical to Redis at one client,
4.8× worse at fifty. See ADR-013 for the real diagnosis and why the wrong guess
is worth writing down.

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

## Durability

Writes go to an append-only log of RESP commands, replayed on startup. What
that log guarantees depends on `--appendfsync`, and the distinction is the whole
subject:

| | `kill -9` | Power loss |
|---|---|---|
| `always` | no loss | no loss |
| `everysec` *(default)* | no loss | up to ~1s of acknowledged writes |
| `no` | no loss | whatever the kernel had not written |

Every setting survives `kill -9`, because `kill -9` only kills the process — the
kernel still holds the bytes and writes them out. `fsync` is what protects
against the machine itself dying, and it costs milliseconds against microseconds
for a plain write.

So `everysec` is making a real concession: **a client can be told `+OK` for a
write that a power cut then erases.** That is Redis's default too, and it is a
defensible trade, but it is a trade rather than a free lunch.

Two properties that are less obvious than they look:

- **Replies never outrun the log.** The log is flushed to the kernel before any
  reply is sent, so every acknowledgement the client ever saw corresponds to
  bytes the kernel is holding. See ADR-009.
- **TTLs keep running while the server is down.** The log records absolute
  deadlines, not the relative `EX 3600` a client sent — otherwise every restart
  would renew every TTL and TTL'd keys would become immortal. See ADR-010.

A log ending mid-command is treated as a crash rather than corruption: it is
truncated to the last complete command and the server starts. Malformed bytes
anywhere *but* the tail cannot be explained by a crash, so the server refuses to
start rather than serve data it cannot vouch for. See ADR-011.

**Not implemented:** log compaction. The log grows with total writes, not with
data size, so a key written a million times costs a million records and replay
time to match. Redis solves this with `BGREWRITEAOF`.

## Design notes

*Architecture walkthrough and tradeoffs to follow as the implementation lands.
Decision history lives in [DECISIONS.md](DECISIONS.md).*
