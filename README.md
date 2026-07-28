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

- [ ] RESP protocol parser (handles commands split across TCP packets)
- [ ] `PING`, `ECHO`, `SET`, `GET`
- [ ] Concurrent client handling
- [ ] Key expiry (`EX` / `PX`)
- [ ] LRU eviction under memory pressure
- [ ] Append-only persistence with crash recovery

## Running

```bash
go run . --port 6379
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

*To be filled in Week 4 — throughput and p99 latency measured with
`redis-benchmark`, compared against real Redis on the same machine.*

## Design notes

*Architecture walkthrough and tradeoffs to follow as the implementation lands.
Decision history lives in [DECISIONS.md](DECISIONS.md).*
