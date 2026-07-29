# Progress Log

Read this first at the start of every session — it restores context that the
code alone does not carry.

---

## Current state
**Week 1 — in progress.** TCP listener accepts clients (one goroutine each) and
the RESP request parser is done and tested. The server still sends no replies,
so `redis-cli` connects and hangs waiting.

- Go 1.26.5 (darwin/arm64)
- redis-cli / redis-server 8.8.1 — the real tools, kept as the correctness oracle
- Remote: <https://github.com/hossam-04/redis-clone> (public)

> Note: real Redis is installed but **not** running as a service. Starting it
> (`brew services start redis`) would bind port 6379 and block our own server.
> Run it on another port when a side-by-side comparison is needed.

## Plan
| Week | Goal | Done |
|------|------|------|
| 1 | TCP listener, RESP parser, `PING` / `ECHO` / `SET` / `GET`. Bar: `redis-cli` connects. | ☐ |
| 2 | Concurrent clients, key expiry (`PX` / `EX`), LRU eviction under memory pressure. | ☐ |
| 3 | Append-only log, replay on startup, survive `kill -9` with no data loss. | ☐ |
| 4 | Benchmark with `redis-benchmark`, record throughput + p99, write the README. | ☐ |

## Next up
Reply encoding (the write path), then command dispatch, so `redis-cli -p 6379
PING` returns `PONG` and Week 1's bar is met.

## Open questions
*(none yet)*

## Session log
- **Session 1** — Created repo, `CLAUDE.md`, `DECISIONS.md` (ADR 001–003).
  Chose Go over Python. Blocked on toolchain install.
- **Session 2** — Cleared the toolchain blocker: installed Go, Redis, and `gh`.
  Published the repo to GitHub. Week 0 closed; TCP framing is the next problem.
- **Session 3** — TCP listener, then the RESP request parser (ADR-004). Key
  idea: the parser takes an `io.Reader`, not a `net.Conn`, so tests can deliver
  bytes one at a time and prove framing works without touching a network.
  24 tests green.
