# Progress Log

Read this first at the start of every session — it restores context that the
code alone does not carry.

---

## Current state
**Week 0 — environment setup, complete.** Toolchain installed and the repo is
pushed. No application code written yet.

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
First design decision: how to frame incoming bytes when a client's command
arrives split across multiple TCP packets.

## Open questions
*(none yet)*

## Session log
- **Session 1** — Created repo, `CLAUDE.md`, `DECISIONS.md` (ADR 001–003).
  Chose Go over Python. Blocked on toolchain install.
- **Session 2** — Cleared the toolchain blocker: installed Go, Redis, and `gh`.
  Published the repo to GitHub. Week 0 closed; TCP framing is the next problem.
