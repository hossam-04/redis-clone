# Progress Log

Read this first at the start of every session — it restores context that the
code alone does not carry.

---

## Current state
**Week 0 — environment setup.** No application code written yet.

## Plan
| Week | Goal | Done |
|------|------|------|
| 1 | TCP listener, RESP parser, `PING` / `ECHO` / `SET` / `GET`. Bar: `redis-cli` connects. | ☐ |
| 2 | Concurrent clients, key expiry (`PX` / `EX`), LRU eviction under memory pressure. | ☐ |
| 3 | Append-only log, replay on startup, survive `kill -9` with no data loss. | ☐ |
| 4 | Benchmark with `redis-benchmark`, record throughput + p99, write the README. | ☐ |

## Next up
Install Go and Redis via Homebrew, then the first design decision: how to frame
incoming bytes when a client's command arrives split across multiple TCP packets.

## Open questions
*(none yet)*

## Session log
- **Session 1** — Created repo, `CLAUDE.md`, `DECISIONS.md` (ADR 001–003).
  Chose Go over Python. Blocked on toolchain install.
