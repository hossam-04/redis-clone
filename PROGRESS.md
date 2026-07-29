# Progress Log

Read this first at the start of every session — it restores context that the
code alone does not carry.

---

## Current state
**Milestone 1 — done.** Real `redis-cli` connects and runs `PING`, `ECHO`,
`SET`, and `GET` against this server unmodified. Read path, write path,
dispatch, and a mutex-guarded store are all in place; 20 concurrent clients run
clean under `-race`.

- Go 1.26.5 (darwin/arm64)
- redis-cli / redis-server 8.8.1 — the real tools, kept as the correctness oracle
- Remote: <https://github.com/hossam-04/redis-clone> (public)

> Note: real Redis is installed but **not** running as a service. Starting it
> (`brew services start redis`) would bind port 6379 and block our own server.
> Run it on another port when a side-by-side comparison is needed.

## Plan
| Milestone | Goal | Done |
|-----------|------|------|
| 1 | TCP listener, RESP parser, `PING` / `ECHO` / `SET` / `GET`. Bar: `redis-cli` connects. | ☑ |
| 2 | Concurrent clients, key expiry (`PX` / `EX`), LRU eviction under memory pressure. | ☐ |
| 3 | Append-only log, replay on startup, survive `kill -9` with no data loss. | ☐ |
| 4 | Benchmark with `redis-benchmark`, record throughput + p99, write the README. | ☐ |

## Next up
Milestone 2: key expiry (`EX` / `PX`) and LRU eviction. First design question is
whether expired keys are removed eagerly on a timer or lazily on access, and
what that choice costs in memory versus CPU.

## Open questions
- `SET` ignores `EX` / `PX` / `NX` / `XX` and rejects them as a wrong argument
  count. Milestone 2 fixes this.
- One `RWMutex` covers the whole map, so every writer in the server serialises.
  Fine at current scale; revisit if benchmarks show lock contention.
- No `CONFIG` command, so `redis-benchmark` prints "Could not fetch server
  CONFIG" and falls back to defaults. Harmless, but a stub may be worth it.

## Session log
- **Session 1** — Created repo, `CLAUDE.md`, `DECISIONS.md` (ADR 001–003).
  Chose Go over Python. Blocked on toolchain install.
- **Session 2** — Cleared the toolchain blocker: installed Go, Redis, and `gh`.
  Published the repo to GitHub. Milestone 0 closed; TCP framing is next.
- **Session 3** — TCP listener, then the RESP request parser (ADR-004). Key
  idea: the parser takes an `io.Reader`, not a `net.Conn`, so tests can deliver
  bytes one at a time and prove framing works without touching a network.
  24 tests green.
- **Session 4** — Reply encoding, command dispatch, mutex-guarded store.
  Milestone 1 bar met: real `redis-cli` works unmodified. Flush rule (ADR-005)
  measured at 16x throughput under pipelining versus flushing per command. Then
  split the flat package into `internal/{resp,store,server}` (ADR-006) and
  added store and dispatch tests, which the package boundary made
  straightforward to write.
