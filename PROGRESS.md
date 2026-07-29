# Progress Log

Read this first at the start of every session — it restores context that the
code alone does not carry.

---

## Current state
**Milestone 3 — done.** Writes are appended to a log of RESP commands and
replayed on startup. Verified by actually doing it: `kill -9` the server with
six keys set, restart, all six come back — including an empty-string value, a
value containing CRLF, and live TTLs. A log chopped mid-command is repaired on
startup (31 trailing bytes discarded, server starts); a log damaged in the
middle makes the server exit 1 rather than serve data it cannot vouch for.

TTLs keep running while the server is down, because the log records absolute
deadlines rather than the relative `EX` a client sent. A key set `EX 3` before a
`kill -9` and a five-second outage comes back gone, not renewed.

Milestone 2 delivered lazy plus active expiry and approximate-LRU eviction under
a memory ceiling. Milestone 1 delivered the listener, the RESP parser and
encoder, dispatch, and the mutex-guarded store.

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
| 2 | Concurrent clients, key expiry (`PX` / `EX`), LRU eviction under memory pressure. | ☑ |
| 3 | Append-only log, replay on startup, survive `kill -9` with no data loss. | ☑ |
| 4 | Benchmark with `redis-benchmark`, record throughput + p99, write the README. | ☐ |

## Next up
Milestone 4: benchmark with `redis-benchmark`, record throughput and p99 against
real Redis on the same machine, and finish the README. Things already known to
be worth measuring: the `clock.Add(1)` on every read is a single contended cache
line, and `--appendfsync always` should show the millisecond cost of `fsync`
directly.

## Open questions
- **No log compaction.** The log grows with total writes, not data size, so a
  key written a million times costs a million records and replay time to match.
  This is the biggest gap in milestone 3; Redis answers it with `BGREWRITEAOF`.
- `SET` still ignores `NX` / `XX` / `KEEPTTL`; `EX` / `PX` / `EXAT` / `PXAT` are
  handled.
- No `PTTL`, `PERSIST`, `EXPIRE`, or `DEL` yet. Note `DEL` matters more than it
  looks: without it there is no way to record a deletion in the log.
- No `FLUSHALL`, which makes test scripts carry state between sections.
- Replay reuses `dispatch`, which writes error replies rather than returning
  errors, so a command the log contains but the server rejects is skipped
  silently. Only commands we wrote ourselves are ever in there, so this is
  currently unreachable, but it is a gap rather than a guarantee.
- A crash leaving a zero-filled tail rather than a short one — which some
  filesystems produce — would parse as corruption rather than a torn tail, and
  the server would refuse to start.
- One `RWMutex` covers the whole map, so every writer in the server serialises,
  and the sweeper is a writer ten times a second. Fine at current scale;
  revisit if benchmarks show contention.
- `DBSIZE` counts expired-but-unreclaimed keys, so it can briefly exceed the
  number of readable keys. That is deliberate — it is what makes the sweeper
  observable — but it differs from what a client might assume.
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
- **Session 5** — Key expiry, both halves (ADR-007). Lazy deletion on read
  needs a read-lock-to-write-lock gap that Go cannot do atomically, so `Get`
  re-reads everything after the gap; without that it would delete a value a
  concurrent client had just written. Active expiry samples 20 keys per round
  off a volatile index and repeats while a sample is >25% expired, capped at
  16 rounds. Tests drive a swappable clock rather than sleeping.
- **Session 6** — Approximate LRU eviction (ADR-008). The load-bearing change
  was `map[string]entry` to `map[string]*entry`: with values, stamping an
  access means assigning the map slot back, which needs the write lock and
  serialises every read in the server. With pointers the map is untouched and
  only an atomic field inside the entry changes. Measured the accuracy the
  sampling costs — 10/20 of a working set retained at 5 samples, 20/20 at 20.
- **Session 7** — Append-only persistence (ADR-009/010/011). The log stores
  commands in the same RESP form clients send, so replay reuses `ReadCommand`
  and no second format can drift from the first. Two non-obvious parts: the log
  is flushed to the kernel *before* the reply is sent, so an acknowledgement
  never outruns durability; and relative expiries are resolved to absolute
  deadlines before logging, since `EX 3600` replayed later would renew a TTL
  that should have lapsed and enough restarts would make TTL'd keys immortal.
  A torn tail is detected by byte offset rather than error kind, because a
  record cut inside a header line looks exactly like a clean end of file.
