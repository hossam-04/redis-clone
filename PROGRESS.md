# Progress Log

Read this first at the start of every session — it restores context that the
code alone does not carry.

---

## Current state
**Milestone 2 — done.** Keys expire two ways: on read (lazy) and via a bounded
sampling sweeper on a 100ms tick (active). `SET k v EX 10`, `PX`, and `TTL`'s
three states all work against real `redis-cli`; 200 keys with `EX 1` that
nothing ever reads drop out of `DBSIZE` on their own within about a second.

Eviction holds a memory ceiling by approximating LRU: entries carry an atomic
access stamp written under the *read* lock, and eviction deletes the oldest of
a small random sample. 2000 writes into a ~100-key budget settle at 100 keys.

Milestone 1 delivered the listener, the RESP parser and encoder, dispatch, and
the mutex-guarded store.

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
| 3 | Append-only log, replay on startup, survive `kill -9` with no data loss. | ☐ |
| 4 | Benchmark with `redis-benchmark`, record throughput + p99, write the README. | ☐ |

## Next up
Milestone 3: append-only persistence and crash recovery. Bar is `kill -9` with
no data loss. First design question is what "no data loss" can even mean given
that a write returning from the kernel is not a write on the disk — which is
the difference between `write` and `fsync`, and the reason durability is a
tunable rather than a yes/no.

## Open questions
- `SET` still ignores `NX` / `XX` / `KEEPTTL`; only `EX` and `PX` are handled.
- No `PTTL`, `PERSIST`, `EXPIRE`, or `DEL` yet. `PTTL` is a few lines given
  `Store.TTL` already returns a duration.
- No `FLUSHALL`, which makes test scripts carry state between sections.
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
