# Architecture Decision Records

Format: **Decision** / *Alternatives considered* / **Why** / *Would revisit if…*

Newest last. Each entry should be readable a month later without the code open.

---

## ADR-001 — Go as the implementation language

**Decision:** Build the server in Go.

*Alternatives:* Python (already installed, zero setup), Rust (best performance
story), C (closest to real Redis).

**Why:** Goroutines make the "one connection per client" concurrency model
readable rather than a callback maze, and the standard library ships a
production-grade `net` package. Go also compiles to a single binary, so anyone
reviewing this repo can run it without a toolchain. Rust was rejected as
learning the borrow checker at the same time as network programming would mean
learning neither well.

*Would revisit if:* the goal shifted from learning systems concepts to squeezing
maximum throughput, where Rust or C would win.

---

## ADR-002 — Hand-roll the core, import nothing interesting

**Decision:** The RESP parser, concurrency model, eviction policy, and
persistence layer are all written from scratch. Standard library only.

*Alternatives:* Use an existing RESP parsing library and an off-the-shelf LRU
package, then focus on assembling them.

**Why:** In a production job, importing a battle-tested parser is the correct
call. Here the parser *is* the deliverable — the point is understanding buffer
boundaries and protocol framing, not shipping fast. Importing the interesting
parts would leave a repo that looks finished and teaches nothing.

*Would revisit if:* this ever became a real project intended for real traffic.

---

## ADR-003 — Compatibility with real `redis-cli` as the correctness bar

**Decision:** Success is defined as the official `redis-cli` and
`redis-benchmark` tools working against this server unmodified.

*Alternatives:* Write a custom client alongside the server and test against that.

**Why:** A self-written client would share my own misunderstandings of the
protocol — if I frame a message wrong, my client would parse it wrong in exactly
the same way and the tests would pass. The real client is an independent judge
that fails loudly on a single misplaced byte.

*Would revisit if:* never. This constraint is the backbone of the project.

---

## ADR-004 — `bufio.Reader` for buffering, hand-rolled framing

**Decision:** Buffer socket reads with the standard library's `bufio.Reader`.
Every byte of RESP framing logic is still written from scratch.

*Alternatives:* Hand-roll the fill/drain/compact buffer; or call `conn.Read`
directly with no buffering at all.

**Why:** ADR-002 bans libraries from doing the *interesting* part, and the
interesting part here is framing — knowing where one command ends and the next
begins. `bufio.Reader` has no idea RESP exists; it is a byte buffer that
amortises syscalls, nothing more. Without it a parser that pulls a few bytes at
a time would issue one `read` syscall per pull at roughly 1–2µs each, which
would dominate the runtime and teach nothing in exchange. Hand-rolling the
buffer would mainly teach ring-buffer compaction off-by-ones, which is a
different subject than this project.

*Would revisit if:* profiling shows `bufio`'s copying is a real bottleneck, or
the learning goal shifts to buffer management specifically.

---

## ADR-005 — Flush replies only when the read buffer is drained

**Decision:** Call `Flush` when `bufio.Reader.Buffered() == 0`, not after every
command.

*Alternatives:* Flush after every command (simple, obviously correct); flush
every N commands, or on a timer.

**Why:** `Flush` is a `write` syscall. One per reply is pure waste when a client
has already pipelined a burst — we can answer the whole burst with a single
write. Measured on this machine: ~90k ops/sec flushing per command, ~1.5M at
pipeline depth 16. Count- or timer-based batching was rejected because it
breaks the simple case: a client that sends one command and waits would sit
there until the batch filled or the timer expired. An empty read buffer is the
precise moment we are about to block, and flushing before blocking is exactly
the guarantee needed — no client ever waits on a reply still sitting in our
buffer.

*Would revisit if:* a client sends a partial command and then waits on replies
to earlier ones. `Buffered() != 0` would suppress the flush while `ReadCommand`
blocks on the remainder. No real client behaves this way, and the only victim
is the connection that did it, but it is the known soft spot in this rule.

---

## ADR-006 — Three internal packages, dependencies pointing one way

**Decision:** Split the code into `internal/resp` (wire protocol),
`internal/store` (data), and `internal/server` (the loop joining them), with
`main.go` at the repo root.

*Alternatives:* Stay flat in `package main`; or adopt the fuller
`cmd/<binary>/` convention.

**Why:** The dependency graph is the point. `resp` and `store` import nothing
of ours and do not know each other exists; `server` is the only package that
imports both. That is what already makes the parser testable against a
`strings.Reader` and the store testable without a socket, and it is now
enforced by the compiler rather than by discipline — `readLine` and
`readBulkString` are unexported, so protocol internals are genuinely
unreachable from command handling. As milestones 2–4 add expiry, eviction, and
persistence, that boundary is what stops the store from growing opinions about
RESP.

`internal/` on top of that means nothing outside this module can import any of
it, so there is no accidental public API to keep stable.

`cmd/` was rejected because the nesting only pays for itself when a repo ships
several binaries. This one ships a single server, and the extra directory would
change the documented run command in exchange for nothing.

*Would revisit if:* the project grows a second binary — a CLI, or a custom
benchmark harness — at which point `cmd/` starts earning its keep.

---

## ADR-007 — Expiry is lazy *and* active, and the active half is probabilistic

**Decision:** Delete expired keys when they are read (lazy), and additionally
sweep a bounded random sample every 100ms (active). Each sweep round inspects
20 keys drawn from an index of keys that carry a deadline, repeats immediately
while more than 25% of a sample comes back expired, and stops after 16 rounds.

*Alternatives:* Lazy only. Active only, with a full scan. One timer per key.

**Why:** Each half is broken alone.

Lazy alone leaks. A key written with a TTL and never read again is never
noticed, so a session cache whose users do not come back grows without bound —
the memory is unreachable but still resident, which is the worst kind.

Active alone cannot afford to be correct. Deleting requires the write lock, so
a full scan of ten million keys holds it for the whole walk and stalls every
client in the server. Not a background cost — a server-wide latency spike, on a
schedule.

One timer per key was rejected outright: a goroutine and a runtime timer per
key costs more than the data being stored.

Sampling threads the needle by **giving up completeness**. It never claims to
have found every expired key, only to keep the expired-but-resident population
bounded, which is all that memory actually requires. The 25% threshold is the
load-bearing number: repeating whenever *any* sampled key is expired would hit
the 16-round cap almost constantly — with just 5% of keys expired, the chance
that one of 20 samples is expired is about 64% — holding the lock sixteen times
longer to reclaim nearly nothing.

The volatile index exists because sampling without it is useless at realistic
ratios. A cache of a million keys with ten thousand TTLs would spend roughly
199 samples in every 200 looking at keys that cannot expire. The index holds
names only; deadlines stay in the entry so `Get` still needs one lookup.

*Would revisit if:* a workload needs keys to disappear at a precise instant.
Sampling is eventually-consistent by construction — a key can outlive its TTL
in memory, though never in a reply, since `Get` checks the deadline before
answering.

---

## ADR-008 — Approximate LRU: sampled eviction, atomic access stamps

**Decision:** Evict by stamping every entry with a logical clock value on
access and, when over the memory limit, deleting the oldest of a small random
sample (5 by default, `--maxmemory-samples`). Entries are stored as pointers so
the stamp can be written atomically under the *read* lock.

*Alternatives:* Exact LRU via a hash map plus intrusive doubly-linked list.
Full scan for the true minimum. Random eviction.

**Why:** The textbook exact-LRU structure is a hash map with a linked list,
moving a node to the front on every read. It is O(1) and it is wrong here:
moving a node mutates shared structure, so every `GET` would need the write
lock, and reads across the entire server would serialise. LRU would cost all
the read concurrency the `RWMutex` exists to provide.

Scanning for the true minimum instead is O(n) per eviction, and eviction fires
under memory pressure — precisely when the server can least afford a full walk
holding the write lock.

Sampling gives up exactness and buys back both. Measured on this server, with a
20-key working set read every round while 800 cold keys stream past a ~100-key
budget:

| `maxmemory-samples` | hot keys retained |
|---------------------|-------------------|
| 1                   | 0 / 20            |
| 2                   | 0 / 20            |
| 5 (default)         | 10 / 20           |
| 10                  | 18 / 20           |
| 20                  | 20 / 20           |
| 50                  | 20 / 20           |

A sample of 1 is random eviction and behaves like it. The curve is steep early
and flat after about 20, which is why the knob exists and why 5 is a
defensible default rather than an obviously correct one.

Keeping the stamps cheap needed one more thing: `map[string]*entry` rather than
`map[string]entry`. With values, updating a field means assigning the slot
back, which is a map write, which needs the write lock. With pointers the map
never changes on a read — only the struct it points at, through an
`atomic.Uint64`. Mutating the contents is not mutating the container. The cost
is a pointer and an allocation per entry.

The memory limit itself is an estimate: key bytes plus value bytes plus a flat
96-byte constant. Go offers no cheap way to ask what a map entry truly costs,
and the honest answer needs `runtime.ReadMemStats`, which stops the world and
so cannot run per `SET`. Real RSS is higher. That is acceptable because
eviction needs a number that moves in proportion to usage, not an accurate one
— whereas a key-count limit would ignore value size entirely and so would not
measure the thing eviction exists to control.

*Would revisit if:* eviction quality at the default sample size proves too poor
for a real workload. Redis's answer is an eviction pool that carries the best
candidates across samplings, which recovers much of the accuracy without
raising the per-eviction cost.

---

## ADR-009 — Persistence is an append-only log of RESP commands

**Decision:** Append every state-changing command to a file, in the same RESP
form clients send, and replay it on startup. Flush the log to the kernel before
replying to the client. `fsync` on a policy: `always`, `everysec` (default), or
`no`.

*Alternatives:* Periodic snapshots of the whole keyspace (Redis's RDB). A custom
binary log format. Logging the resulting state rather than the command.

**Why the same format clients use:** replay hands the file straight to
`ReadCommand`. There is no second parser, so there is no second parser to keep
in step — a log format of its own would be free to drift away from the wire
format, and the drift would only show up during recovery, which is the worst
possible time to discover a bug. It also means anything a client can say, the
log can hold, for free.

**Why append rather than snapshot:** an append is O(1) per write and loses at
most the tail on a crash. A snapshot has to walk the entire keyspace, so its
cost scales with the data rather than the change, and everything written between
snapshots is lost. Real Redis offers both; the log is the one that teaches
durability.

**Why flush before replying:** a reply is a promise that the command was
accepted. Sending it while the command still sits in this process's buffer means
`kill -9` loses a write we already confirmed. Flushing first makes every
acknowledgement the client ever saw correspond to bytes the kernel holds. It
rides the same batching rule as ADR-005, so a pipelined burst costs one log
write rather than one per command.

**Why fsync is a separate dial:** `write` and "the data is on the disk" are
different claims. `write` hands bytes to the kernel, which acknowledges them and
passes them to the drive whenever convenient. `fsync` forces the issue and
waits, at milliseconds against microseconds — a thousandfold difference, which
is why it cannot be unconditional.

That yields two failure modes, and they are not equally severe:

| Failure | What dies | Survives with |
|---|---|---|
| `kill -9` | the process only | `write` — the kernel is still there |
| Power loss | the machine | `fsync` |

Our stated bar is `kill -9`, so flushing suffices for it and `fsync` addresses
the harder case. `everysec` is the default, and it is worth being blunt about
what it concedes: **a client can receive `+OK` for a write that a power failure
then erases**, up to a second's worth. That is a real lie about durability, and
choosing the default means choosing to tell it.

*Would revisit if:* the log's unbounded growth becomes the binding problem
before power-loss durability does. There is no compaction yet — a key written a
million times produces a million records, and replay time grows with total
writes rather than with data size. Redis's answer is `BGREWRITEAOF`, which
rewrites the log as the shortest command sequence that reproduces current state.

---

## ADR-010 — The log records absolute deadlines, never relative TTLs

**Decision:** A `SET` carrying `EX`/`PX` is written to the log as `SET key value
PXAT <unix-millis>`. `parseExpiry` resolves every expiry form to an instant
before anything else sees it.

*Alternatives:* Log the command verbatim. Log a separate expiry record
alongside it.

**Why:** a relative expiry only means anything at the moment it was issued.
Recorded verbatim:

```
14:00   SET session data EX 3600     → logged as "EX 3600"
14:30   crash
17:00   restart, replay
```

Replaying `EX 3600` at 17:00 grants a fresh hour to a key that should have died
at 15:00. Worse, it compounds: every restart renews every TTL, so on a server
that restarts periodically, TTL'd keys become effectively immortal — and the
symptom is unbounded memory growth in a cache that looks correctly configured.

Resolving to an instant at `SET` time makes the recorded value mean the same
thing whenever it is read. A deadline already in the past is accepted rather
than rejected, because that is exactly what replay produces when a TTL lapsed
during the outage: the key is restored already dead and the sweeper reclaims it.

One consequence worth naming: an absolute timestamp carries no monotonic clock
reading, so comparisons against it use the wall clock. That is inherent — an
absolute instant is a wall-clock idea — and it means a replayed deadline is
vulnerable to the system clock being stepped in a way a live relative TTL is
not. Verified empirically: a key set with `EX 3` before a `kill -9` and a
five-second outage comes back gone, not renewed.

*Would revisit if:* never for the log. This is not a preference, it is a
correctness requirement.

---

## ADR-011 — A torn tail is recovered; damage anywhere else refuses to start

**Decision:** On replay, a partial command at the end of the log is truncated
away and the server starts. Malformed bytes anywhere else are a fatal error.

*Alternatives:* Always refuse on any parse failure. Always skip bad records and
continue. Truncate at the first bad byte wherever it is.

**Why:** a partial record at the tail is not a bug, it is **the normal signature
of a crash**. A process killed mid-append leaves a prefix of the last record,
every time. Refusing to start on it would mean never recovering from precisely
the event this log exists to survive.

Truncating is also necessary rather than merely tolerant: leaving the fragment
in place would put the next append after garbage, converting a recoverable tail
into corruption in the middle of the file.

Damage elsewhere is a different claim about the world. Appends are sequential
and `O_APPEND` never seeks, so a crash **cannot** produce a bad record followed
by good ones. If that is what the file contains, something we do not understand
has altered it, and starting anyway would silently serve wrong data. The file is
left untouched in that case, since we cannot tell good bytes from bad and
truncating would destroy evidence.

The tail is identified by **byte offset, not by error kind** — a subtlety worth
recording. A record cut off inside a header line surfaces as plain `io.EOF`,
indistinguishable from a clean end of file. Trusting the error would leave the
fragment on disk. So replay tracks the offset just past the last command that
applied, and compares.

*Would revisit if:* crashes start producing zero-filled tails rather than short
ones, which some filesystems do. Those parse as malformed rather than short and
would currently be treated as unexplained corruption.
