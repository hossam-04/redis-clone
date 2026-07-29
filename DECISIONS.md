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
