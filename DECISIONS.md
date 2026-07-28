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
