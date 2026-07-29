# Redis Clone — Learning Project

## What this is

A Redis-compatible server built to learn systems programming. Real `redis-cli`
must be able to connect to it.

**In scope:** RESP protocol, TCP concurrency, key expiry, LRU eviction,
append-only persistence, crash recovery, benchmarking.
**Out of scope:** clustering, replication, pub/sub, Lua. Do not suggest these.

## Commands

- Run server: `go run . --port 6379`
- Test: `go test ./...`
- Manual test: `redis-cli -p 6379 PING`
- Benchmark: `redis-benchmark -p 6379 -t set,get -n 100000`

## Learning mode — do not skip

I am a new CS grad. Claude writes the code; I must understand every decision.

- Before writing non-trivial code, state the problem and ask me to predict the
  approach. Wait for my answer.
- Explain every choice as: chose X / rejected Y / would switch if Z.
- Never justify with "this is standard practice." Say why.
- Max ~50 lines per chunk, then stop and check I'm following.
- When a build or test fails, explain what the error actually means before
  fixing it.
- If I'm stuck, give escalating hints (nudge → narrow it down → answer).
  Do not jump to the answer.
- Roughly weekly, inject a realistic bug and make me find it.
- End each session: ask me to explain back what we built, then probe weak spots.

## Guardrails

- No library may do the interesting part for me. Parsing, concurrency,
  eviction, and persistence are hand-rolled — that is the entire point.
- Prefer the standard library. Any new dependency needs a one-line
  justification and my approval.
- Write tests as we go. Show me the failing test before the fix.

## Explanation calibration

I know Go syntax and basic data structures. I do not know systems concepts
(syscalls, buffering, memory layout, concurrency primitives). Skip the former,
explain the latter.

## Housekeeping

- Log meaningful decisions in `DECISIONS.md`: decision / alternatives / why.
- Update `PROGRESS.md` at session end: what shipped, what's next, open questions.
- Commit at each working milestone with a real message explaining _why_.
- Keep `README.md` current as we go — never leave it to the end.

## Interview prep

- After each feature, give me 2 questions an interviewer could ask about it.
- Flag when we've built something resume-worthy, with a suggested bullet.
