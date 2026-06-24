# Performance

How the Messenger's inbound path performs, and why. Measured 2026-06-24 with the Gatling harness in
[`loadtest/`](../loadtest) driving real WebSockets (each virtual user = a chat pair that creates a
chat over REST, opens master + client sockets with Ory identity headers, then streams `CHAT_IN` and
**awaits the server `SENT` ack** — so every measured message is fully persisted, delivered and
acknowledged, not just written to a socket).

## Headline

- **~3,000 messages/second sustained** at 300 chat pairs (600 live WebSocket clients), held for 4
  minutes, at **p95 19 ms / p99 25 ms**.
- Throughput scales cleanly with concurrency; latency stays flat (no queue build-up).
- **No message loss and no memory growth in steady state** — zero inbound/outbound drops, zero
  full GC, old-generation heap plateaus.

The service comfortably meets and exceeds the **≥ 1,000 msg/s** target.

## Measured capacity

| Chat pairs | WS clients | Throughput (msg/s) | p95 | p99 |
|-----------:|-----------:|-------------------:|----:|----:|
| 50  | 100 | ~520   | 13 ms | 15 ms |
| 100 | 200 | ~1,040 | 14 ms | 16 ms |
| 200 | 400 | ~2,040 | 19 ms | 25 ms |
| 300 | 600 | ~3,000 | 19 ms | 25 ms |

`msg/s` is the sustained send→ack rate over the steady-state hold. Numbers come from a single host
that **also** runs the load generator, PostgreSQL, Redis and MinIO — i.e. everything contends for the
same CPU and disk. They are a **pessimistic floor**; a real deployment with the database and clients
off-box has more headroom.

## How the inbound path works

A `CHAT_IN` becomes one `message_history` + one `outbox` row, written transactionally before the
message is delivered and acked. The throughput-critical decision is **how those writes are
committed**:

1. **Parallel inbound pipeline.** The inbound publisher (`InboundPublisher`) runs in PARALLEL, so many
   `routeIn` flows are in flight at once instead of one-at-a-time.
2. **Group-commit.** `InboundPersistBatcher` (`core/router/persist`) coalesces the concurrent persists
   with `group().intoLists().of(max-size, linger-ms)` and writes each group in a **single
   transaction** (`OutboxManager.saveBatch`), running up to `max-concurrent-batches` transactions
   concurrently. Each message's pipeline resumes the instant its batch commits; delivery, ack, cache
   and watermark stages stay **per-message**, and the `SENT` ack is still emitted per message.
3. **Poison-row isolation.** A batch transaction is all-or-nothing. If one rolls back, the batcher
   retries those messages individually via `OutboxManager.save` — the offending row fails alone
   instead of dooming its batch-mates, and a rolled-back transaction commits nothing so no row is
   ever double-inserted.

This keeps the database and CPU busy enough to clear thousands of messages per second while leaving
the per-message correctness guarantees untouched.

## Tuning

All three batch parameters are configurable under `processing.messages.inbound.persist-batch`
(env-overridable, `prod` profile). Shipped defaults are sized for the default 20-connection DB pool:

| Param | Default | Effect |
|-------|---------|--------|
| `max-size` | `64` | Max messages per transaction. Higher = fewer commits/fsync and a higher ceiling. |
| `linger-ms` | `5` | Max time a partial batch waits before flushing. This is the only latency the batching adds to a message. |
| `max-concurrent-batches` | `8` | Batch transactions in flight at once. Higher = more parallelism until the DB saturates. **Keep ≤ the DB pool size.** |

See the **Performance tuning** table in the [README](../README.md#-performance-tuning) for the full
set of inbound/outbound/poller knobs.

## Resource profile (at ~3,000 msg/s, 600 clients, 4-minute hold)

| Signal | Observed | Reading |
|--------|----------|---------|
| CPU | 7–13% | Not CPU-bound — large headroom remains. |
| DB pool active | 0–8 of 20 | Batching means few, fat transactions; the pool is rarely busy. |
| Inbound/outbound drops | 0 | Nothing shed in steady state. |
| Batch size | avg ~27, max 64 | Healthy coalescing (hits the `max-size` cap under load). |
| Young GC | ~134 pauses, 0.61 s total | ~4.5 ms average pause. |
| Full GC | 0 | — |
| Heap (old gen) | plateaus ~264 MB | Working set settles and stops growing — no leak signal. |
| Resident memory (RSS) | ~0.5 GB idle → ~2 GB under load | The real RAM footprint. |

### A note on the 25–33 GB "memory" figure

Process monitors (`top`/`htop` **VIRT** column) show the JVM reserving **~24–33 GB of virtual address
space**. That is reservation, not usage: on a 32-core host the JVM and glibc pre-reserve G1 heap
regions, hundreds of thread stacks (one set per WebSocket connection), memory-mapped jars and up to
`8 × cores` 64 MB malloc arenas. Only **RSS** costs real RAM, and RSS stayed **under 0.5 GB at idle and
~2 GB under full 600-client load** (heap committed ~88 MB idle). The large VIRT number is normal and
harmless. To make monitoring less alarming, the container sets `MALLOC_ARENA_MAX=2`, which collapses
the arena reservations (and trims RSS a little) with negligible throughput impact.

## Known limit — mass connection churn

When a very large number of WebSocket connections close **simultaneously** (e.g. all 600 at the end of
a soak), the per-disconnect work — presence broadcast, Tetris watermark cleanup, idempotency
bookkeeping, all of which hit Redis — can briefly exceed the Redis client connection pool
(`ConnectionPoolTooBusyException`, default wait queue 24). Messages still in flight on those closing
sockets may not complete (~0.8% in the 600-at-once teardown). This is a **connection-churn** limit on
the disconnect path, not a steady-state or throughput limit (steady state had zero drops). It is
addressed by ops hardening: sizing the Redis client pool for churn and draining connections gracefully
on shutdown.

## Running the load test

```bash
# 1. Boot the app (prod profile) against Postgres + Redis + MinIO.
# 2. From loadtest/ (JDK 21):
mvn gatling:test -Dgatling.simulationClass=load.MessengerLoadSimulation \
    -Dload.users=300 -Dload.ramp=15 -Dload.duration=240 -Dload.pauseMs=0
```

`load.users` = concurrent chat pairs, `ramp` / `duration` in seconds. Watch the server live at
`/q/metrics` (throughput, `sql_pool_active`, `messenger_persist_batch_*`, GC, WS sessions). The HTML
report is written under `loadtest/target/gatling/`.
