# Mutiny backpressure & the persist-batcher stall (engineering note)

A field note from the 2026-06-29 load tests: what stalled, the root cause, how Mutiny backpressure
actually works, the fix, and the reusable rule — plus where else the same class of defect may lurk.

## The incident

Under a load test that lifted the per-connection credit limiter (test-only override) and fire-hosed
~3,000 msg/s from 30 connections, persistence **stalled permanently**: `message_history` froze, ~987k
messages were dropped at ingress, and the app logged:

```
ERROR InboundPersistBatcher: Persist-batch stream failed — retrying:
io.smallrye.mutiny.subscription.BackPressureFailure: Cannot emit item due to lack of requests
```

CPU was 2–3 % the whole time — **not** a resource limit. A reactive backpressure defect.

## Is this a Mutiny bug or our misuse?

**Our misuse / an unconfigured sharp edge — not a Mutiny bug.** We composed a time-triggered emitter
with a backpressuring consumer and never said what to do when the producer outruns the consumer at that
junction. Mutiny did the safe-but-harsh thing: fail loudly rather than lose data silently or grow memory
unbounded. The fix is the *correct* use of the API — declare an overflow strategy on that junction.

## How Mutiny backpressure actually works (the mental model)

Mutiny is full **Reactive Streams** (peer to Reactor), not "primitive". Two signals flow in **opposite
directions**:

- **Demand flows UP** via `request(n)`: a subscriber tells its upstream "send me at most N". A consumer
  applies backpressure simply by **not requesting** (`request(0)`) — no exception, no blocking.
- **Items and errors flow DOWN** toward the subscriber.

So `merge(concurrency)` does not throw when saturated — it just **stops requesting** (clean
backpressure). The exception comes from **upstream** of merge.

### Push-nodes are the catch

Some nodes produce an item on **their own event, not on a downstream request** — "push" sources. When
downstream demand is 0, a push-node cannot honor backpressure and must apply a **strategy**. Each
push-node needs one set deliberately:

| Push-node | Pushes when… | Strategy when downstream demand = 0 |
|-----------|--------------|--------------------------------------|
| `Multi.createFrom().emitter(...)` | you call `emit()` | the `BackPressureStrategy` you pass (BUFFER / DROP / LATEST / ERROR / IGNORE) |
| `group().intoLists().of(size, <Duration>)` (`MultiBufferWithTimeoutOp`) | the **linger timer** fires | **default: throw `BackPressureFailure`** (kills the stream) |
| `group().intoLists().of(size)` (size-only) | — | **safe**: emits only on demand or completion (no timer push) |

`merge(n)` is a *pull* node — it backpressures by not requesting, never by throwing.

## What stalled, precisely

```
emitter(BUFFER) → group().intoLists().of(64, 5ms) → merge(8) [flush → DB]
```

When all 8 `merge` slots were in flight (busy), `merge` requested 0 from the group. The group's **5 ms
linger timer** then fired, tried to emit a batch into 0 demand → `BackPressureFailure` → the whole
stream failed. `retry().indefinitely()` re-subscribed, immediately re-saturated, re-failed — so under
sustained overload persistence stayed at ~0 and in-flight messages' completions were abandoned.

We had set an overflow strategy on the **emitter** (`bufferUnconditionally`) but **not** on the
**group→merge** junction — the second push-node inherited the "fail" default.

## The fix

Insert an explicit overflow strategy at that junction — `onOverflow().invoke(shed).drop()`:

```java
.group().intoLists().of(maxSize, Duration.ofMillis(lingerMs))
.onOverflow().invoke(batch -> {                 // before dropping, fail the messages cleanly
    shed.increment(batch.size());                // metric: messenger.persist.batch.shed
    for (Pending p : batch) p.done().complete(StageResult.failed()); // sender gets `overloaded`, retries
}).drop()                                        // shed the excess batch (no unbounded memory)
.onItem().transformToUni(this::flush).merge(concurrency)
```

- `onOverflow()` subscribes to the group with **unbounded demand** (`request(Long.MAX)`), so the group
  **never sees demand = 0** → the timed flush always has a taker → `BackPressureFailure` becomes
  impossible; the stream cannot die this way.
- Under genuine overload, items pile at this node and `.drop()` sheds whole batches — but `.invoke(...)`
  runs first, completing each message `FAILED` so the sender is told `overloaded` and retries.
  **No silent loss, no unbounded memory.**

Verified: the same brutal run that froze at 341,501 then sustained ~3,025 msg/s, persisted 1,324,500,
shed 0, dropped 0, 0 `BackPressureFailure`, flat memory. Commit `f0d3394`; test
`shedsUnderSaturationAndSurvives`.

In production the credit limiter (100 burst / 10/s per connection) keeps this overload regime
unreachable; the fix is defense-in-depth.

## The reusable rule

> **Every push-node needs an explicit overflow strategy.** When you compose a timer/emitter-driven
> producer with anything that can apply backpressure (`merge`, a slow `transformToUni`, a bounded
> consumer), declare what happens when the producer outruns it — `onOverflow().drop()/buffer(n)/…` or
> the emitter's `BackPressureStrategy`. Never leave a timed `group().intoLists().of(size, time)` feeding
> a backpressuring stage without an `onOverflow` in between.

## Where else this may lurk (audit backlog)

- **`RedisPresenceManager`** — flagged by the architect (2026-06-29) as the same class of issue. Quick
  look: `getAll()` uses the **size-only** `group().intoLists().of(BATCH_SIZE)` + `.concatenate()`, which
  is backpressure-safe — so the *exact* timed-flush defect is not obviously there. **Action: audit the
  class for unguarded push-nodes / timed buffers / emitter strategies and apply this pattern where a
  producer can outrun a backpressuring sink.**
- **Read-receipt group-commit batcher** (planned) — build it with this `onOverflow` pattern from the
  start, not a bare timed `group()`, so it can't reproduce this stall.
