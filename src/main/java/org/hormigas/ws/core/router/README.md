# Message Router — how it works

> Package: `org.hormigas.ws.core.router`. This is the heart of the messenger: it takes one
> `Message` and runs it through a **strategy-specific pipeline of stages** (persist, deliver, ack,
> cache, GC-mark). Reactive (Mutiny `Uni`), hexagonal (stages depend on **ports**, not adapters).

## 1. The 10 000-foot view

There are **two routers**, one per direction, both fed by a **bounded back-pressured publisher**:

```
            INBOUND (client → server)                       OUTBOUND (server → client)
  WebsocketService.onMessage(rawJson)               OutboxPoller.pull()  (RoutingScheduler, every ~1s)
        │  validate + membership guard                     │  fetch a batch of outbox rows
        ▼                                                   ▼
  InboundPublisher.publish(msg) ──┐                 RoutingBackpressurePublisher.publish(msg) ──┐
   (BackpressurePublisher, SEQUENTIAL, bounded queue)        (BackpressurePublisher, SEQUENTIAL) │
        │  emit → Multi (backpressure)  │                    │  emit → Multi (backpressure)       │
        ▼                               ▼                     ▼                                    ▼
  MessageInboundRouter.routeIn(msg)                   MessageOutboundRouter.routeOut(msg)
        │                                                   │
        ▼                                                   ▼
  PipelineResolver.resolvePipeline(msg) ── MessageType → PipelineType
        │                                                   │
        ▼                                                   ▼
  InboundPrototype.createOutboundContext(...)         RouterContext.builder()  (sets serverTimestamp only)
   (assigns server messageId, flips IN→OUT type, …)
        │                                                   │
        ▼                                                   ▼
  switch(PipelineType) → a chain of PipelineStage         switch(PipelineType) → a chain of stages
        │                                                   │
        ▼                                                   ▼
  Uni<MessageEnvelope> { message, processed }         Uni<MessageEnvelope> { message, processed }
```

Key idea: **`MessageType` decides the strategy; the strategy is a fixed chain of stages.** A stage
is a pure `Uni<RouterContext> apply(RouterContext)` — it does one thing (save / deliver / cache /
mark), records its outcome on the shared `RouterContext`, and is **failure-isolated** (every stage
recovers to the same context on error so one fault can't kill the stream).

Entry points:
- **Inbound:** `WebsocketService` (`infrastructure/websocket`) → `InboundPublisher`
  (`infrastructure/websocket/inbound`) → `MessageInboundRouter.routeIn`.
- **Outbound:** `OutboxPoller` (`core/poller/outbox`) driven by `RoutingScheduler`
  (`scheduler`) → `RoutingBackpressurePublisher` → `MessageOutboundRouter.routeOut`.

---

## 2. Routing matrix — `MessagePipelineResolver`

`MessagePipelineResolver` is just an `EnumMap<MessageType, PipelineType>`. The full mapping:

| MessageType    | PipelineType        | Direction | Strategy meaning |
|----------------|---------------------|-----------|------------------|
| `CHAT_IN`      | `INBOUND_PERSISTENT`| in        | A — persist (history+outbox) then deliver, ack, cache, GC-mark |
| `SIGNAL_IN`    | `INBOUND_CACHED`    | in        | S — deliver live + cache, **no persistence** (WebRTC signaling) |
| `PRESENT_INIT` / `PRESENT_JOIN` / `PRESENT_LEAVE` | `INBOUND_DIRECT` | in | B — deliver live only |
| `CHAT_ACK`     | `ACK_PERSISTENT`    | in        | recipient's delivery ACK → advance GC watermark + SENT→DELIVERED + clean cache |
| `SIGNAL_ACK`   | `ACK_CACHED`        | in        | signaling ACK → clean cache only |
| `CHAT_OUT`     | `OUTBOUND_CACHED`   | out       | deliver (from outbox/poller) + cache + GC-mark |
| `SIGNAL_OUT`   | `OUTBOUND_CACHED`   | out       | deliver + cache + GC-mark |
| `SERVICE_OUT`  | `OUTBOUND_DIRECT`   | out       | deliver live only (e.g. overload notice) |
| anything else / null | `SKIP`        | —         | straight to FinalStage, no work |

> **Note — `READ_IN` / `READ_OUT` are NOT in the matrix.** They are handled directly in
> `WebsocketService` (`markReadAndNotify` / `readReceiptFor`), not routed through a pipeline, so the
> resolver returns `SKIP` for them. If you ever publish a `READ_*` into a router it is a no-op.

---

## 3. The pipelines (stage chains)

### Inbound — `MessageInboundRouter.routeIn`

```
INBOUND_PERSISTENT (CHAT_IN):
    OutboxStage ─► DeliveryStage ─► ┌─ parallel (PipelineMerger.runParallel) ─┐ ─► FinalStage
                                    │  AckStage · CacheStage · TetrisSentStage │
                                    └──────────────────────────────────────────┘

INBOUND_CACHED (SIGNAL_IN):
    DeliveryStage ─► CacheStage ─► FinalStage

INBOUND_DIRECT (PRESENT_*):
    DeliveryStage ─► FinalStage

ACK_PERSISTENT (CHAT_ACK):
    TetrisAckStage ─► CleanCacheStage ─► FinalStage

ACK_CACHED (SIGNAL_ACK):
    CleanCacheStage ─► FinalStage

SKIP:
    FinalStage
```

### Outbound — `MessageOutboundRouter.routeOut`

```
OUTBOUND_CACHED (CHAT_OUT, SIGNAL_OUT):
    DeliveryStage ─► CacheStage ─► TetrisSentStage ─► FinalStage

OUTBOUND_DIRECT (SERVICE_OUT):
    DeliveryStage ─► FinalStage

SKIP:
    FinalStage
```

The outbound router does **not** use `InboundPrototype`; it builds the `RouterContext` directly and
only stamps `serverTimestamp`. The message it routes was already shaped (and given its server
`messageId`) on the inbound pass before it was written to the outbox.

---

## 4. `RouterContext` — the mutable bag passed down the chain

`RouterContext<Message>` (Lombok `@Data @Builder`) carries the payload plus four **per-concern
outcomes**, each a `StageResult`:

| field          | set by                         | meaning |
|----------------|--------------------------------|---------|
| `persisted`    | `OutboxStage`                  | written to history+outbox? |
| `delivered`    | `DeliveryStage`                | pushed to the recipient's live WS session? |
| `cached`       | `CacheStage` / `CleanCacheStage` | present in the idempotency/redelivery cache? |
| `acknowledged` | `AckStage` / `TetrisAckStage`  | server SENT-ack sent / recipient ACK processed? |

Plus `pipelineType`, `error` (`@With`), `payload` (`@With`), and `done`.

`StageResult` (`domain/stage`) is one of `UNKNOWN, UPDATED, PASSED, SKIPPED, FAILED`.
`isSuccess() == PASSED || UPDATED`. **Default for every outcome is `UNKNOWN`** — this matters (see §7).

---

## 5. What each stage does

| Stage | Port used | Sets | Behaviour |
|-------|-----------|------|-----------|
| **OutboxStage** | `OutboxManager.save` | `persisted` | Atomic insert into `message_history` + `outbox`. If the adapter returns an updated payload (DB-assigned id) it swaps it into the context. |
| **DeliveryStage** | `DeliveryChannel.deliver`, `IdempotencyManager.isInProgress` | `delivered` | Push to the recipient's live WS session, with backoff retry. **Gate:** for gated pipelines, skip unless `persisted` succeeded; **INBOUND_CACHED/INBOUND_DIRECT bypass the gate** (they never persist — this is what makes signaling/presence actually deliver). `CHAT_ACK` / `PRESENT_JOIN` / `PRESENT_INIT` bypass the in-progress idempotency check. |
| **AckStage** | (delegates to `DeliveryStage`), `IdGenerator` | `acknowledged` | If `persisted` succeeded, builds a `CHAT_ACK` (`server` → original sender, `correlationId` = the sent message's id) and delivers it — this is the **SENT receipt** back to the sender. |
| **CacheStage** | `IdempotencyManager.add` | `cached` | Only if `delivered` succeeded, add to the idempotency/redelivery cache; else `SKIPPED`. |
| **CleanCacheStage** | `IdempotencyManager.remove` | `cached` | Remove the cache entry (on the ACK pipelines, once the message is acknowledged). |
| **TetrisSentStage** | `TetrisMarker.onSent` | — | Only if `delivered` succeeded, mark the id **pending** in the per-recipient GC watermark (so the outbox row is not GC'd until ACK/disconnect). |
| **TetrisAckStage** | `TetrisMarker.onAck`, `ReadReceipts.markDelivered` | `acknowledged` | The recipient's delivery ACK advances the GC watermark **and** flips the persisted status SENT→DELIVERED (`correlationId` = delivered id). The status write is best-effort. |
| **FinalStage** | — | `done` | Computes the terminal success flag (see §6). |

---

## 6. `FinalStage` — what "done" means per pipeline

`done` (surfaced as `MessageEnvelope.processed`) is computed from the outcome that matters for that
strategy:

| PipelineType | `done` = |
|--------------|----------|
| `INBOUND_PERSISTENT` | `persisted.isSuccess()` |
| `INBOUND_CACHED` / `INBOUND_DIRECT` | `delivered.isSuccess()` |
| `OUTBOUND_CACHED` / `OUTBOUND_DIRECT` | `delivered.isSuccess()` |
| `ACK_PERSISTENT` | `acknowledged.isSuccess()` |
| `ACK_CACHED` | `cached.isSuccess()` |
| any error on the context | `done = false` |

---

## 7. Concurrency, failure isolation & a known quirk

**Failure isolation.** Every stage ends with `.onFailure().invoke(ctx::setError).onFailure()
.recoverWithItem(ctx)` — a stage never throws out of the chain; it records the error on the context
and passes the context along. `FinalStage` turns a present `error` into `done = false`.

**Parallel group (`PipelineMerger.runParallel`).** Only `INBOUND_PERSISTENT` fans out: after persist
+ deliver, it runs `AckStage`, `CacheStage`, `TetrisSentStage` concurrently
(`Uni.combine().all()`), recovers each independently, then **rebuilds a fresh context** by merging
the four `StageResult`s (`AND` for success, `OR` for failure) and keeping the first error.

> ⚠️ **Known quirk — the merged outcomes under-report success.** `mergeStageResult` seeds the
> aggregate at `UNKNOWN` and only yields `PASSED` when **both** inputs are already success. Since the
> seed is `UNKNOWN`, the aggregate stays `UNKNOWN` even when a stage passed. Net effect: after the
> parallel group, `INBOUND_PERSISTENT`'s `persisted`/`delivered` in the *rebuilt* context read
> `UNKNOWN`, so `FinalStage` computes `done = false` for persistent messages. **This is cosmetic** —
> the real work (history+outbox write, live push, GC-mark) already happened as side effects in the
> stages; nothing downstream acts on `done` beyond logging. If you ever start trusting
> `MessageEnvelope.processed` for `CHAT_IN`, fix the merge seed first.

**Back-pressure & ordering.** Both publishers run the sink (`routeIn` / `routeOut`) in
`SEQUENTIAL` mode over a **bounded** queue (`processing.messages.{inbound,outbound}.queue-size`).
`publish()` returns `false` when the queue is full (the message is dropped and the caller is told —
inbound sends the sender a `SERVICE_OUT` overload notice). On a terminal stream error the publisher
**re-subscribes with backoff** so a single fault doesn't disable delivery for the JVM lifetime.

---

## 8. `InboundPrototype` — the inbound message transform

`createOutboundContext(pipeline, message)` builds the initial inbound context and normalizes the
message **server-side** (never trust the client for these):

- `messageId` ← a fresh server ULID (`IdGenerator`) — monotonic, doubles as the history page cursor.
- `type` ← `switchMesssageType`: `CHAT_IN → CHAT_OUT`, `SIGNAL_IN → SIGNAL_OUT`, otherwise unchanged.
- `correlationId` ← for `CHAT_ACK` keep the client's `correlationId`; otherwise the message's own id.
- `serverTimestamp` ← now.

So by the time stages run, a `CHAT_IN` is already a `CHAT_OUT` carrying its server id; that is what
`OutboxStage` persists and what the outbound poller later re-routes.

---

## 9. Gotchas / things to remember before you change this

1. **Delivery gate vs. non-persistent pipelines.** `DeliveryStage` historically skipped delivery
   unless `persisted` succeeded. `INBOUND_CACHED` (signaling) and `INBOUND_DIRECT` (presence) never
   persist, so they were silently dropped until the gate was made to bypass them. Keep that bypass.
2. **Outbound poller delivery is enabled by marking the outbound context `persisted`.** `routeOut`
   sets `persisted = PASSED` (the message came from the durable outbox, so it *is* persisted) —
   otherwise `DeliveryStage`'s gate would skip every poller (re)delivery and the outbox poller would
   never re-push live. Double-delivery to an already-served **online** recipient is prevented by the
   idempotency cache (`DeliveryStage.canDeliver → isInProgress`, 30s TTL); an **offline** recipient
   (never cached) gets the live re-push when it reconnects. Verified by
   `e2etests/poller-redelivery-check.js` (S1: online → exactly one `CHAT_OUT`; S2: offline→reconnect
   → poller redelivers live). If you remove that line, live redelivery silently dies again.
3. **The merge quirk (§7)** — `done` is unreliable for `INBOUND_PERSISTENT`; it's cosmetic today.
4. **`READ_IN`/`READ_OUT` bypass the router** (handled in `WebsocketService`).
5. **Stages are CDI beans**; the routers `@Inject` them by constructor. Adding a stage = add the bean
   + wire it into the relevant `switch` branch in `MessageInboundRouter` / `MessageOutboundRouter`.
6. **Idempotency / GC live in ports**, not in the router: `IdempotencyManager` (Redis),
   `TetrisMarker` (Redis ZSET watermark), `OutboxManager` (Postgres). The router only orchestrates.

---

## 10. File map

| Concern | File |
|---------|------|
| Strategy selection | `pipeline/MessagePipelineResolver.java`, `PipelineResolver.java` |
| Inbound chains | `pipeline/MessageInboundRouter.java`, `InboundRouter.java` |
| Outbound chains | `pipeline/MessageOutboundRouter.java`, `OutboundRouter.java` |
| Initial context / normalize | `context/InboundPrototype.java`, `context/RouterContext.java` |
| Stages | `stage/stages/*.java`, `stage/PipelineStage.java` |
| Parallel merge | `concurency/PipelineMerger.java`, `concurency/Merger.java` |
| Back-pressure feed | `publisher/RoutingBackpressurePublisher.java`, `infrastructure/websocket/inbound/InboundPublisher.java` |
| Logging | `logger/inout/*.java`, `logger/RouterLogger.java` |
| Outcome type | `domain/stage/StageResult.java` |
