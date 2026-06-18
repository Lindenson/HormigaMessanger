# Proposal: improve-coverage-20260618

## Why
Baseline JaCoCo coverage is **34.5% line / 31.5% branch**. The entire reactive
orchestration/prod-delivery layer is at **0%** — it is exercised only transitively by
the Karate e2e suite. This is the exact blast radius of the upcoming per-conversation
ordering and performance refactors, so it must have a unit safety net first.

## What (this run — prod-path orchestration first)
- Zero-coverage prod-path classes: pipeline routers + resolver, all pipeline stages,
  InboundPrototype, PipelineMerger, OutboxPoller, OutboxGarbageCollector (incl. the
  Redis-loss rehydrate gate), PresenceCoordinator, LocalSessionRegistry, credits
  (token bucket + filter), FeedbackRegulator, TetrisWatermark, retention scheduler.
- JUnit 5 + Mockito, EPO standards (@DisplayName, parameterized where data-driven, no
  logic in tests).

## Out of scope
- Production-code changes (refactors/fixes). A test revealing a bug → raise, don't fix.
- Alt-profile in-memory storage impls (OutboxManagerInMemory, InMemoryMessageHistory,
  *ConcurrentInsertionOrderMap, TimeOrderedStringKeyMap, OutboxBatchBuffer) — not the
  prod (redis) path; deferred to a follow-up coverage round.
- Backpressure publishers/metrics, loggers, DTO/records/enums (Lombok/trivial).
- Karate e2e (handled separately).
- Coverage thresholds in pom.xml.

## Result (completed 2026-06-18)
Coverage: **34.5% → 58.1% line, 31.5% → 49.7% branch** overall; **76.9% line on the
prod-path** (excl. alt-profile in-memory impls + plumbing). 159 → 266 tests, all green.
Covered this run: pipeline resolver/routers/all 8 stages, InboundPrototype, PipelineMerger,
OutboxPoller, OutboxGarbageCollector (rehydrate gate), LocalSessionRegistry, credits
(bucket+filter), FeedbackRegulator, TetrisWatermark, retention + liveness schedulers,
PresenceCoordinator, ChatResource (REST).

## Deferred to a follow-up round (low prod value)
- Alt-profile `memory`-storage impls (~360 lines, not used in prod): OutboxManagerInMemory,
  InMemoryMessageHistory, *ConcurrentInsertionOrderMap, TimeOrderedStringKeyMap, OutboxBatchBuffer,
  IdempotencyManagerInMemory. (Candidate for deletion as dead code rather than testing.)
- WebsocketService (@WebSocket — covered functionally by the 18 Karate e2e scenarios).
- Backpressure publishers/metrics, router loggers, thin Async* wrappers, DTO/records.
