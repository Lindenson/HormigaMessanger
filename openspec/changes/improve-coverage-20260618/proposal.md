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
