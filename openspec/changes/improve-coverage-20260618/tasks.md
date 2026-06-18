# tasks — improve-coverage-20260618

Prod-path orchestration first (all currently 0% line). Plain JUnit5 + Mockito where possible.

- [x] T1 — `core.router.pipeline.MessagePipelineResolver` (0%) — type→PipelineType mapping + null/unknown → SKIP
- [x] T2 — `core.router.context.InboundPrototype` (0%) — messageId regen, correlationId rule, type switch, serverTs
- [x] T3 — `core.garbage.collector.OutboxGarbageCollector` (0%) — primed→compute→collect; unprimed→rehydrate first; failure→0
- [x] T4 — `core.poller.outbox.OutboxPoller` (0%) — skip-when-queue-not-empty, fetch+publish, batch-size metric
- [x] T5 — `core.router.concurency.PipelineMerger` (0%) — runParallel merges stage results into ctx
- [x] T6 — `core.session.LocalSessionRegistry` (0%) — register/deregister, multi-conn set, streamByClientId, single-session
- [x] T7 — `core.credits.lazy.LazyCreditsBuket` (0%) — token bucket consume/refill/cap
- [x] T8 — `core.credits.filter.InboundMessageFilter` (0%) — credit predicate gating
- [x] T9 — `core.feedback.regulator.FeedbackRegulator` (0%) — adaptive interval up/down/bounds
- [x] T10 — `core.watermark.tetris.TetrisWatermark` (0%) — onDisconnect delegate + recover
- [x] T11 — `scheduler.HistoryRetentionScheduler` (0%) — purge(normalCutoff, frozenCutoff) sums both sweeps
- [x] T12 — `infrastructure.websocket.coordinator.PresenceCoordinator` (0%) — join/leave/evict prior session
- [x] T13 — `core.router.stage.stages.OutboxStage` (0%)
- [x] T14 — `core.router.stage.stages.AckStage` (0%)
- [x] T15 — `core.router.stage.stages.DeliveryStage` (0%) — canDeliver gating, persisted-fail skip

Follow-up (next round): remaining stages (Cache/CleanCache/Final/TetrisSent/TetrisAck),
MessageInboundRouter/MessageOutboundRouter, ChatResource/WebsocketService unit, alt-profile
in-memory impls, backpressure/loggers.
