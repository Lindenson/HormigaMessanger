package org.hormigas.ws.core.batch;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.outbox.OutboxManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Group-commit facade for the inbound persist (plan B). A thin wiring over a {@link GroupCommitBatcher}
 * whose port op writes {@code history+outbox} in one transaction ({@link OutboxManager#saveBatch}) and
 * isolates poison rows (a rolled-back batch is retried row-by-row via {@link OutboxManager#save}, so one
 * bad row fails alone). Called by the persist pipeline stage; each enqueued message resolves with its own
 * {@link StageResult} when its batch commits.
 */
@Slf4j
@ApplicationScoped
public class InboundPersistBatcher {

    @Inject
    OutboxManager<Message> outbox;

    @Inject
    MessengerConfig config;

    @Inject
    MeterRegistry meterRegistry;

    private Counter fallbacks;
    private GroupCommitBatcher<Message, StageResult<Message>> batcher;

    @PostConstruct
    void init() {
        var cfg = config.inbound().persistBatch();
        fallbacks = Counter.builder("messenger.persist.batch.fallbacks")
                .description("Messages retried individually after a batch transaction failure").register(meterRegistry);

        batcher = GroupCommitBuilder.<Message, StageResult<Message>>create()
                .withBatchOp(this::persist)
                .withShedValue(StageResult.failed())
                .withMaxSize(cfg.maxSize()).withLingerMs(cfg.lingerMs()).withConcurrency(cfg.maxConcurrentBatches())
                .withRetryBackoff(config.streamRetry().minBackoffMs(), config.streamRetry().maxBackoffMs())
                .withMetrics(meterRegistry, "messenger.persist.batch")
                .build();

        log.info("Inbound persist batcher ready (maxSize={}, lingerMs={}, maxConcurrentBatches={})",
                cfg.maxSize(), cfg.lingerMs(), cfg.maxConcurrentBatches());
    }

    /** Enqueue a message for group-commit; resolves with its {@link StageResult} once its batch commits. */
    public Uni<StageResult<Message>> enqueue(Message message) {
        return batcher.enqueue(message);
    }

    /** Persist a batch in one transaction, isolate poison rows, and return results in input order. */
    private Uni<List<StageResult<Message>>> persist(List<Message> msgs) {
        return outbox.saveBatch(msgs)
                .onItem().transformToUni(results -> resolveFailures(msgs, results))
                .onItem().transform(resolved -> msgs.stream()
                        .map(m -> resolved.getOrDefault(m.getMessageId(), StageResult.<Message>failed()))
                        .toList());
    }

    /**
     * Isolate poison rows: if the batch transaction rolled back (some/all results FAILED), retry the
     * failed messages one at a time via {@link OutboxManager#save}. The transaction is all-or-nothing, so
     * a rollback committed nothing — re-persisting individually cannot double-insert.
     */
    private Uni<Map<String, StageResult<Message>>> resolveFailures(
            List<Message> msgs, Map<String, StageResult<Message>> results) {

        List<Message> failed = msgs.stream()
                .filter(m -> {
                    StageResult<Message> r = results.get(m.getMessageId());
                    return r == null || r.isFailed();
                })
                .toList();

        if (failed.isEmpty()) {
            return Uni.createFrom().item(results);
        }

        fallbacks.increment(failed.size());
        log.warn("Batch persist had {} failed row(s) — retrying individually to isolate the culprit", failed.size());

        Map<String, StageResult<Message>> merged = new HashMap<>(results);
        return Multi.createFrom().iterable(failed)
                .onItem().transformToUniAndConcatenate(m ->
                        outbox.save(m).onItem().invoke(r -> merged.put(m.getMessageId(), r)))
                .collect().asList()
                .replaceWith(merged);
    }
}
