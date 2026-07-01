package org.hormigas.ws.core.batch;

import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.ports.message.ReadReceipts;

/**
 * Group-commit facade for read-status writes (READ_IN). A thin wiring over a {@link GroupCommitBatcher}
 * whose port op is {@link ReadReceipts#markReadBatch} (one transaction per batch). Called by the READ
 * pipeline stage and the REST fallback; each enqueued mark resolves with the count newly marked READ.
 * Read receipts are idempotent/re-sent, so a shed mark safely resolves to {@code 0}.
 */
@Slf4j
@ApplicationScoped
public class ReadStatusBatcher {

    @Inject
    ReadReceipts receipts;

    @Inject
    MessengerConfig config;

    @Inject
    MeterRegistry meterRegistry;

    private GroupCommitBatcher<ReadReceipts.MarkRead, Integer> batcher;

    @PostConstruct
    void init() {
        var cfg = config.readBatch();
        batcher = GroupCommitBuilder.<ReadReceipts.MarkRead, Integer>create()
                .withBatchOp(receipts::markReadBatch)
                .withShedValue(0)
                .withMaxSize(cfg.maxSize()).withLingerMs(cfg.lingerMs()).withConcurrency(cfg.maxConcurrentBatches())
                .withRetryBackoff(config.streamRetry().minBackoffMs(), config.streamRetry().maxBackoffMs())
                .withMetrics(meterRegistry, "messenger.read.batch")
                .build();

        log.info("Read-status batcher ready (maxSize={}, lingerMs={}, maxConcurrentBatches={})",
                cfg.maxSize(), cfg.lingerMs(), cfg.maxConcurrentBatches());
    }

    /** Enqueue a read-mark; resolves with the count of messages newly marked READ (0 if none / shed). */
    public Uni<Integer> enqueue(String conversationId, String readerId) {
        return batcher.enqueue(new ReadReceipts.MarkRead(conversationId, readerId));
    }
}
