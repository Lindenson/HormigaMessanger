package org.hormigas.ws.core.batch;

import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Uni;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Fluent builder for a {@link GroupCommitBatcher} (mirrors {@code core.backpressure.BackpressureBuilder}).
 * Every batcher differs only in its port op, per-item result type, shed value and metric names — supply
 * those here; the reactive engine is shared.
 *
 * <pre>
 * GroupCommitBatcher&lt;Message, StageResult&lt;Message&gt;&gt; b = GroupCommitBuilder
 *     .&lt;Message, StageResult&lt;Message&gt;&gt;create()
 *     .withBatchOp(this::persist)               // List&lt;I&gt; -&gt; Uni&lt;List&lt;O&gt;&gt; (index-aligned)
 *     .withShedValue(StageResult.failed())
 *     .withMaxSize(64).withLingerMs(5).withConcurrency(8)
 *     .withMetrics(registry, "messenger.persist.batch")
 *     .build();
 * </pre>
 */
public final class GroupCommitBuilder<I, O> {

    private Function<List<I>, Uni<List<O>>> batchOp;
    private O shedValue;
    private int maxSize = 64;
    private int lingerMs = 5;
    private int concurrency = 8;
    private int retryMinBackoffMs = 200;
    private int retryMaxBackoffMs = 5000;
    private MeterRegistry registry;
    private String metricPrefix;

    private GroupCommitBuilder() {}

    public static <I, O> GroupCommitBuilder<I, O> create() {
        return new GroupCommitBuilder<>();
    }

    /** The port batch op: given one batch's items, return their results positionally (index-aligned). */
    public GroupCommitBuilder<I, O> withBatchOp(Function<List<I>, Uni<List<O>>> batchOp) {
        this.batchOp = batchOp;
        return this;
    }

    /** Result an item resolves to when its batch is shed under overload or the flush fails. */
    public GroupCommitBuilder<I, O> withShedValue(O shedValue) {
        this.shedValue = shedValue;
        return this;
    }

    public GroupCommitBuilder<I, O> withMaxSize(int maxSize) {
        this.maxSize = maxSize;
        return this;
    }

    public GroupCommitBuilder<I, O> withLingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
        return this;
    }

    /** Max batch transactions in flight at once (keep ≤ the backing pool size). */
    public GroupCommitBuilder<I, O> withConcurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    /** Re-subscribe backoff for a terminal stream failure (indefinite retry). */
    public GroupCommitBuilder<I, O> withRetryBackoff(int minBackoffMs, int maxBackoffMs) {
        this.retryMinBackoffMs = minBackoffMs;
        this.retryMaxBackoffMs = maxBackoffMs;
        return this;
    }

    /** Metric registry + prefix; the engine registers {@code <prefix>.{flushes,shed,size,flush}}. */
    public GroupCommitBuilder<I, O> withMetrics(MeterRegistry registry, String metricPrefix) {
        this.registry = registry;
        this.metricPrefix = metricPrefix;
        return this;
    }

    public GroupCommitBatcher<I, O> build() {
        Objects.requireNonNull(batchOp, "batchOp");
        Objects.requireNonNull(shedValue, "shedValue");
        Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(metricPrefix, "metricPrefix");
        return new GroupCommitBatcher<>(batchOp, shedValue, maxSize, lingerMs, concurrency,
                retryMinBackoffMs, retryMaxBackoffMs, registry, metricPrefix);
    }
}
