package org.hormigas.ws.core.batch;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.mutiny.subscription.UniEmitter;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Generic group-commit facade between a <b>client</b> (a pipeline stage or a REST resource) and a
 * batch-capable <b>port</b>. Each {@link #enqueue(Object)} returns a {@link Uni} that resolves with that
 * item's own result once its batch commits — so callers stay per-item while the port is hit in batches.
 *
 * <p>The reactive plumbing is identical for every batcher (only the port op / result type differ), so it
 * lives here once — built via {@link GroupCommitBuilder}, mirroring {@code core.backpressure}. The
 * pipeline is the backpressure-safe shape (see {@code docs/notes/mutiny-backpressure.md}): the timed
 * {@code group().intoLists().of(size, linger)} is drained through {@code onOverflow().invoke(shed).drop()},
 * so the timed flush always has demand (no {@code BackPressureFailure} stall) and, under sustained
 * source-faster-than-sink overload, whole batches are shed — each item completed with {@code shedValue}.
 *
 * @param <I> input item type (e.g. a Message, a read-mark)
 * @param <O> per-item result type (e.g. a StageResult, a marked-count)
 */
public final class GroupCommitBatcher<I, O> {

    /** Port batch op: given the items of one batch, return their results <b>positionally</b> (index-aligned). */
    private final Function<List<I>, Uni<List<O>>> batchOp;
    /** Value an item resolves to when its batch is shed (overload) or the flush fails. */
    private final O shedValue;

    private final Counter flushes;
    private final Counter shed;
    private final DistributionSummary sizes;
    private final Timer flushTimer;
    private final MeterRegistry registry;

    private final AtomicReference<MultiEmitter<? super Pending<I, O>>> emitter = new AtomicReference<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);

    private record Pending<A, B>(A item, UniEmitter<? super B> done) {}

    GroupCommitBatcher(Function<List<I>, Uni<List<O>>> batchOp, O shedValue,
                       int maxSize, int lingerMs, int concurrency,
                       int retryMinBackoffMs, int retryMaxBackoffMs,
                       MeterRegistry registry, String metricPrefix) {
        this.batchOp = batchOp;
        this.shedValue = shedValue;
        this.registry = registry;
        this.flushes = Counter.builder(metricPrefix + ".flushes")
                .description("Batch transactions committed").register(registry);
        this.shed = Counter.builder(metricPrefix + ".shed")
                .description("Items shed when batches outran the sink (resolved with the shed value)").register(registry);
        this.sizes = DistributionSummary.builder(metricPrefix + ".size")
                .description("Items coalesced into one batch").register(registry);
        this.flushTimer = Timer.builder(metricPrefix + ".flush")
                .description("Latency of one batch flush").register(registry);

        Multi.createFrom().<Pending<I, O>>emitter(emitter::set, BackPressureStrategy.BUFFER)
                .onOverflow().bufferUnconditionally()
                .group().intoLists().of(maxSize, Duration.ofMillis(lingerMs))
                // Demand cushion: the timed group flush ignores downstream demand; draining it through a
                // drop-overflow keeps it from BackPressureFailure-stalling and sheds whole batches under
                // sustained overload — completing each item with shedValue (never a silent hang).
                .onOverflow().invoke(batch -> {
                    shed.increment(batch.size());
                    for (Pending<I, O> p : batch) {
                        p.done().complete(shedValue);
                    }
                }).drop()
                .onItem().transformToUni(this::flush).merge(concurrency)
                .onFailure().invoke(f -> Log.errorf(f, "Batch stream failed (%s) — retrying", metricPrefix))
                .onFailure().retry()
                .withBackOff(Duration.ofMillis(retryMinBackoffMs), Duration.ofMillis(retryMaxBackoffMs)).indefinitely()
                .subscribe().with(
                        ignored -> { /* per-batch side effects happen in flush */ },
                        failure -> Log.errorf(failure, "Batch stream terminated (%s, retries exhausted)", metricPrefix));

        ready.set(true);
    }

    /**
     * Enqueue one item for group-commit. The returned Uni completes with the item's result once its batch
     * resolves (or {@code shedValue} if the batcher isn't ready / the batch is shed / the flush fails).
     */
    public Uni<O> enqueue(I item) {
        return Uni.createFrom().emitter(em -> {
            MultiEmitter<? super Pending<I, O>> e = emitter.get();
            if (!ready.get() || e == null) {
                em.complete(shedValue);
                return;
            }
            e.emit(new Pending<>(item, em));
        });
    }

    private Uni<Void> flush(List<Pending<I, O>> batch) {
        sizes.record(batch.size());
        Timer.Sample sample = Timer.start(registry);
        List<I> items = batch.stream().map(Pending::item).toList();
        return batchOp.apply(items)
                .onItem().invoke(results -> {
                    sample.stop(flushTimer);
                    flushes.increment();
                    for (int i = 0; i < batch.size(); i++) {
                        batch.get(i).done().complete(i < results.size() ? results.get(i) : shedValue);
                    }
                })
                .onFailure().recoverWithItem(err -> {
                    Log.errorf(err, "Batch flush failed — completing %d item(s) with the shed value", batch.size());
                    for (Pending<I, O> p : batch) {
                        p.done().complete(shedValue);
                    }
                    return List.of();
                })
                .replaceWithVoid();
    }
}
