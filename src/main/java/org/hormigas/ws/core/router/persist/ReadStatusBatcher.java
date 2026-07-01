package org.hormigas.ws.core.router.persist;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.mutiny.subscription.UniEmitter;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.ports.message.ReadReceipts;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Group-commit accumulator for read-status writes (READ_IN). Every {@code READ_IN} routes through the
 * pipeline ({@code ReadStage}); its DB write must be batched, not row-per-event — same principle and
 * shape as {@link InboundPersistBatcher} for inbound persistence. Enqueued marks are coalesced into one
 * transaction ({@link ReadReceipts#markReadBatch}), several transactions running concurrently.
 *
 * <p>Built with the backpressure-safe pattern learned from the persist batcher (see
 * {@code docs/notes/mutiny-backpressure.md}): the timed {@code group().intoLists().of(size, time)} is
 * drained through {@code onOverflow().invoke(shed).drop()} so the timed flush always has demand (no
 * {@code BackPressureFailure} stall). Under genuine overload whole batches are shed — each mark resolved
 * as {@code 0} (nothing recorded); a read receipt is idempotent and re-sent on the next READ_IN /
 * reconnect, so shedding is safe.
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

    private record Pending(ReadReceipts.MarkRead op, UniEmitter<? super Integer> done) {}

    private final AtomicReference<MultiEmitter<? super Pending>> emitter = new AtomicReference<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);

    private Counter batches;
    private Counter shed;

    @PostConstruct
    void init() {
        var cfg = config.readBatch();
        int maxSize = cfg.maxSize();
        int lingerMs = cfg.lingerMs();
        int concurrency = cfg.maxConcurrentBatches();

        batches = Counter.builder("messenger.read.batch.flushes")
                .description("Read-status batch transactions committed").register(meterRegistry);
        shed = Counter.builder("messenger.read.batch.shed")
                .description("Read marks shed (resolved 0) when batches outran the DB sink").register(meterRegistry);

        Multi.createFrom().<Pending>emitter(em -> emitter.set(em), BackPressureStrategy.BUFFER)
                .onOverflow().bufferUnconditionally()
                .group().intoLists().of(maxSize, Duration.ofMillis(lingerMs))
                // Same demand cushion as InboundPersistBatcher: the timed group flush ignores downstream
                // demand; draining through drop-overflow keeps it from BackPressureFailure-stalling and
                // sheds whole batches (each mark → 0) under sustained overload instead of failing.
                .onOverflow().invoke(batch -> {
                    shed.increment(batch.size());
                    for (Pending p : batch) {
                        p.done().complete(0);
                    }
                }).drop()
                .onItem().transformToUni(this::flush).merge(concurrency)
                .onFailure().invoke(f -> Log.error("Read-status batch stream failed — retrying", f))
                .onFailure().retry().withBackOff(Duration.ofMillis(200), Duration.ofSeconds(5)).indefinitely()
                .subscribe().with(
                        ignored -> { /* per-batch side effects happen in flush */ },
                        failure -> Log.error("Read-status batch stream terminated (retries exhausted)", failure));

        ready.set(true);
        Log.infof("Read-status batcher ready (maxSize=%d, lingerMs=%d, maxConcurrentBatches=%d)",
                maxSize, lingerMs, concurrency);
    }

    /**
     * Enqueue a read-mark for group-commit. The returned Uni completes with the count of messages newly
     * marked READ once its batch commits (0 if none / if shed under overload).
     */
    public Uni<Integer> enqueue(String conversationId, String readerId) {
        return Uni.createFrom().emitter(em -> {
            MultiEmitter<? super Pending> e = emitter.get();
            if (!ready.get() || e == null) {
                log.warn("Read-status batcher not ready — marking 0 for conv {}", conversationId);
                em.complete(0);
                return;
            }
            e.emit(new Pending(new ReadReceipts.MarkRead(conversationId, readerId), em));
        });
    }

    private Uni<Void> flush(List<Pending> batch) {
        List<ReadReceipts.MarkRead> ops = batch.stream().map(Pending::op).toList();
        return receipts.markReadBatch(ops)
                .onItem().invoke(counts -> {
                    batches.increment();
                    for (int i = 0; i < batch.size(); i++) {
                        batch.get(i).done().complete(i < counts.size() ? counts.get(i) : 0);
                    }
                })
                .onFailure().recoverWithItem(err -> {
                    log.error("Read-status batch flush failed — completing {} mark(s) as 0", batch.size(), err);
                    for (Pending p : batch) {
                        p.done().complete(0);
                    }
                    return List.of();
                })
                .replaceWithVoid();
    }
}
