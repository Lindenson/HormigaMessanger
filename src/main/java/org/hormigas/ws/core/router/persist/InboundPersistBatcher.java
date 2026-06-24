package org.hormigas.ws.core.router.persist;

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
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.outbox.OutboxManager;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Group-commit accumulator for the inbound persist (plan B — see {@code quality/performance}
 * load-test findings R2). Profiling showed the ~225 msg/s ceiling was pure serialization: the
 * SEQUENTIAL inbound pipeline held ≤1 persist in flight, leaving CPU at ~3% and 19/20 DB
 * connections idle. With the inbound publisher switched to PARALLEL, many {@code routeIn} flows
 * arrive here at once; this batcher coalesces their {@code history+outbox} writes into one
 * transaction per batch ({@link OutboxManager#saveBatch}) and runs up to
 * {@code maxConcurrentBatches} such transactions concurrently to fill the idle pool.
 *
 * <p>Each enqueued message gets a {@link Uni} that completes with its own {@link StageResult}
 * when its batch commits, so the rest of the per-message pipeline (deliver → ack → cache →
 * tetris) is unchanged. A batch whose transaction rolls back is retried message-by-message via
 * {@link OutboxManager#save} so a single poison row fails alone instead of dooming its batch-mates.
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

    private record Pending(Message msg, UniEmitter<? super StageResult<Message>> done) {}

    private final AtomicReference<MultiEmitter<? super Pending>> emitter = new AtomicReference<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);

    private DistributionSummary batchSize;
    private Counter batches;
    private Counter fallbacks;
    private Timer flushTimer;

    @PostConstruct
    void init() {
        var cfg = config.inbound().persistBatch();
        int maxSize = cfg.maxSize();
        int lingerMs = cfg.lingerMs();
        int concurrency = cfg.maxConcurrentBatches();

        batchSize = DistributionSummary.builder("messenger.persist.batch.size")
                .description("Number of messages coalesced into one inbound persist transaction").register(meterRegistry);
        batches = Counter.builder("messenger.persist.batch.flushes")
                .description("Inbound persist batch transactions committed").register(meterRegistry);
        fallbacks = Counter.builder("messenger.persist.batch.fallbacks")
                .description("Messages retried individually after a batch transaction failure").register(meterRegistry);
        flushTimer = Timer.builder("messenger.persist.batch.flush")
                .description("Latency of one inbound persist batch flush").register(meterRegistry);

        Multi.createFrom().<Pending>emitter(em -> emitter.set(em), BackPressureStrategy.BUFFER)
                .onOverflow().bufferUnconditionally()
                .group().intoLists().of(maxSize, Duration.ofMillis(lingerMs))
                .onItem().transformToUni(this::flush).merge(concurrency)
                .onFailure().invoke(f -> Log.error("Persist-batch stream failed — retrying", f))
                .onFailure().retry().withBackOff(Duration.ofMillis(200), Duration.ofSeconds(5)).indefinitely()
                .subscribe().with(
                        ignored -> { /* per-batch side effects happen in flush */ },
                        failure -> Log.error("Persist-batch stream terminated (retries exhausted)", failure));

        ready.set(true);
        Log.infof("Inbound persist batcher ready (maxSize=%d, lingerMs=%d, maxConcurrentBatches=%d)",
                maxSize, lingerMs, concurrency);
    }

    /**
     * Enqueue a message for group-commit. The returned Uni completes with this message's
     * {@link StageResult} once its batch resolves (UPDATED with the DB id on success, FAILED otherwise).
     */
    public Uni<StageResult<Message>> enqueue(Message message) {
        return Uni.createFrom().emitter(em -> {
            MultiEmitter<? super Pending> e = emitter.get();
            if (!ready.get() || e == null) {
                log.warn("Persist batcher not ready — failing message {}", message != null ? message.getMessageId() : null);
                em.complete(StageResult.failed());
                return;
            }
            e.emit(new Pending(message, em));
        });
    }

    private Uni<Void> flush(List<Pending> batch) {
        batchSize.record(batch.size());
        Timer.Sample sample = Timer.start(meterRegistry);
        List<Message> msgs = batch.stream().map(Pending::msg).toList();

        return outbox.saveBatch(msgs)
                .onItem().transformToUni(results -> resolveFailures(batch, results))
                .onItem().invoke(resolved -> {
                    sample.stop(flushTimer);
                    batches.increment();
                    for (Pending p : batch) {
                        p.done().complete(resolved.getOrDefault(p.msg().getMessageId(), StageResult.failed()));
                    }
                })
                .onFailure().recoverWithItem(err -> {
                    log.error("Inbound batch flush failed unexpectedly — completing {} message(s) as FAILED",
                            batch.size(), err);
                    for (Pending p : batch) {
                        p.done().complete(StageResult.failed());
                    }
                    return Map.of();
                })
                .replaceWithVoid();
    }

    /**
     * Isolate poison rows: if the batch transaction rolled back (some/all results FAILED), retry the
     * failed messages one at a time via {@link OutboxManager#save}. The transaction is all-or-nothing,
     * so a rollback committed nothing — re-persisting individually cannot double-insert. The culprit
     * then fails alone while its batch-mates succeed.
     */
    private Uni<Map<String, StageResult<Message>>> resolveFailures(
            List<Pending> batch, Map<String, StageResult<Message>> results) {

        List<Pending> failed = batch.stream()
                .filter(p -> {
                    StageResult<Message> r = results.get(p.msg().getMessageId());
                    return r == null || r.isFailed();
                })
                .toList();

        if (failed.isEmpty()) {
            return Uni.createFrom().item(results);
        }

        fallbacks.increment(failed.size());
        log.warn("Batch persist had {} failed row(s) — retrying individually to isolate the culprit", failed.size());

        // Own, mutable copy — never assume the map returned across the OutboxManager port is mutable.
        Map<String, StageResult<Message>> merged = new java.util.HashMap<>(results);
        return Multi.createFrom().iterable(failed)
                .onItem().transformToUniAndConcatenate(p ->
                        outbox.save(p.msg()).onItem().invoke(r -> merged.put(p.msg().getMessageId(), r)))
                .collect().asList()
                .replaceWith(merged);
    }
}
