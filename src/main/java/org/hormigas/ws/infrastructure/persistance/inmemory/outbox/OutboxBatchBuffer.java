package org.hormigas.ws.infrastructure.persistance.inmemory.outbox;

import io.smallrye.mutiny.Multi;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


@Slf4j
public class OutboxBatchBuffer {

    private static final int BATCH_SIZE = 500;
    private static final Duration FLUSH_INTERVAL = Duration.ofMillis(1000);
    private static final Duration MAX_DELAY = FLUSH_INTERVAL.multipliedBy(2);

    private final AtomicReference<ConcurrentLinkedQueue<Message>> bufferRef =
            new AtomicReference<>(new ConcurrentLinkedQueue<>());

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private final OutboxManagerInMemory outboxManager;
    private volatile long lastFlushTime = System.currentTimeMillis();

    public OutboxBatchBuffer(OutboxManagerInMemory outboxManager) {
        this.outboxManager = outboxManager;
        startAutoFlush();
    }

    public StageResult<Message> add(@Nullable Message msg) {
        if (msg == null) return StageResult.failed();
        if (!canBatch()) {
            outboxManager.remove(msg)
                    .subscribe().with(
                            ok -> log.debug("Directly removed {}", msg.getMessageId()),
                            err -> log.error("Direct remove failed", err)
                    );
            return StageResult.passed();
        }

        var queue = bufferRef.get();
        queue.add(msg);

        if (queue.size() >= BATCH_SIZE) {
            flush();
        }

        return StageResult.passed();
    }

    public boolean canBatch() {
        var qSize = bufferRef.get().size();
        long sinceLastFlush = System.currentTimeMillis() - lastFlushTime;
        if (qSize > BATCH_SIZE * 5 || sinceLastFlush > MAX_DELAY.toMillis()) {
            log.warn("Bypassing buffer: too large {} or scheduler lag {} detected", qSize, sinceLastFlush);
            lastFlushTime = System.currentTimeMillis();
            return false;
        }
        return true;
    }

    private void flush() {
        try {
            ConcurrentLinkedQueue<Message> current = bufferRef.getAndSet(new ConcurrentLinkedQueue<>());
            if (current.isEmpty()) return;

            List<Message> batch = new ArrayList<>(current);
            log.debug("Flushing {} messages", batch.size());

            Multi.createFrom().iterable(batch)
                    .onItem().transformToUniAndConcatenate(outboxManager::remove)
                    .onFailure().invoke(err -> {
                        log.error("Failed to remove batch", err);
                        bufferRef.get().addAll(batch);
                    })
                    .subscribe().with(
                            ignored -> {},
                            err -> log.error("Subscriber failed", err)
                    );

        } catch (Throwable t) {
            log.error("Unexpected error during flush()", t);
        } finally {
            lastFlushTime = System.currentTimeMillis();
        }
    }

    private void startAutoFlush() {
        scheduler.scheduleAtFixedRate(() -> {
                    try {
                        flush();
                    } catch (Throwable t) {
                        log.error("Unexpected error in Outbox auto-flush", t);
                    }
                },
                FLUSH_INTERVAL.toMillis(),
                FLUSH_INTERVAL.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }
}

