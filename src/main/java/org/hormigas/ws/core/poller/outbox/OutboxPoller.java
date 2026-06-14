package org.hormigas.ws.core.poller.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.smallrye.mutiny.Uni;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.backpressure.BackpressurePublisher;
import org.hormigas.ws.core.feedback.Regulator;
import org.hormigas.ws.core.poller.BatchPoller;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.outbox.OutboxManager;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class OutboxPoller implements BatchPoller {

    private final BackpressurePublisher<Message> publisher;
    private final OutboxManager<Message> outboxManager;
    private final int batchSize;
    private final Timer pollTimer;
    private final Regulator regulator;
    private final AtomicLong currentDelayMs = new AtomicLong(0);
    private final AtomicInteger lastBatchSize = new AtomicInteger(0);
    private final Counter skippedCounter;
    private final Counter pollCounter;


    @Builder
    public OutboxPoller(BackpressurePublisher<Message> publisher,
                        OutboxManager<Message> outboxManager,
                        MeterRegistry registry,
                        Regulator regulator,
                        int batchSize) {

        this.publisher = publisher;
        this.outboxManager = outboxManager;
        this.batchSize = batchSize;
        this.regulator = regulator;

        pollCounter = Counter.builder("routing_scheduler_polls_total")
                .description("Total number of scheduler polls")
                .register(registry);

        skippedCounter = Counter.builder("routing_scheduler_skipped_total")
                .description("Number of skipped fetches due to non-empty queue")
                .register(registry);

        pollTimer = Timer.builder("routing_scheduler_poll_duration")
                .description("Duration of each polling operation")
                .register(registry);


        Gauge.builder("routing_scheduler_current_delay_ms", currentDelayMs, AtomicLong::get)
                .description("Current delay interval in milliseconds (regulated)")
                .register(registry);

        Gauge.builder("routing_scheduler_last_batch_size", lastBatchSize, AtomicInteger::get)
                .description("Number of messages processed in the last batch")
                .register(registry);
    }


    @Override
    public Uni<Void> poll() {
        long delayMs = regulator.getCurrentIntervalMs().toMillis();
        currentDelayMs.set(delayMs);
        pollCounter.increment();

        return Uni.createFrom().voidItem()
                .onItem().delayIt().by(Duration.ofMillis(delayMs))
                .invoke(() -> log.debug("Polling outbox (interval={} ms)", delayMs))
                .call(this::timedPullMessages);
    }

    private Uni<Void> timedPullMessages() {
        return Uni.createFrom().item(System.nanoTime())
                .call(startTime -> pullMessagesToProcess()
                        .eventually(() -> {
                            long durationNs = System.nanoTime() - startTime;
                            pollTimer.record(durationNs, java.util.concurrent.TimeUnit.NANOSECONDS);
                            return Uni.createFrom().voidItem();
                        })
                )
                .replaceWithVoid();
    }

    private Uni<Void> pullMessagesToProcess() {
        if (publisher.queueIsNotEmpty()) {
            log.warn("Queue is not empty. Skipping fetch.");
            skippedCounter.increment();
            return Uni.createFrom().voidItem();
        }

        log.debug("Fetching messages for dispatch");
        return outboxManager.fetchBatch(batchSize).invoke(messages -> {
            lastBatchSize.set(messages.size());
            messages.forEach(publisher::publish);
        }).replaceWithVoid();
    }
}
