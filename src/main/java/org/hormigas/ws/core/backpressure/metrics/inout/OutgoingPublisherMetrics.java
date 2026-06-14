package org.hormigas.ws.core.backpressure.metrics.inout;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.hormigas.ws.core.backpressure.metrics.PublisherMetrics;
import org.hormigas.ws.core.feedback.provider.OutEventProvider;
import org.hormigas.ws.core.feedback.events.OutgoingHealthEvent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class OutgoingPublisherMetrics implements PublisherMetrics {

    private final DistributionSummary queueSnapshot;
    private final Timer processingTimer;
    private final Counter outgoing;
    private final Counter dropped;
    private final Counter failed;
    private final OutEventProvider<OutgoingHealthEvent> eventProvider;


    private final AtomicInteger dropSplash = new AtomicInteger(0);

    public OutgoingPublisherMetrics(MeterRegistry registry,
                                    OutEventProvider<OutgoingHealthEvent> eventProvider)
    {
        this.processingTimer = Timer.builder("outgoing_processing_duration").register(registry);
        this.outgoing = Counter.builder("outgoing_published_total").register(registry);
        this.dropped = Counter.builder("outgoing_dropped_total").register(registry);
        this.failed = Counter.builder("outgoing_failed_total").register(registry);
        this.queueSnapshot = DistributionSummary.builder("outgoing_queue_size").register(registry);
        this.eventProvider = eventProvider;
    }

    @Override
    public void updateQueueSize(int newSize) {
        queueSnapshot.record(newSize);
    }

    @Override
    public void resetQueueSize() {
        queueSnapshot.record(0);
    }

    @Override
    public void recordDone() {
        if (dropSplash.get() > 0) {
            eventProvider.fireOut(new OutgoingHealthEvent(false, dropSplash.decrementAndGet()));
        }
        outgoing.increment();
    }

    @Override
    public void recordDropped() {
        dropped.increment();
        eventProvider.fireOut(new OutgoingHealthEvent(true, dropSplash.incrementAndGet()));
    }

    @Override
    public void recordFailed() {
        failed.increment();
    }

    public void recordProcessingTime(long nanos) {
        processingTimer.record(nanos, TimeUnit.NANOSECONDS);
    }
}
