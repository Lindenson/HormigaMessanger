package org.hormigas.ws.core.backpressure.metrics.inout;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.hormigas.ws.core.backpressure.metrics.PublisherMetrics;
import org.hormigas.ws.core.feedback.events.IncomingHealthEvent;
import org.hormigas.ws.core.feedback.provider.InEventProvider;

import java.util.concurrent.atomic.AtomicInteger;

public class IncommingPublisherMetrics implements PublisherMetrics {

    private final DistributionSummary queueSnapshot;
    private final Counter incoming;
    private final Counter dropped;
    private final Counter failed;
    private final InEventProvider<IncomingHealthEvent> eventProvider;

    private final AtomicInteger dropSplash = new AtomicInteger(0);


    public IncommingPublisherMetrics(MeterRegistry registry,
                                     InEventProvider<IncomingHealthEvent> eventProvider) {
        this.incoming = Counter.builder("incoming_published_total").register(registry);
        this.dropped = Counter.builder("incoming_dropped_total").register(registry);
        this.failed = Counter.builder("incoming_failed_total").register(registry);
        this.queueSnapshot = DistributionSummary.builder("incoming_queue_size").register(registry);
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
            eventProvider.fireIn(new IncomingHealthEvent(false, dropSplash.decrementAndGet()));
        }
        incoming.increment();
    }

    @Override
    public void recordDropped() {
        dropped.increment();
        eventProvider.fireIn(new IncomingHealthEvent(true, dropSplash.incrementAndGet()));
    }

    @Override
    public void recordFailed() {
        failed.increment();
    }

    @Override
    public void recordProcessingTime(long nanos) {
        //not implemented
    }

}
