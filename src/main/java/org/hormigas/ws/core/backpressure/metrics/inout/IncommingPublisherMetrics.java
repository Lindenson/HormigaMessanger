package org.hormigas.ws.core.backpressure.metrics.inout;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.hormigas.ws.core.backpressure.metrics.PublisherMetrics;

import java.util.concurrent.TimeUnit;

public class IncommingPublisherMetrics implements PublisherMetrics {

    private final DistributionSummary queueSnapshot;
    private final Timer processingTimer;
    private final Counter incoming;
    private final Counter dropped;
    private final Counter failed;

    public IncommingPublisherMetrics(MeterRegistry registry) {
        this.incoming = Counter.builder("incoming_published_total").register(registry);
        this.dropped = Counter.builder("incoming_dropped_total").register(registry);
        this.failed = Counter.builder("incoming_failed_total").register(registry);
        this.queueSnapshot = DistributionSummary.builder("incoming_queue_size").register(registry);
        this.processingTimer = Timer.builder("incoming_processing_duration").register(registry);
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
        incoming.increment();
    }

    @Override
    public void recordDropped() {
        // Inbound overload is surfaced to the sender at the WS layer (overload notice), not via an
        // unconsumed health event — see WebsocketService. Here we only count it.
        dropped.increment();
    }

    @Override
    public void recordFailed() {
        failed.increment();
    }

    @Override
    public void recordProcessingTime(long nanos) {
        processingTimer.record(nanos, TimeUnit.NANOSECONDS);
    }
}
