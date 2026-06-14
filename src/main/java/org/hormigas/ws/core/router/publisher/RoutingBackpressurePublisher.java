package org.hormigas.ws.core.router.publisher;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.subscription.MultiEmitter;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.backpressure.BackpressurePublisher;
import org.hormigas.ws.core.backpressure.builder.WithBackpressure;
import org.hormigas.ws.core.backpressure.metrics.inout.OutgoingPublisherMetrics;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.core.router.OutboundRouter;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.core.feedback.events.OutgoingHealthEvent;
import org.hormigas.ws.core.feedback.provider.OutEventProvider;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hormigas.ws.core.backpressure.BackpressureBuilder.Mode.SEQUENTIAL;
import static org.hormigas.ws.core.backpressure.BackpressureBuilder.PublisherKind.OUTGOING;


@Slf4j
@ApplicationScoped
public class RoutingBackpressurePublisher implements BackpressurePublisher<Message> {

    private final AtomicReference<MultiEmitter<? super Message>> emitter = new AtomicReference<>();
    private OutgoingPublisherMetrics metrics;

    private final AtomicBoolean ready = new AtomicBoolean(Boolean.FALSE);
    private final AtomicInteger queueSize = new AtomicInteger(0);

    @Inject
    MeterRegistry meterRegistry;

    @Inject
    OutboundRouter<Message> pipelineRouter;

    @Inject
    MessengerConfig messengerConfig;

    @Inject
    OutEventProvider<OutgoingHealthEvent> eventsProvider;

    @PostConstruct
    void init() {
        this.metrics = new OutgoingPublisherMetrics(meterRegistry, eventsProvider);

        WithBackpressure.<Message, OutgoingPublisherMetrics>builder()
                .withPublisherKind(OUTGOING)
                .withSink(pipelineRouter::routeOut)
                .withMetrics(metrics)
                .withQueueSizeCounter(queueSize)
                .withEmitter(emitter)
                .withMode(SEQUENTIAL)
                .build()
                .subscribe().with(
                        ignored -> Log.debug("Publishing messages!"),
                        failure -> {
                            queueSize.set(0);
                            metrics.resetQueueSize();
                            Log.error("Processor terminated unexpectedly", failure);
                        }
                );
        ready.set(true);
    }

    @Override
    public void publish(Message msg) {
        if (!ready.get()) {
            Log.warn("Not initialized");
            return;
        }
        if (queueIsFull()) {
            metrics.recordDropped();
            Log.warn("Message dropped due to limit");
            return;
        }
        Log.debug("Message was published");
        emitter.get().emit(msg);
    }

    @Override
    public boolean queueIsNotEmpty() {
        if (queueSize.get() > 0) {
            log.debug("Queue is not empty");
            return true;
        }
        return false;
    }

    @Override
    public boolean queueIsFull() {
        metrics.updateQueueSize(queueSize.get());
        if (queueSize.incrementAndGet() > messengerConfig.outbound().queueSize()) {
            log.debug("Queue is full");
            queueSize.decrementAndGet();
            return true;
        }
        return false;
    }
}
