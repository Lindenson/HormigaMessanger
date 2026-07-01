package org.hormigas.ws.infrastructure.websocket.inbound;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.subscription.MultiEmitter;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.backpressure.builder.WithBackpressure;
import org.hormigas.ws.core.backpressure.metrics.inout.IncommingPublisherMetrics;
import org.hormigas.ws.core.backpressure.BackpressurePublisher;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.core.router.InboundRouter;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.emit.ChatMessageEmitter;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hormigas.ws.core.backpressure.BackpressureBuilder.Mode.PARALLEL;
import static org.hormigas.ws.core.backpressure.BackpressureBuilder.PublisherKind.INCOMING;


@Slf4j
@ApplicationScoped
public class InboundPublisher implements BackpressurePublisher<Message>, ChatMessageEmitter {


    @Inject
    MessengerConfig messengerConfig;

    @Inject
    InboundRouter<Message> pipelineRouter;

    @Inject
    MeterRegistry meterRegistry;

    private IncommingPublisherMetrics metrics;

    private final AtomicReference<MultiEmitter<? super Message>> emitter = new AtomicReference<>();
    private final AtomicBoolean ready = new AtomicBoolean(Boolean.FALSE);
    private final AtomicInteger queueSize = new AtomicInteger(0);

    @PostConstruct
    void init() {

        metrics = new IncommingPublisherMetrics(meterRegistry);

        WithBackpressure.<Message, IncommingPublisherMetrics>builder()
                .withPublisherKind(INCOMING)
                .withSink(pipelineRouter::routeIn)
                .withQueueSizeCounter(queueSize)
                .withEmitter(emitter)
                .withMetrics(metrics)
                // PARALLEL (merge): many routeIn flows run concurrently so the persist batcher can
                // actually accumulate a batch. Under SEQUENTIAL the publisher awaited each message
                // before emitting the next, so a batch would never fill (plan B / load findings R2).
                .withMode(PARALLEL)
                .build()
                // F2: a terminal stream error must not permanently disable the publisher — re-subscribe
                // with backoff (the emitter consumer resets the counter on each (re)subscribe).
                .onFailure().invoke(f -> Log.error("Incoming publisher stream failed — retrying", f))
                .onFailure().retry().withBackOff(
                        Duration.ofMillis(messengerConfig.streamRetry().minBackoffMs()),
                        Duration.ofMillis(messengerConfig.streamRetry().maxBackoffMs())).indefinitely()
                .subscribe().with(
                        ignored -> Log.debug("Publishing incoming messages!"),
                        failure -> Log.error("Incoming publisher terminated (retries exhausted)", failure)
                );
        ready.set(true);
    }

    @Override
    public boolean publish(Message msg) {
        if (!ready.get()) {
            Log.warn("Incoming publisher not initialized");
            return false;
        }
        if (queueIsFull()) {
            metrics.recordDropped();
            Log.warn("Incoming message dropped due to limit");
            return false;
        }
        Log.debug("Incoming message was published");
        emitter.get().emit(msg);
        return true;
    }

    /** {@link ChatMessageEmitter}: route a server-originated message through the same inbound pipeline. */
    @Override
    public boolean emit(Message message) {
        return publish(message);
    }

    @Override
    public boolean queueIsNotEmpty() {
        if (queueSize.get() > 0) {
            log.debug("Incoming message queue is not empty");
            return true;
        }
        return false;
    }

    @Override
    public boolean queueIsFull() {
        metrics.updateQueueSize(queueSize.get());
        if (queueSize.incrementAndGet() > messengerConfig.inbound().queueSize()) {
            log.debug("Incoming queue is full");
            queueSize.decrementAndGet();
            return true;
        }
        return false;
    }
}
