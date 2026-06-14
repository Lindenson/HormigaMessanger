package org.hormigas.ws.core.backpressure.publisher.inout;

import io.smallrye.mutiny.Uni;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.backpressure.metrics.PublisherMetrics;
import org.hormigas.ws.core.backpressure.publisher.PublisherTemplate;
import org.hormigas.ws.domain.message.MessageEnvelope;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Slf4j
public class OutgoingPublisher<T, M extends PublisherMetrics> extends PublisherTemplate<T, M> {

    public OutgoingPublisher(Function<T, Uni<MessageEnvelope<T>>> sink,
                             M metrics,
                             AtomicInteger queueSizeContainer) {
        super(sink, metrics, queueSizeContainer);
    }

    @Override
    public Uni<Void> publishMessage(T msg) {
        long start = System.nanoTime();
        return getSink().apply(msg)
                .onItem().invoke(processed -> {
                    if (processed.isProcessed()) {
                        log.debug("Outgoing message processed");
                        getMetrics().recordProcessingTime(System.nanoTime() - start);
                        getMetrics().recordDone();
                    }
                })
                .onFailure().invoke(failure -> {
                    getMetrics().recordFailed();
                    log.error("Failed to process outgoing message", failure);
                })
                .replaceWithVoid()
                .eventually(() -> getMetrics().updateQueueSize(getQueueSizeContainer().decrementAndGet()));
    }
}
