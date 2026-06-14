package org.hormigas.ws.core.backpressure.publisher.inout;

import io.smallrye.mutiny.Uni;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.backpressure.metrics.PublisherMetrics;
import org.hormigas.ws.core.backpressure.publisher.PublisherTemplate;
import org.hormigas.ws.domain.message.MessageEnvelope;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Slf4j
public class IncomingPublisher<T, M extends PublisherMetrics>
        extends PublisherTemplate<T, M> {

    public IncomingPublisher(Function<T, Uni<MessageEnvelope<T>>> sink,
                             M metrics,
                             AtomicInteger size) {
        super(sink, metrics, size);
    }

    @Override
    public Uni<Void> publishMessage(T msg) {
        return getSink().apply(msg)
                .onItem().invoke(processed -> {
                    if (processed.isProcessed()) {
                        getMetrics().recordDone();
                        log.debug("Incoming message processed");
                    }
                })
                .onFailure().invoke(failure -> {
                    getMetrics().recordFailed();
                    log.error("Failed to processed message", failure);
                })
                .replaceWithVoid()
                .eventually(() -> getMetrics().updateQueueSize(getQueueSizeContainer().decrementAndGet()));
    }

}
