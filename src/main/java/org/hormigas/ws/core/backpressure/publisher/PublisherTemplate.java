package org.hormigas.ws.core.backpressure.publisher;

import io.smallrye.mutiny.Uni;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.hormigas.ws.core.backpressure.metrics.PublisherMetrics;
import org.hormigas.ws.domain.message.MessageEnvelope;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Getter
@RequiredArgsConstructor
public abstract class PublisherTemplate<T, M extends PublisherMetrics> {
    private final Function<T, Uni<MessageEnvelope<T>>> sink;
    private final M metrics;
    private final AtomicInteger queueSizeContainer;
    public abstract Uni<Void> publishMessage(T payload);
}
