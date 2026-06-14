package org.hormigas.ws.core.backpressure.factory;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.backpressure.metrics.PublisherMetrics;
import org.hormigas.ws.core.backpressure.publisher.inout.IncomingPublisher;
import org.hormigas.ws.core.backpressure.publisher.inout.OutgoingPublisher;
import org.hormigas.ws.core.backpressure.publisher.PublisherTemplate;
import org.hormigas.ws.domain.message.MessageEnvelope;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hormigas.ws.core.backpressure.BackpressureBuilder.PublisherKind;

public class PublisherFactory {
    public static <T, M extends PublisherMetrics>
    PublisherTemplate<T, M> create(PublisherKind kind,
                                   Function<T, Uni<MessageEnvelope<T>>> sink,
                                   M metrics, AtomicInteger queSize) {
        return switch (kind) {
            case OUTGOING -> new OutgoingPublisher<>(sink, metrics, queSize);
            case INCOMING -> new IncomingPublisher<>(sink, metrics, queSize);
        };
    }
}
