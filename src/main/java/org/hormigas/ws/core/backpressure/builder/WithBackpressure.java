package org.hormigas.ws.core.backpressure.builder;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;
import org.hormigas.ws.core.backpressure.BackpressureBuilder;
import org.hormigas.ws.core.backpressure.factory.PublisherFactory;
import org.hormigas.ws.core.backpressure.metrics.PublisherMetrics;
import org.hormigas.ws.domain.message.MessageEnvelope;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class WithBackpressure<T, M extends PublisherMetrics> implements BackpressureBuilder<T, M, MessageEnvelope<T>> {

    AtomicReference<MultiEmitter<? super T>> emitter;
    private Function<T, Uni<MessageEnvelope<T>>> sink;
    private M metrics;
    private AtomicInteger size;
    private Mode mode;
    private PublisherKind kind;


    private WithBackpressure() {
    }

    public static <T, M extends PublisherMetrics> WithBackpressure<T, M> builder() {
        return new WithBackpressure<>();
    }

    public BackpressureBuilder<T, M, MessageEnvelope<T>> withSink(Function<T, Uni<MessageEnvelope<T>>> processor) {
        this.sink = processor;
        return this;
    }

    @Override
    public BackpressureBuilder<T, M, MessageEnvelope<T>> withMode(Mode mode) {
        this.mode = mode;
        return this;
    }

    @Override
    public BackpressureBuilder<T, M, MessageEnvelope<T>> withMetrics(M metrics) {
        this.metrics = metrics;
        return this;
    }

    @Override
    public BackpressureBuilder<T, M, MessageEnvelope<T>> withQueueSizeCounter(AtomicInteger queueSizeContainer) {
        this.size = queueSizeContainer;
        return this;
    }

    @Override
    public BackpressureBuilder<T, M, MessageEnvelope<T>> withPublisherKind(PublisherKind kind) {
        this.kind = kind;
        return this;
    }

    @Override
    public BackpressureBuilder<T, M, MessageEnvelope<T>> withEmitter(AtomicReference<MultiEmitter<? super T>> emitter) {
        this.emitter = emitter;
        return this;
    }

    @Override
    public Multi<Void> build() {

        if (sink == null || metrics == null || size == null
                || emitter == null || mode == null || kind == null) {
            throw new IllegalStateException("Dependencies not set!");
        }

        var publisher = PublisherFactory.create(kind, sink, metrics, size);

        return switch (mode) {
            case PARALLEL -> Multi.createFrom()
                    .<T>emitter(em -> emitter.set(em), BackPressureStrategy.BUFFER)
                    .onOverflow().bufferUnconditionally()
                    .onItem().transformToUniAndMerge(publisher::publishMessage);
            case SEQUENTIAL -> Multi.createFrom()
                    .<T>emitter(em -> emitter.set(em), BackPressureStrategy.BUFFER)
                    .onOverflow().bufferUnconditionally()
                    .onItem().transformToUniAndConcatenate(publisher::publishMessage);
        };
    }
}

