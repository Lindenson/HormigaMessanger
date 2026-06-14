package org.hormigas.ws.core.backpressure;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public interface BackpressureBuilder<T, M, S> {
    BackpressureBuilder<T, M, S> withSink(Function<T, Uni<S>> processor);
    BackpressureBuilder<T, M, S> withEmitter(AtomicReference<MultiEmitter<? super T>> emitter);
    BackpressureBuilder<T, M, S> withMetrics(M metrics);
    BackpressureBuilder<T, M, S> withMode(Mode mode);
    BackpressureBuilder<T, M, S> withQueueSizeCounter(AtomicInteger queueSizeContainer);
    BackpressureBuilder<T, M, S> withPublisherKind(PublisherKind name);

    Multi<Void> build();

    enum Mode {PARALLEL, SEQUENTIAL}
    enum PublisherKind {OUTGOING, INCOMING}
}
