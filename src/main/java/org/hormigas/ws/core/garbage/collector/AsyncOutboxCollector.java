package org.hormigas.ws.core.garbage.collector;

import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.core.garbage.AsyncGarbageCollector;
import org.hormigas.ws.core.garbage.GarbageCollector;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.outbox.OutboxManager;
import org.hormigas.ws.ports.tetris.TetrisMarker;
import org.hormigas.ws.ports.watermark.WatermarksRegistry;

@Slf4j
@ApplicationScoped
public class AsyncOutboxCollector implements AsyncGarbageCollector {

    @Inject
    OutboxManager<Message> outboxManager;

    @Inject
    TetrisMarker<Message> tetrisMarker;


    private GarbageCollector delegate;

    @PostConstruct
    void init() {
        delegate = new OutboxGarbageCollector(outboxManager, tetrisMarker);
    }

    @Override
    public void collect() {
        delegate.collect().runSubscriptionOn(Infrastructure.getDefaultExecutor()).subscribe().with(
                collected -> log.debug("Garbage collection finished, total collected: {}", collected),
                err -> log.error("Garbage collection failed", err));
    }
}
