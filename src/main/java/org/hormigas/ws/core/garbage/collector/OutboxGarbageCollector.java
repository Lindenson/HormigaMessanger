package org.hormigas.ws.core.garbage.collector;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.garbage.GarbageCollector;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.outbox.OutboxManager;
import org.hormigas.ws.ports.tetris.TetrisMarker;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class OutboxGarbageCollector implements GarbageCollector {

    private final OutboxManager<Message> outboxManager;
    private final TetrisMarker<Message> tetrisMarker;

    @Override
    public Uni<Integer> collect() {
        // Gate GC on primed safe-delete state. After a cold start / Redis state loss the marker is
        // unprimed; rehydrate the pending set from the durable outbox first so GC can never advance
        // past (and prematurely trim) undelivered rows the marker has simply forgotten.
        return tetrisMarker.isPrimed()
                .onItem().transformToUni(primed -> primed
                        ? Uni.createFrom().voidItem()
                        : outboxManager.pendingByRecipient()
                                .onItem().transformToUni(tetrisMarker::rehydrate))
                .onItem().transformToUni(ignored -> tetrisMarker.computeGlobalSafeDeleteId())
                .onItem().transformToUni(outboxManager::collectGarbage)
                .onFailure().recoverWithItem(er -> {
                    log.error("Garbage collected error {}", er.getMessage());
                    return 0;
                });
    }
}
