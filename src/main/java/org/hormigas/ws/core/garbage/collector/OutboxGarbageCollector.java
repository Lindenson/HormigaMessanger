package org.hormigas.ws.core.garbage.collector;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.garbage.GarbageCollector;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.outbox.OutboxManager;
import org.hormigas.ws.ports.tetris.TetrisMarker;
import org.hormigas.ws.ports.watermark.WatermarksRegistry;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class OutboxGarbageCollector implements GarbageCollector {

    private final OutboxManager<Message> outboxManager;
    private final TetrisMarker<Message> tetrisMarker;

    @Override
    public Uni<Integer> collect() {
        return tetrisMarker.computeGlobalSafeDeleteId()
                .onItem().transformToUni(outboxManager::collectGarbage)
                .onFailure().recoverWithItem(er -> {
                    log.error("Garbage collected error {}", er.getMessage());
                    return 0;
                });
    }
}
