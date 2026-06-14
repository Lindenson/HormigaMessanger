package org.hormigas.ws.ports.tetris;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.stage.StageResult;

import java.util.List;

public interface TetrisMarker<T> {
    Uni<StageResult<T>> onSent(T message);
    Uni<StageResult<T>> onAck(T message);
    Uni<StageResult<T>> onDisconnect(String clientId);
    Uni<Long> computeGlobalSafeDeleteId();
    Uni<List<String>> findHeavyClients(int threshold, int limit);
}
