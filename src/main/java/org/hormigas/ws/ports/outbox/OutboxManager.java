package org.hormigas.ws.ports.outbox;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.stage.StageResult;

import java.util.List;

public interface OutboxManager<T> {
    Uni<StageResult<T>> save(T message);
    Uni<StageResult<T>> remove(T message);
    Uni<T> fetch();
    Uni<List<T>> fetchBatch(int batchSize);
    Uni<Integer> collectGarbage(Long from);
}
