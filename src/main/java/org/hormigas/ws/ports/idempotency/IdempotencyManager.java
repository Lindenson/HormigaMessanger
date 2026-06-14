package org.hormigas.ws.ports.idempotency;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.stage.StageResult;

public interface IdempotencyManager<T> {
    Uni<StageResult<T>> add(T id);
    Uni<StageResult<T>> remove(T id);
    Uni<Boolean> isInProgress(T id);
}
