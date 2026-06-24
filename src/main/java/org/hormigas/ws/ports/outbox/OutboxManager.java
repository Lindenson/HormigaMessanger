package org.hormigas.ws.ports.outbox;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.stage.StageResult;

import java.util.List;
import java.util.Map;

public interface OutboxManager<T> {
    Uni<StageResult<T>> save(T message);

    /**
     * Persist to the outbox ONLY (no History row) — the transient delivery vehicle for Strategy C
     * system notices (ADR-014). Durability of the notice itself is the {@code dead_letter} draft,
     * not this row, so the watermark GC may reclaim it freely.
     */
    Uni<StageResult<T>> saveTransient(T message);

    Uni<StageResult<T>> remove(T message);
    Uni<T> fetch();
    Uni<List<T>> fetchBatch(int batchSize);
    Uni<Integer> collectGarbage(Long from);

    /**
     * Still-buffered (not-yet-collected) outbox rows grouped recipientId → row ids. This is the
     * durable pending set used to rehydrate the safe-delete marker after state loss.
     */
    Uni<Map<String, List<Long>>> pendingByRecipient();
}
