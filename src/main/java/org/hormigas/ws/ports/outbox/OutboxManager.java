package org.hormigas.ws.ports.outbox;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.stage.StageResult;

import java.util.List;
import java.util.Map;

public interface OutboxManager<T> {
    Uni<StageResult<T>> save(T message);

    /**
     * Group-commit variant of {@link #save}: persist many messages' {@code history+outbox} rows in a
     * single transaction (plan B). Returns a map keyed by {@code messageId} → that message's
     * {@link StageResult} (UPDATED with the DB-assigned outbox id on success, FAILED otherwise), so
     * the caller can resolve each message's pipeline independently. Matching is by {@code messageId}
     * (the RETURNING order is not guaranteed). On a transaction failure the whole batch is FAILED;
     * the caller is expected to fall back to per-message {@link #save} to isolate the culprit.
     */
    Uni<Map<String, StageResult<T>>> saveBatch(List<T> messages);

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
