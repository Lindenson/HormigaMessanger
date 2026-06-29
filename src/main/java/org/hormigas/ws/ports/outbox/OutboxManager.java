package org.hormigas.ws.ports.outbox;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.stage.StageResult;

import java.util.List;
import java.util.Map;

public interface OutboxManager<T> {
    /**
     * Persist a single message's {@code history+outbox} rows in its own transaction.
     *
     * @deprecated Per-message persistence is the throughput ceiling (one tx/fsync per message — see
     * {@code docs/PERFORMANCE.md}). Use {@link #saveBatch} on the hot inbound path; {@code save} is
     * retained only as the per-message fallback the batcher uses to isolate a poison row.
     */
    @Deprecated
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

    /**
     * Claim a single message from the outbox.
     *
     * @deprecated Single-row claims multiply DB round-trips; the poller drains via {@link #fetchBatch}
     * (size + time bounded). Use {@code fetchBatch}. Kept for compatibility; no hot-path caller.
     */
    @Deprecated
    Uni<T> fetch();

    Uni<List<T>> fetchBatch(int batchSize);
    Uni<Integer> collectGarbage(Long from);

    /**
     * Still-buffered (not-yet-collected) outbox rows grouped recipientId → row ids. This is the
     * durable pending set used to rehydrate the safe-delete marker after state loss.
     */
    Uni<Map<String, List<Long>>> pendingByRecipient();
}
