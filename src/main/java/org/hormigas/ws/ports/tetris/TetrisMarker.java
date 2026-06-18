package org.hormigas.ws.ports.tetris;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.stage.StageResult;

import java.util.List;
import java.util.Map;

public interface TetrisMarker<T> {
    Uni<StageResult<T>> onSent(T message);
    Uni<StageResult<T>> onAck(T message);
    Uni<StageResult<T>> onDisconnect(String clientId);
    Uni<Long> computeGlobalSafeDeleteId();
    Uni<List<String>> findHeavyClients(int threshold, int limit);

    /**
     * Whether the marker holds primed (non-lost) state. Returns false after a cold start / state
     * loss (e.g. Redis flush), signalling that the safe-delete state must be rehydrated from the
     * durable outbox before GC may advance — otherwise GC could prematurely trim undelivered rows.
     */
    Uni<Boolean> isPrimed();

    /**
     * Rebuild the pending-message state from the durable outbox (recipientId → outbox row ids) and
     * mark the state primed. Idempotent. The outbox is the source of truth for what is not yet
     * safe to delete.
     */
    Uni<Void> rehydrate(Map<String, List<Long>> pendingByRecipient);
}
