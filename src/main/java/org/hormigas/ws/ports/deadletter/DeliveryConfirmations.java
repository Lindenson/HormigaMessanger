package org.hormigas.ws.ports.deadletter;

import io.smallrye.mutiny.Uni;

import java.util.List;

/**
 * Records delivery confirmations (SYSTEM_ACK) for the dead-letter retract sweep (ADR-014). Backed by
 * a Redis set — cheap, like the idempotency cache. Fail-safe: losing the set means confirmations are
 * not drained, so drafts are conservatively retained (false positives), never silently lost.
 */
public interface DeliveryConfirmations {

    /** Mark a system notice's messageId as delivery-confirmed (SYSTEM_ACK). */
    Uni<Void> confirm(String messageId);

    /** Up to {@code limit} confirmed messageIds (non-destructive — removal is via {@link #clear}). */
    Uni<List<String>> peek(int limit);

    /** Remove confirmations from the set, AFTER their drafts have been deleted (drain step). */
    Uni<Void> clear(List<String> messageIds);
}
