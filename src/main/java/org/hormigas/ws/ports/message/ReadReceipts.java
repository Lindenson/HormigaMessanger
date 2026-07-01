package org.hormigas.ws.ports.message;

import io.smallrye.mutiny.Uni;

import java.util.List;

/**
 * Driven port for read receipts (UC-U14). The recipient marks their received messages READ;
 * either participant can read the per-message status back (SENT → READ). Kept separate from the
 * message body so the receipt status is a lightweight projection, not a change to the wire model.
 */
public interface ReadReceipts {

    /**
     * Mark a message DELIVERED once it has been pushed to the online recipient (UC-U13, decision #2:
     * DELIVERED on delivery). Guarded SENT→DELIVERED only, so it never downgrades a message already READ.
     * Returns the count newly marked (0 if not found or already DELIVERED/READ).
     */
    Uni<Integer> markDelivered(String messageId);

    /** Mark all messages addressed to {@code readerId} in the conversation as READ; returns the count newly marked. */
    Uni<Integer> markRead(String conversationId, String readerId);

    /**
     * Group-commit variant: mark READ for a batch of {@code (conversationId, readerId)} ops in a SINGLE
     * transaction, returning the newly-marked count per op, in order. Used by the read-status batcher so
     * READ receipts — like inbound persistence — hit the DB only in batches, never row-per-event.
     */
    Uni<List<Integer>> markReadBatch(List<MarkRead> ops);

    /** Per-message status for a conversation (oldest-first). */
    Uni<List<Receipt>> receipts(String conversationId);

    record Receipt(String messageId, String status) {}

    /** One read-mark operation: everything {@code readerId} received in {@code conversationId}. */
    record MarkRead(String conversationId, String readerId) {}
}
