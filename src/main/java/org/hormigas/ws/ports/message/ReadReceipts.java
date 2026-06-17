package org.hormigas.ws.ports.message;

import io.smallrye.mutiny.Uni;

import java.util.List;

/**
 * Driven port for read receipts (UC-U14). The recipient marks their received messages READ;
 * either participant can read the per-message status back (SENT → READ). Kept separate from the
 * message body so the receipt status is a lightweight projection, not a change to the wire model.
 */
public interface ReadReceipts {

    /** Mark all messages addressed to {@code readerId} in the conversation as READ; returns the count newly marked. */
    Uni<Integer> markRead(String conversationId, String readerId);

    /** Per-message status for a conversation (oldest-first). */
    Uni<List<Receipt>> receipts(String conversationId);

    record Receipt(String messageId, String status) {}
}
