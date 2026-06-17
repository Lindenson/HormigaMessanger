package org.hormigas.ws.ports.message;

import io.smallrye.mutiny.Uni;

/**
 * Driven port for message mutability rules (UC-U21/U22): conditional delete + freeze.
 * Messages are never edited; they can be deleted only while NOT frozen, and freezing makes them
 * permanent (a contract was reached).
 */
public interface MessageModeration {

    /** Delete a message — only if it exists and is not frozen. */
    Uni<DeleteOutcome> deleteMessage(String conversationId, String messageId);

    /**
     * Freeze the (currently non-frozen) messages of one order within a conversation; returns the
     * number frozen. Message-level only — there is NO chat-level freeze (UC-U22, decisions #3/#4):
     * a contract on one order must not freeze unrelated orders' messages in the same (order-agnostic)
     * chat.
     */
    Uni<Integer> freezeByOrder(String conversationId, String orderId);

    enum DeleteOutcome { DELETED, NOT_FOUND, FROZEN }
}
