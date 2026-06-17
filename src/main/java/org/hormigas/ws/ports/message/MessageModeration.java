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

    /** Freeze all (currently non-frozen) messages of a conversation; returns the number frozen. */
    Uni<Integer> freezeConversation(String conversationId);

    enum DeleteOutcome { DELETED, NOT_FOUND, FROZEN }
}
