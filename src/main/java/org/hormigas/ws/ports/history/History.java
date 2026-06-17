package org.hormigas.ws.ports.history;

import io.smallrye.mutiny.Uni;

import java.util.List;

public interface History<T> {
    Uni<List<T>> getByRecipientId(String clientId);
    Uni<List<T>> getBySenderId(String clientId);
    Uni<List<T>> getAllBySenderId(String clientId);

    /** All messages of a conversation, oldest-first — the reconnect/history-sync read (UC-U50). */
    Uni<List<T>> getByConversation(String conversationId);

    /**
     * A page of a conversation's messages, oldest-first, after {@code sinceMessageId} (exclusive;
     * null = from the start), capped at {@code limit} — the incremental reconnect/history-sync read
     * (UC-U50). messageId is a ULID (monotonic), so it doubles as the page cursor.
     */
    Uni<List<T>> getByConversation(String conversationId, String sinceMessageId, int limit);

    void addBySenderId(String clientId, T message);
}
