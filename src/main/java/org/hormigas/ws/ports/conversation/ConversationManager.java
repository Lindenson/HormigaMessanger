package org.hormigas.ws.ports.conversation;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.conversation.Conversation;

import java.util.List;

/**
 * Driven port for conversation persistence. Implemented by an infrastructure adapter.
 */
public interface ConversationManager {

    /** The conversation for a participant pair, or {@code null} if none exists. */
    Uni<Conversation> findByPair(String clientId, String masterId);

    /** A conversation by id, or {@code null}. */
    Uni<Conversation> findById(String id);

    /** Conversations the identity participates in and has NOT soft-deleted, most-recent first. */
    Uni<List<Conversation>> findByParticipant(String userId);

    /**
     * Insert a conversation. Idempotent on the (clientId, masterId) pair: if one already exists,
     * the existing row is returned instead of creating a duplicate.
     */
    Uni<Conversation> insertIfAbsent(Conversation conversation);

    /** Soft-delete (hide) the conversation for the given participant only. */
    Uni<Void> hideFor(String chatId, String userId);

    /** Set the block flag for the given participant's side. */
    Uni<Void> setBlocked(String chatId, String userId, boolean blocked);
}
