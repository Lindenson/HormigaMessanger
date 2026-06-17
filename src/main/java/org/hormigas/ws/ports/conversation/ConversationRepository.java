package org.hormigas.ws.ports.conversation;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.conversation.Conversation;

import java.util.List;

/**
 * Driven port for conversation persistence. Implemented by an infrastructure adapter.
 */
public interface ConversationRepository {

    /** The conversation for a participant pair, or {@code null} if none exists. */
    Uni<Conversation> findByPair(String clientId, String masterId);

    /** A conversation by id, or {@code null}. */
    Uni<Conversation> findById(String id);

    /** All conversations the given identity participates in (most-recent activity first). */
    Uni<List<Conversation>> findByParticipant(String userId);

    /**
     * Insert a conversation. Idempotent on the (clientId, masterId) pair: if one already exists,
     * the existing row is returned instead of creating a duplicate.
     */
    Uni<Conversation> insertIfAbsent(Conversation conversation);
}
