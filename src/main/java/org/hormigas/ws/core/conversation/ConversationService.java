package org.hormigas.ws.core.conversation;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.ports.conversation.ConversationRepository;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * The universal "create chat" + chat-query use case (core). Trigger-agnostic — invoked by the
 * REST adapter and (later) the Order-event adapter over this same operation.
 */
@ApplicationScoped
public class ConversationService {

    @Inject
    ConversationRepository repository;

    @Inject
    IdGenerator idGenerator;

    /** Idempotent create: returns the existing chat for the pair, or a new one. */
    public Uni<CreateResult> createChat(String clientId, String masterId, Map<String, String> metadata) {
        return repository.findByPair(clientId, masterId)
                .flatMap(existing -> {
                    if (existing != null) {
                        return Uni.createFrom().item(new CreateResult(existing, false));
                    }
                    Instant now = Instant.now();
                    Conversation fresh = new Conversation(
                            idGenerator.generateId(), clientId, masterId,
                            metadata != null ? metadata : Map.of(), false, false, now, now);
                    return repository.insertIfAbsent(fresh)
                            .map(saved -> new CreateResult(saved, true));
                });
    }

    public Uni<List<Conversation>> listChats(String participantId) {
        return repository.findByParticipant(participantId);
    }

    public Uni<Conversation> findById(String id) {
        return repository.findById(id);
    }

    /** Soft-delete (hide) the chat for the caller; membership-checked. */
    public Uni<Outcome> hide(String chatId, String userId) {
        return guarded(chatId, userId, () -> repository.hideFor(chatId, userId));
    }

    /** Block/unblock the peer for the caller; membership-checked. */
    public Uni<Outcome> setBlocked(String chatId, String userId, boolean blocked) {
        return guarded(chatId, userId, () -> repository.setBlocked(chatId, userId, blocked));
    }

    private Uni<Outcome> guarded(String chatId, String userId, java.util.function.Supplier<Uni<Void>> action) {
        return repository.findById(chatId).flatMap(c -> {
            if (c == null) return Uni.createFrom().item(Outcome.NOT_FOUND);
            if (!c.hasParticipant(userId)) return Uni.createFrom().item(Outcome.FORBIDDEN);
            return action.get().replaceWith(Outcome.OK);
        });
    }

    /**
     * Whether {@code senderId} may send into conversation {@code conversationId} right now:
     * the conversation must exist, the sender must be a participant, and neither side may have
     * blocked the other (UC-H07 / FR-MSG-01). Enforced on the WS send path.
     */
    public Uni<SendCheck> canSend(String conversationId, String senderId) {
        if (conversationId == null || conversationId.isBlank()) {
            return Uni.createFrom().item(SendCheck.NO_CONVERSATION);
        }
        return repository.findById(conversationId).map(c -> {
            if (c == null) return SendCheck.NO_CONVERSATION;
            if (!c.hasParticipant(senderId)) return SendCheck.NOT_MEMBER;
            if (c.isBlocked()) return SendCheck.BLOCKED;
            return SendCheck.ALLOW;
        });
    }

    /** Result of an idempotent create: the chat and whether it was newly created (201 vs 200). */
    public record CreateResult(Conversation conversation, boolean created) {}

    public enum Outcome { OK, NOT_FOUND, FORBIDDEN }

    public enum SendCheck { ALLOW, NO_CONVERSATION, NOT_MEMBER, BLOCKED }
}
