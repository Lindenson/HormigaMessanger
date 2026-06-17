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
                            metadata != null ? metadata : Map.of(), now, now);
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

    /** Result of an idempotent create: the chat and whether it was newly created (201 vs 200). */
    public record CreateResult(Conversation conversation, boolean created) {}
}
