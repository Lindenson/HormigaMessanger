package org.hormigas.ws.core.conversation;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.conversation.ConversationService.SendCheck;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.ports.conversation.ConversationRepository;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for the send-guard (UC-H07 send-side / FR-MSG-01) — pure JUnit, stub repo, no infra.
 */
class ConversationServiceTest {

    private ConversationService service(Conversation toReturn) {
        ConversationService s = new ConversationService();
        s.repository = new StubRepo(toReturn);
        return s;
    }

    private Conversation conv(boolean clientBlocked, boolean masterBlocked) {
        Instant now = Instant.now();
        return new Conversation("conv-1", "client-1", "master-1", Map.of(),
                clientBlocked, masterBlocked, now, now);
    }

    @Test
    void allow_when_participant_and_not_blocked() {
        assertEquals(SendCheck.ALLOW,
                service(conv(false, false)).canSend("conv-1", "master-1").await().indefinitely());
    }

    @Test
    void noConversation_when_blank_id() {
        assertEquals(SendCheck.NO_CONVERSATION,
                service(conv(false, false)).canSend("  ", "master-1").await().indefinitely());
    }

    @Test
    void noConversation_when_not_found() {
        assertEquals(SendCheck.NO_CONVERSATION,
                service(null).canSend("conv-1", "master-1").await().indefinitely());
    }

    @Test
    void notMember_when_sender_is_outsider() {
        assertEquals(SendCheck.NOT_MEMBER,
                service(conv(false, false)).canSend("conv-1", "outsider-9").await().indefinitely());
    }

    @Test
    void blocked_when_either_side_blocked() {
        assertEquals(SendCheck.BLOCKED,
                service(conv(true, false)).canSend("conv-1", "master-1").await().indefinitely());
        assertEquals(SendCheck.BLOCKED,
                service(conv(false, true)).canSend("conv-1", "client-1").await().indefinitely());
    }

    /** Minimal stub: findById returns the canned conversation; other methods unused. */
    private record StubRepo(Conversation byId) implements ConversationRepository {
        public Uni<Conversation> findByPair(String c, String m) { return Uni.createFrom().item(byId); }
        public Uni<Conversation> findById(String id) { return Uni.createFrom().item(byId); }
        public Uni<List<Conversation>> findByParticipant(String u) { return Uni.createFrom().item(List.of()); }
        public Uni<Conversation> insertIfAbsent(Conversation c) { return Uni.createFrom().item(c); }
        public Uni<Void> hideFor(String chatId, String userId) { return Uni.createFrom().voidItem(); }
        public Uni<Void> setBlocked(String chatId, String userId, boolean b) { return Uni.createFrom().voidItem(); }
    }
}
