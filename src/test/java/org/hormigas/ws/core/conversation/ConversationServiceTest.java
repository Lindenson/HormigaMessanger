package org.hormigas.ws.core.conversation;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.conversation.ConversationService.CreateResult;
import org.hormigas.ws.core.conversation.ConversationService.Outcome;
import org.hormigas.ws.core.conversation.ConversationService.SendCheck;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.ports.conversation.ConversationRepository;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the chat use case (no infra). Verifies FR-CHAT-02 (idempotent create),
 * membership/authz outcomes, and the send-guard (UC-H07 / FR-MSG-01).
 */
class ConversationServiceTest {

    private static Conversation conv(boolean clientBlocked, boolean masterBlocked) {
        Instant now = Instant.now();
        return new Conversation("conv-1", "client-1", "master-1", Map.of(),
                clientBlocked, masterBlocked, now, now);
    }

    private ConversationService service(StubRepo repo) {
        ConversationService s = new ConversationService();
        s.repository = repo;
        s.idGenerator = () -> "gen-1";
        return s;
    }

    // ── createChat (idempotent) ────────────────────────────────────────────────

    @Test
    void createChat_new_pair_creates() {
        StubRepo repo = new StubRepo();           // findByPair → null (no existing)
        CreateResult r = service(repo).createChat("client-1", "master-1", Map.of("orderId", "o-1"))
                .await().indefinitely();
        assertTrue(r.created());
        assertEquals("gen-1", r.conversation().id());
        assertEquals("client-1", r.conversation().clientId());
        assertEquals("o-1", r.conversation().metadata().get("orderId"));
        assertNotNull(repo.inserted);
    }

    @Test
    void createChat_existing_pair_returns_existing() {
        StubRepo repo = new StubRepo();
        repo.pair = conv(false, false);           // an existing conversation for the pair
        CreateResult r = service(repo).createChat("client-1", "master-1", Map.of()).await().indefinitely();
        assertFalse(r.created());
        assertEquals("conv-1", r.conversation().id());
        assertNull(repo.inserted, "must not insert when one already exists");
    }

    // ── hide / block outcomes (membership-checked) ──────────────────────────────

    @Test
    void hide_and_block_outcomes() {
        StubRepo repo = new StubRepo();
        repo.byId = null;
        assertEquals(Outcome.NOT_FOUND, service(repo).hide("conv-1", "master-1").await().indefinitely());

        repo.byId = conv(false, false);
        assertEquals(Outcome.FORBIDDEN, service(repo).hide("conv-1", "outsider").await().indefinitely());
        assertEquals(Outcome.OK, service(repo).hide("conv-1", "master-1").await().indefinitely());
        assertEquals(Outcome.OK, service(repo).setBlocked("conv-1", "client-1", true).await().indefinitely());
        assertEquals(Outcome.FORBIDDEN, service(repo).setBlocked("conv-1", "outsider", true).await().indefinitely());
    }

    // ── send-guard (canSend) ────────────────────────────────────────────────────

    @Test
    void canSend_allow_when_participant_and_not_blocked() {
        StubRepo repo = new StubRepo(); repo.byId = conv(false, false);
        assertEquals(SendCheck.ALLOW, service(repo).canSend("conv-1", "master-1").await().indefinitely());
    }

    @Test
    void canSend_noConversation_blank_or_missing() {
        StubRepo repo = new StubRepo(); repo.byId = conv(false, false);
        assertEquals(SendCheck.NO_CONVERSATION, service(repo).canSend("  ", "master-1").await().indefinitely());
        repo.byId = null;
        assertEquals(SendCheck.NO_CONVERSATION, service(repo).canSend("conv-1", "master-1").await().indefinitely());
    }

    @Test
    void canSend_notMember_and_blocked() {
        StubRepo repo = new StubRepo(); repo.byId = conv(false, false);
        assertEquals(SendCheck.NOT_MEMBER, service(repo).canSend("conv-1", "outsider").await().indefinitely());
        repo.byId = conv(true, false);
        assertEquals(SendCheck.BLOCKED, service(repo).canSend("conv-1", "master-1").await().indefinitely());
    }

    /** Configurable in-memory stub. */
    static class StubRepo implements ConversationRepository {
        Conversation pair;       // findByPair result
        Conversation byId;       // findById result
        Conversation inserted;   // captured insert
        public Uni<Conversation> findByPair(String c, String m) { return Uni.createFrom().item(pair); }
        public Uni<Conversation> findById(String id) { return Uni.createFrom().item(byId); }
        public Uni<List<Conversation>> findByParticipant(String u) { return Uni.createFrom().item(List.of()); }
        public Uni<Conversation> insertIfAbsent(Conversation c) { inserted = c; return Uni.createFrom().item(c); }
        public Uni<Void> hideFor(String chatId, String userId) { return Uni.createFrom().voidItem(); }
        public Uni<Void> setBlocked(String chatId, String userId, boolean b) { return Uni.createFrom().voidItem(); }
    }
}
