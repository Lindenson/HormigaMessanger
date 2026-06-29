package org.hormigas.ws.core.conversation;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.conversation.CreateResult;
import org.hormigas.ws.domain.conversation.Outcome;
import org.hormigas.ws.domain.conversation.SendCheck;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.conversation.ConversationManager;
import org.hormigas.ws.ports.history.History;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the chat use case (no infra). Verifies FR-CHAT-02 (idempotent create),
 * membership/authz outcomes, and the send-guard (UC-H07 / FR-MSG-01).
 */
class ConversationsTest {

    private static Conversation conv(boolean clientBlocked, boolean masterBlocked) {
        Instant now = Instant.now();
        return new Conversation("conv-1", "client-1", "master-1", Map.of(),
                clientBlocked, masterBlocked, now, now);
    }

    private static Conversation convDeleted(String deletedFromClient, String deletedFromMaster) {
        Instant now = Instant.now();
        return new Conversation("conv-1", "client-1", "master-1", Map.of(),
                false, false, deletedFromClient, deletedFromMaster, now, now);
    }

    private Conversations service(StubRepo repo) {
        Conversations s = new Conversations();
        s.repository = repo;
        s.idGenerator = () -> "gen-1";
        // cache reads delegate straight to the repo (no caching under unit test); invalidate is a no-op
        s.directory = new org.hormigas.ws.ports.conversation.ConversationDirectory() {
            public io.smallrye.mutiny.Uni<org.hormigas.ws.domain.conversation.Conversation> findById(String id) {
                return repo.findById(id);
            }
            public void invalidate(String id) { }
        };
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

    // ── history "delete for me" watermark (D3) ──────────────────────────────────

    @Test
    void history_no_watermark_reads_from_since() {
        StubRepo repo = new StubRepo(); repo.byId = conv(false, false);
        StubHistory hist = new StubHistory();
        Conversations s = service(repo); s.history = hist;
        var page = s.history("conv-1", "client-1", null, null, false).await().indefinitely();
        assertEquals(Outcome.OK, page.outcome());
        assertEquals(2, page.value().size());
        assertNull(hist.lastSince, "no watermark and no since → read from the start");
    }

    @Test
    void history_floored_at_the_callers_delete_watermark() {
        StubRepo repo = new StubRepo(); repo.byId = convDeleted("WM-CLIENT", null);
        StubHistory hist = new StubHistory();
        Conversations s = service(repo); s.history = hist;
        s.history("conv-1", "client-1", null, null, false).await().indefinitely();
        assertEquals("WM-CLIENT", hist.lastSince, "the deleter's read is floored at their watermark");
    }

    @Test
    void history_uses_the_later_of_since_and_watermark() {
        StubRepo repo = new StubRepo(); repo.byId = convDeleted("WM-CLIENT", null);
        StubHistory hist = new StubHistory();
        Conversations s = service(repo); s.history = hist;
        s.history("conv-1", "client-1", "WM-CLIENTZ", null, false).await().indefinitely(); // since after wm
        assertEquals("WM-CLIENTZ", hist.lastSince);
        s.history("conv-1", "client-1", "WM-AAAA", null, false).await().indefinitely();    // since before wm
        assertEquals("WM-CLIENT", hist.lastSince, "the watermark floors a stale cursor");
    }

    @Test
    void history_includeDeleted_bypasses_the_watermark() {
        StubRepo repo = new StubRepo(); repo.byId = convDeleted("WM-CLIENT", null);
        StubHistory hist = new StubHistory();
        Conversations s = service(repo); s.history = hist;
        s.history("conv-1", "client-1", null, null, true).await().indefinitely();
        assertNull(hist.lastSince, "includeDeleted ignores the watermark and reads from since");
    }

    @Test
    void history_peer_who_did_not_delete_is_unfiltered() {
        StubRepo repo = new StubRepo(); repo.byId = convDeleted("WM-CLIENT", null); // only client deleted
        StubHistory hist = new StubHistory();
        Conversations s = service(repo); s.history = hist;
        s.history("conv-1", "master-1", null, null, false).await().indefinitely();
        assertNull(hist.lastSince, "the peer who didn't delete has no floor");
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

    @Test
    void canSend_allowed_even_when_a_side_deleted_the_chat() {
        StubRepo repo = new StubRepo();
        // delete is "delete for me", not a messaging stop — both sides may still send
        repo.byId = convDeleted("WM-CLIENT", null);
        assertEquals(SendCheck.ALLOW, service(repo).canSend("conv-1", "master-1").await().indefinitely());
        assertEquals(SendCheck.ALLOW, service(repo).canSend("conv-1", "client-1").await().indefinitely());
    }

    // ── evaluateSend (S1: carries the conversation so the caller can derive the authentic recipient) ──

    @Test
    void evaluateSend_allow_returns_conversation() {
        StubRepo repo = new StubRepo(); repo.byId = conv(false, false);
        var decision = service(repo).evaluateSend("conv-1", "master-1").await().indefinitely();
        assertEquals(SendCheck.ALLOW, decision.check());
        assertNotNull(decision.conversation(), "ALLOW must carry the resolved conversation");
        assertEquals("client-1", decision.conversation().clientId());
        assertEquals("master-1", decision.conversation().masterId());
    }

    @Test
    void evaluateSend_nonAllow_has_null_conversation() {
        StubRepo repo = new StubRepo(); repo.byId = conv(false, false);
        assertNull(service(repo).evaluateSend("conv-1", "outsider").await().indefinitely().conversation(),
                "NOT_MEMBER must not leak a conversation");
        repo.byId = conv(true, false);
        assertNull(service(repo).evaluateSend("conv-1", "master-1").await().indefinitely().conversation(),
                "BLOCKED must not leak a conversation");
        repo.byId = null;
        assertNull(service(repo).evaluateSend("conv-1", "master-1").await().indefinitely().conversation());
    }

    /** Configurable in-memory stub. */
    static class StubRepo implements ConversationManager {
        Conversation pair;       // findByPair result
        Conversation byId;       // findById result
        Conversation inserted;   // captured insert
        public Uni<Conversation> findByPair(String c, String m) { return Uni.createFrom().item(pair); }
        public Uni<Conversation> findById(String id) { return Uni.createFrom().item(byId); }
        public Uni<List<Conversation>> findByParticipant(String u) { return Uni.createFrom().item(List.of()); }
        public Uni<Conversation> insertIfAbsent(Conversation c) { inserted = c; return Uni.createFrom().item(c); }
        public Uni<Void> hideFor(String chatId, String userId) { return Uni.createFrom().voidItem(); }
        public Uni<Void> setBlocked(String chatId, String userId, boolean b) { return Uni.createFrom().voidItem(); }
        public Uni<List<Conversation>> findAll(org.hormigas.ws.domain.conversation.ChatQuery q) { return Uni.createFrom().item(List.of()); }
        public Uni<Long> count(org.hormigas.ws.domain.conversation.ChatQuery q) { return Uni.createFrom().item(0L); }
        public Uni<org.hormigas.ws.domain.conversation.ChatStats> stats() { return Uni.createFrom().item(new org.hormigas.ws.domain.conversation.ChatStats(0, 0)); }
    }

    /** History stub that records the effective floor (since) passed in and returns two messages. */
    static class StubHistory implements History<Message> {
        String lastSince;
        boolean queried;
        private List<Message> two() { return List.of(Message.builder().build(), Message.builder().build()); }
        public Uni<List<Message>> getByRecipientId(String c) { return Uni.createFrom().item(two()); }
        public Uni<List<Message>> getBySenderId(String c) { return Uni.createFrom().item(two()); }
        public Uni<List<Message>> getAllBySenderId(String c) { return Uni.createFrom().item(two()); }
        public Uni<List<Message>> getByConversation(String id) { queried = true; return Uni.createFrom().item(two()); }
        public Uni<List<Message>> getByConversation(String id, String since, int limit) {
            queried = true; lastSince = since; return Uni.createFrom().item(two());
        }
        public void addBySenderId(String c, Message m) { }
    }
}
