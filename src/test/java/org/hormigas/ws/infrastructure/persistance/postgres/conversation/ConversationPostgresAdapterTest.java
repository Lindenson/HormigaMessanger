package org.hormigas.ws.infrastructure.persistance.postgres.conversation;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.pgclient.PgPool;
import jakarta.inject.Inject;
import org.hormigas.ws.domain.conversation.Conversation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for {@link ConversationPostgresAdapter} against a real Postgres (Dev Services).
 * Verifies the persistence business rules behind FR-CHAT-02 (idempotent create), UC-U03
 * (soft-delete excludes only the caller's view), UC-H07 (per-side block).
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConversationPostgresAdapterTest {

    @Inject
    PgPool client;

    @Inject
    ConversationPostgresAdapter adapter;

    @BeforeAll
    void setupSchema() {
        client.query("""
                CREATE TABLE IF NOT EXISTS conversation (
                    id VARCHAR(128) PRIMARY KEY,
                    client_id VARCHAR(128) NOT NULL,
                    master_id VARCHAR(128) NOT NULL,
                    metadata_json JSONB,
                    client_blocked BOOLEAN NOT NULL DEFAULT FALSE,
                    master_blocked BOOLEAN NOT NULL DEFAULT FALSE,
                    deleted_from_client VARCHAR(128),
                    deleted_from_master VARCHAR(128),
                    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
                    CONSTRAINT uq_conversation_pair UNIQUE (client_id, master_id)
                );
                """).execute().await().indefinitely();
        // Minimal message_history (columns the watermark SQL touches + V2's NOT NULL set).
        client.query("""
                CREATE TABLE IF NOT EXISTS message_history (
                    id BIGSERIAL PRIMARY KEY,
                    message_id VARCHAR(128) NOT NULL,
                    conversation_id VARCHAR(128) NOT NULL,
                    sender_id VARCHAR(128) NOT NULL,
                    recipient_id VARCHAR(128) NOT NULL,
                    payload_json JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now() NOT NULL
                );
                """).execute().await().indefinitely();
    }

    @BeforeEach
    void cleanup() {
        client.query("TRUNCATE conversation").execute().await().indefinitely();
        client.query("TRUNCATE message_history").execute().await().indefinitely();
    }

    private Conversation newConv(String id, String c, String m, Map<String, String> meta) {
        Instant now = Instant.now();
        return new Conversation(id, c, m, meta, false, false, now, now);
    }

    /** Append a message to a conversation (messageId is the ULID cursor — lexicographic == chrono). */
    private void insertMessage(String messageId, String conversationId) {
        client.preparedQuery("""
                        INSERT INTO message_history (message_id, conversation_id, sender_id, recipient_id, payload_json)
                        VALUES ($1, $2, 'snd', 'rcp', '{}'::jsonb)""")
                .execute(io.vertx.mutiny.sqlclient.Tuple.of(messageId, conversationId))
                .await().indefinitely();
    }

    @Test
    void insertIfAbsent_creates_then_findable() {
        Conversation saved = adapter.insertIfAbsent(newConv("c1", "cli", "mas", Map.of("orderId", "o-1")))
                .await().indefinitely();
        assertEquals("c1", saved.id());
        assertEquals("o-1", saved.metadata().get("orderId"));

        assertEquals("c1", adapter.findById("c1").await().indefinitely().id());
        assertEquals("c1", adapter.findByPair("cli", "mas").await().indefinitely().id());
    }

    @Test
    void insertIfAbsent_is_idempotent_on_pair() {
        Conversation first = adapter.insertIfAbsent(newConv("c1", "cli", "mas", Map.of())).await().indefinitely();
        // a second insert for the SAME pair with a different id returns the existing row, no duplicate
        Conversation second = adapter.insertIfAbsent(newConv("c2", "cli", "mas", Map.of())).await().indefinitely();
        assertEquals(first.id(), second.id(), "same pair must resolve to the same conversation");

        Long count = client.preparedQuery("SELECT COUNT(*) c FROM conversation WHERE client_id=$1 AND master_id=$2")
                .execute(io.vertx.mutiny.sqlclient.Tuple.of("cli", "mas"))
                .await().indefinitely().iterator().next().getLong("c");
        assertEquals(1L, count);
    }

    @Test
    void findByParticipant_excludes_callers_deleted_until_a_newer_message_revives_it() {
        adapter.insertIfAbsent(newConv("c1", "cli", "mas", Map.of())).await().indefinitely();
        insertMessage("01A", "c1");

        assertEquals(1, adapter.findByParticipant("cli").await().indefinitely().size());
        assertEquals(1, adapter.findByParticipant("mas").await().indefinitely().size());

        // client deletes → watermark set to the latest messageId (01A); gone from client's list,
        // still present for master (per-side)
        adapter.hideFor("c1", "cli").await().indefinitely();
        assertTrue(adapter.findByParticipant("cli").await().indefinitely().isEmpty());
        assertEquals(1, adapter.findByParticipant("mas").await().indefinitely().size());

        // a NEW message above the watermark revives the chat for the client — no reopen logic
        insertMessage("01B", "c1");
        assertEquals(1, adapter.findByParticipant("cli").await().indefinitely().size(),
                "a message after the delete watermark brings the chat back");
    }

    @Test
    void setBlocked_flags_only_the_callers_side() {
        adapter.insertIfAbsent(newConv("c1", "cli", "mas", Map.of())).await().indefinitely();
        assertFalse(adapter.findById("c1").await().indefinitely().isBlocked());

        adapter.setBlocked("c1", "mas", true).await().indefinitely();
        Conversation blocked = adapter.findById("c1").await().indefinitely();
        assertTrue(blocked.isBlocked());
        assertTrue(blocked.masterBlocked());
        assertFalse(blocked.clientBlocked());

        adapter.setBlocked("c1", "mas", false).await().indefinitely();
        assertFalse(adapter.findById("c1").await().indefinitely().isBlocked());
    }

    @Test
    void findByPair_and_findById_return_null_when_absent() {
        assertNull(adapter.findByPair("nope", "nope").await().indefinitely());
        assertNull(adapter.findById("nope").await().indefinitely());
        assertEquals(List.of(), adapter.findByParticipant("nope").await().indefinitely());
    }

    // ── admin reads (findAll / count / stats) ───────────────────────────────────

    @Test
    void admin_findAll_filters_by_participant_blocked_and_paginates() {
        adapter.insertIfAbsent(newConv("c1", "alice", "bob", Map.of())).await().indefinitely();
        adapter.insertIfAbsent(newConv("c2", "alice", "carol", Map.of())).await().indefinitely();
        adapter.insertIfAbsent(newConv("c3", "dave", "carol", Map.of())).await().indefinitely();
        adapter.setBlocked("c2", "alice", true).await().indefinitely();

        // unfiltered: all three (admin sees everything)
        assertEquals(3, adapter.findAll(query(null, null, false, 50, 0)).await().indefinitely().size());

        // by participant (either side of the pair)
        assertEquals(2, adapter.findAll(query("alice", null, false, 50, 0)).await().indefinitely().size());
        assertEquals(2, adapter.findAll(query("carol", null, false, 50, 0)).await().indefinitely().size());

        // blocked-only
        var blocked = adapter.findAll(query(null, null, true, 50, 0)).await().indefinitely();
        assertEquals(1, blocked.size());
        assertEquals("c2", blocked.get(0).id());

        // paging: limit 2 then offset 2
        assertEquals(2, adapter.findAll(query(null, null, false, 2, 0)).await().indefinitely().size());
        assertEquals(1, adapter.findAll(query(null, null, false, 2, 2)).await().indefinitely().size());
    }

    @Test
    void admin_count_matches_filters_ignoring_paging() {
        adapter.insertIfAbsent(newConv("c1", "alice", "bob", Map.of())).await().indefinitely();
        adapter.insertIfAbsent(newConv("c2", "alice", "carol", Map.of())).await().indefinitely();
        adapter.setBlocked("c2", "carol", true).await().indefinitely();

        assertEquals(2L, adapter.count(query(null, null, false, 1, 0)).await().indefinitely());
        assertEquals(2L, adapter.count(query("alice", null, false, 1, 0)).await().indefinitely());
        assertEquals(1L, adapter.count(query(null, null, true, 1, 0)).await().indefinitely());
    }

    @Test
    void admin_stats_counts_total_and_blocked() {
        adapter.insertIfAbsent(newConv("c1", "alice", "bob", Map.of())).await().indefinitely();
        adapter.insertIfAbsent(newConv("c2", "alice", "carol", Map.of())).await().indefinitely();
        adapter.setBlocked("c2", "alice", true).await().indefinitely();

        var stats = adapter.stats().await().indefinitely();
        assertEquals(2L, stats.total());
        assertEquals(1L, stats.blocked());
    }

    private static org.hormigas.ws.domain.conversation.ChatQuery query(
            String participant, String conversationId, boolean blocked, int limit, int offset) {
        return new org.hormigas.ws.domain.conversation.ChatQuery(participant, conversationId, blocked,
                null, null, org.hormigas.ws.domain.conversation.ChatSort.UPDATED_DESC, limit, offset);
    }
}
