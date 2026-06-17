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
                    client_hidden  BOOLEAN NOT NULL DEFAULT FALSE,
                    master_hidden  BOOLEAN NOT NULL DEFAULT FALSE,
                    client_blocked BOOLEAN NOT NULL DEFAULT FALSE,
                    master_blocked BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
                    CONSTRAINT uq_conversation_pair UNIQUE (client_id, master_id)
                );
                """).execute().await().indefinitely();
    }

    @BeforeEach
    void cleanup() {
        client.query("TRUNCATE conversation").execute().await().indefinitely();
    }

    private Conversation newConv(String id, String c, String m, Map<String, String> meta) {
        Instant now = Instant.now();
        return new Conversation(id, c, m, meta, false, false, now, now);
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
    void findByParticipant_lists_for_both_and_excludes_caller_hidden() {
        adapter.insertIfAbsent(newConv("c1", "cli", "mas", Map.of())).await().indefinitely();

        assertEquals(1, adapter.findByParticipant("cli").await().indefinitely().size());
        assertEquals(1, adapter.findByParticipant("mas").await().indefinitely().size());

        // client hides → gone from client's list, still present for master
        adapter.hideFor("c1", "cli").await().indefinitely();
        assertTrue(adapter.findByParticipant("cli").await().indefinitely().isEmpty());
        assertEquals(1, adapter.findByParticipant("mas").await().indefinitely().size());
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
}
