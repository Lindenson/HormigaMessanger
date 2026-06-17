package org.hormigas.ws.infrastructure.persistance.postgres.message;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.inject.Inject;
import org.hormigas.ws.ports.message.MessageModeration.DeleteOutcome;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for {@link MessageModerationAdapter} (UC-U21/U22) against real Postgres.
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MessageModerationAdapterTest {

    @Inject PgPool client;
    @Inject MessageModerationAdapter adapter;

    @BeforeAll
    void schema() {
        client.query("""
                CREATE TABLE IF NOT EXISTS message_history (
                    id BIGSERIAL PRIMARY KEY,
                    message_id VARCHAR(128) NOT NULL,
                    conversation_id VARCHAR(128) NOT NULL,
                    sender_id VARCHAR(128) NOT NULL,
                    recipient_id VARCHAR(128) NOT NULL,
                    payload_json JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
                    frozen BOOLEAN NOT NULL DEFAULT FALSE
                );
                """).execute().await().indefinitely();
        // Another @QuarkusTest may have created message_history first (without frozen) on the
        // shared Dev Services DB — ensure the column exists regardless of test order.
        client.query("ALTER TABLE message_history ADD COLUMN IF NOT EXISTS frozen BOOLEAN NOT NULL DEFAULT FALSE")
                .execute().await().indefinitely();
    }

    @BeforeEach
    void cleanup() {
        client.query("TRUNCATE message_history RESTART IDENTITY").execute().await().indefinitely();
    }

    private void insert(String convId, String msgId, boolean frozen) {
        client.preparedQuery("""
                INSERT INTO message_history (message_id, conversation_id, sender_id, recipient_id, payload_json, frozen)
                VALUES ($1,$2,'s','r','{}'::jsonb,$3)""")
                .execute(Tuple.of(msgId, convId, frozen)).await().indefinitely();
    }

    @Test
    void delete_absent_is_not_found() {
        assertEquals(DeleteOutcome.NOT_FOUND, adapter.deleteMessage("conv", "missing").await().indefinitely());
    }

    @Test
    void delete_non_frozen_succeeds() {
        insert("conv", "m1", false);
        assertEquals(DeleteOutcome.DELETED, adapter.deleteMessage("conv", "m1").await().indefinitely());
        assertEquals(DeleteOutcome.NOT_FOUND, adapter.deleteMessage("conv", "m1").await().indefinitely());
    }

    @Test
    void delete_frozen_is_rejected() {
        insert("conv", "m1", true);
        assertEquals(DeleteOutcome.FROZEN, adapter.deleteMessage("conv", "m1").await().indefinitely());
    }

    @Test
    void freezeConversation_freezes_non_frozen_and_blocks_delete() {
        insert("conv", "m1", false);
        insert("conv", "m2", false);
        insert("other", "m3", false);
        Integer n = adapter.freezeConversation("conv").await().indefinitely();
        assertEquals(2, n);
        assertEquals(DeleteOutcome.FROZEN, adapter.deleteMessage("conv", "m1").await().indefinitely());
        // a different conversation is untouched
        assertEquals(DeleteOutcome.DELETED, adapter.deleteMessage("other", "m3").await().indefinitely());
    }
}
