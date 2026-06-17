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
                    frozen BOOLEAN NOT NULL DEFAULT FALSE,
                    order_id VARCHAR(128)
                );
                """).execute().await().indefinitely();
        // Another @QuarkusTest may have created message_history first (without these columns) on
        // the shared Dev Services DB — ensure they exist regardless of test order.
        client.query("ALTER TABLE message_history ADD COLUMN IF NOT EXISTS frozen BOOLEAN NOT NULL DEFAULT FALSE")
                .execute().await().indefinitely();
        client.query("ALTER TABLE message_history ADD COLUMN IF NOT EXISTS order_id VARCHAR(128)")
                .execute().await().indefinitely();
    }

    @BeforeEach
    void cleanup() {
        client.query("TRUNCATE message_history RESTART IDENTITY").execute().await().indefinitely();
    }

    private void insert(String convId, String msgId, boolean frozen) {
        insert(convId, msgId, frozen, null);
    }

    private void insert(String convId, String msgId, boolean frozen, String orderId) {
        client.preparedQuery("""
                INSERT INTO message_history (message_id, conversation_id, sender_id, recipient_id, payload_json, frozen, order_id)
                VALUES ($1,$2,'s','r','{}'::jsonb,$3,$4)""")
                .execute(Tuple.of(msgId, convId, frozen, orderId)).await().indefinitely();
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
    void freezeByOrder_freezes_only_that_order_and_blocks_its_delete() {
        // one conversation, two orders — a contract on order-1 must NOT freeze order-2's messages
        insert("conv", "m1", false, "order-1");
        insert("conv", "m2", false, "order-1");
        insert("conv", "m3", false, "order-2");
        Integer n = adapter.freezeByOrder("conv", "order-1").await().indefinitely();
        assertEquals(2, n, "only the two order-1 messages are frozen");
        // order-1 messages are now immutable
        assertEquals(DeleteOutcome.FROZEN, adapter.deleteMessage("conv", "m1").await().indefinitely());
        // order-2 message in the SAME chat stays deletable (no chat-level freeze)
        assertEquals(DeleteOutcome.DELETED, adapter.deleteMessage("conv", "m3").await().indefinitely());
    }

    @Test
    void freezeByOrder_is_idempotent_and_scoped() {
        insert("conv", "m1", false, "order-1");
        assertEquals(1, adapter.freezeByOrder("conv", "order-1").await().indefinitely());
        // re-freeze marks nothing new; an unknown order freezes nothing
        assertEquals(0, adapter.freezeByOrder("conv", "order-1").await().indefinitely());
        assertEquals(0, adapter.freezeByOrder("conv", "order-x").await().indefinitely());
    }
}
