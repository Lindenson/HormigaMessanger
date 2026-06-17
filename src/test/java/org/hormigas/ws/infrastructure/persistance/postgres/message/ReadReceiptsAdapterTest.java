package org.hormigas.ws.infrastructure.persistance.postgres.message;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.inject.Inject;
import org.hormigas.ws.ports.message.ReadReceipts.Receipt;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for {@link ReadReceiptsAdapter} (UC-U13/U14) against real Postgres.
 * The recipient marks their received messages READ; status reads back per message (SENT → READ),
 * and a second mark is idempotent (0 newly marked).
 */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReadReceiptsAdapterTest {

    @Inject PgPool client;
    @Inject ReadReceiptsAdapter adapter;

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
                    status VARCHAR(16) NOT NULL DEFAULT 'SENT'
                );
                """).execute().await().indefinitely();
        // Another @QuarkusTest may have created message_history first (without status) on the
        // shared Dev Services DB — ensure the column exists regardless of test order.
        client.query("ALTER TABLE message_history ADD COLUMN IF NOT EXISTS status VARCHAR(16) NOT NULL DEFAULT 'SENT'")
                .execute().await().indefinitely();
    }

    @BeforeEach
    void cleanup() {
        client.query("TRUNCATE message_history RESTART IDENTITY").execute().await().indefinitely();
    }

    private void insert(String convId, String msgId, String sender, String recipient) {
        client.preparedQuery("""
                INSERT INTO message_history (message_id, conversation_id, sender_id, recipient_id, payload_json)
                VALUES ($1,$2,$3,$4,'{}'::jsonb)""")
                .execute(Tuple.of(msgId, convId, sender, recipient)).await().indefinitely();
    }

    @Test
    void markRead_marks_only_the_readers_messages() {
        insert("conv", "m1", "master", "client"); // addressed to client
        insert("conv", "m2", "client", "master"); // addressed to master
        Integer n = adapter.markRead("conv", "client").await().indefinitely();
        assertEquals(1, n, "only the message addressed to the client is marked");

        List<Receipt> receipts = adapter.receipts("conv").await().indefinitely();
        assertEquals("READ", receipts.stream().filter(r -> r.messageId().equals("m1")).findFirst().orElseThrow().status());
        assertEquals("SENT", receipts.stream().filter(r -> r.messageId().equals("m2")).findFirst().orElseThrow().status());
    }

    @Test
    void markRead_is_idempotent() {
        insert("conv", "m1", "master", "client");
        assertEquals(1, adapter.markRead("conv", "client").await().indefinitely());
        assertEquals(0, adapter.markRead("conv", "client").await().indefinitely(), "already-READ messages are not re-counted");
    }

    @Test
    void receipts_returns_all_messages_oldest_first() {
        insert("conv", "m1", "master", "client");
        insert("conv", "m2", "client", "master");
        List<Receipt> receipts = adapter.receipts("conv").await().indefinitely();
        assertEquals(2, receipts.size());
        assertEquals("m1", receipts.get(0).messageId());
        assertEquals("m2", receipts.get(1).messageId());
    }
}
