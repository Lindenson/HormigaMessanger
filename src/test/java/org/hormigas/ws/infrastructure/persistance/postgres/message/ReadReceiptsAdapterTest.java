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
    void markDelivered_moves_SENT_to_DELIVERED_and_never_downgrades_READ() {
        insert("conv", "m1", "master", "client"); // SENT
        assertEquals(1, adapter.markDelivered("m1").await().indefinitely());
        assertEquals("DELIVERED",
                adapter.receipts("conv").await().indefinitely().get(0).status());
        // already DELIVERED → no re-count
        assertEquals(0, adapter.markDelivered("m1").await().indefinitely());
        // once READ, a late delivery mark must not downgrade it
        adapter.markRead("conv", "client").await().indefinitely();
        assertEquals(0, adapter.markDelivered("m1").await().indefinitely());
        assertEquals("READ",
                adapter.receipts("conv").await().indefinitely().get(0).status());
    }

    @Test
    void markRead_is_idempotent() {
        insert("conv", "m1", "master", "client");
        assertEquals(1, adapter.markRead("conv", "client").await().indefinitely());
        assertEquals(0, adapter.markRead("conv", "client").await().indefinitely(), "already-READ messages are not re-counted");
    }

    @Test
    void markReadBatch_marks_each_op_in_one_transaction_returning_per_op_counts() {
        insert("cA", "a1", "master", "client");   // → client
        insert("cA", "a2", "master", "client");   // → client
        insert("cB", "b1", "master", "other");    // → other
        insert("cA", "a3", "client", "master");   // → master (not this reader)

        List<Integer> counts = adapter.markReadBatch(List.of(
                new org.hormigas.ws.ports.message.ReadReceipts.MarkRead("cA", "client"),
                new org.hormigas.ws.ports.message.ReadReceipts.MarkRead("cB", "other"),
                new org.hormigas.ws.ports.message.ReadReceipts.MarkRead("cA", "client") // duplicate → already READ
        )).await().indefinitely();

        assertEquals(List.of(2, 1, 0), counts, "per-op counts in order; the duplicate marks nothing new");
        assertEquals("READ", adapter.receipts("cA").await().indefinitely().stream()
                .filter(r -> r.messageId().equals("a1")).findFirst().orElseThrow().status());
    }

    @Test
    void markReadBatch_empty_is_noop() {
        assertEquals(List.of(), adapter.markReadBatch(List.of()).await().indefinitely());
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
