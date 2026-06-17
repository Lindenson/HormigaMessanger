package org.hormigas.ws.infrastructure.persistance.postgres.outbox;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.pgclient.PgPool;
import jakarta.inject.Inject;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.HistoryRow;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.Inserted;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxMessage;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OutboxPostgresRepositoryTest {

    @Inject
    PgPool client;

    @Inject
    OutboxPostgresRepository repo;

    @BeforeAll
    public void setupSchema() {
        client.query("""
                CREATE TABLE IF NOT EXISTS message_history (
                    id BIGSERIAL PRIMARY KEY,
                    message_id VARCHAR(128) NOT NULL,
                    conversation_id VARCHAR(128),
                    sender_id VARCHAR(128),
                    recipient_id VARCHAR(128),
                    payload_json JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
                    order_id VARCHAR(128)
                );
                CREATE INDEX idx_mh_conversation_id ON message_history(conversation_id);
                CREATE INDEX idx_mh_sender_id ON message_history(sender_id);
                CREATE INDEX idx_mh_recipient_id ON message_history(recipient_id);
                CREATE INDEX idx_mh_sender_recipient ON message_history(sender_id, recipient_id);
                CREATE TABLE IF NOT EXISTS outbox (
                    id BIGSERIAL PRIMARY KEY,
                    type VARCHAR(64) NOT NULL,
                    sender_id VARCHAR(128) NOT NULL,
                    recipient_id VARCHAR(128) NOT NULL,
                    conversation_id VARCHAR(128),
                    message_id VARCHAR(128) NOT NULL,
                    correlation_id VARCHAR(128),
                    sender_ts BIGINT NOT NULL,
                    sender_tz VARCHAR(64),
                    server_ts BIGINT NOT NULL,
                    payload_json JSONB NOT NULL,
                    meta_json JSONB,
                    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
                    lease_until TIMESTAMPTZ DEFAULT now() NOT NULL,
                    processing_attempts INT DEFAULT 0 NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_outbox_id_lease ON outbox(id, lease_until);
                """).execute().await().indefinitely();
        // Shared Dev Services DB: ensure order_id exists regardless of which test created the table.
        client.query("ALTER TABLE message_history ADD COLUMN IF NOT EXISTS order_id VARCHAR(128)")
                .execute().await().indefinitely();
    }

    @BeforeEach
    public void cleanup() {
        client.query("TRUNCATE outbox, message_history RESTART IDENTITY").execute().await().indefinitely();
    }

    private OutboxMessage sample(String sender, String recipient, String conversationId, int idx) {
        String payload = "{\"kind\":\"text\",\"body\":\"hello " + idx + "\"}";
        String meta = "{\"k\":\"v\"}";
        return new OutboxMessage(
                "CHAT_OUT",       // type
                sender,
                recipient,
                conversationId,
                "msg-" + idx,
                null,          // correlationId
                Instant.now().toEpochMilli(),
                "UTC",
                Instant.now().toEpochMilli(),
                payload,
                meta
        );
    }

    private HistoryRow sampleHistory(String conversationId, int idx) {
        String msgId = "msg-" + idx;
        String payloadJson = "{\"messageId\":\"" + msgId + "\",\"payload\":{\"kind\":\"text\",\"body\":\"hello " + idx + "\"}}";
        return new HistoryRow(msgId, conversationId, "sender-" + idx, "recipient-" + idx, null, payloadJson, Instant.now());
    }

    @Test
    public void testInsertHistoryAndOutboxTransactional() {
        String conversationId = "conv-1";
        var h = sampleHistory(conversationId, 1);
        var o = sample("s1", "r1", conversationId, 1);

        List<Inserted> res = repo.insertHistoryAndOutboxTransactional(List.of(h), List.of(o))
                .await().indefinitely();
        assertEquals(1, res.size());

        var rows = client.query("SELECT count(*) FROM outbox").execute().await().indefinitely();
        assertEquals(1L, rows.iterator().next().getLong(0));
    }

    @Test
    public void testFetchBatchForProcessingLeasesAndReturnsRows() {
        String conversationId = "conv-2";
        var s = "sender2";
        var r = "recipient2";

        repo.insertOutboxBatch(List.of(
                sample(s, r, conversationId, 1),
                sample(s, r, conversationId, 2),
                sample(s, r, conversationId, 3)
        )).await().indefinitely();
        var batch = repo.fetchBatchForProcessing(2, Duration.ofSeconds(5)).await().indefinitely();
        assertEquals(2, batch.size());
        assertTrue(batch.get(0).leaseUntil().isAfter(Instant.now()));

        var batch2 = repo.fetchBatchForProcessing(10, Duration.ofSeconds(5)).await().indefinitely();
        assertEquals(1, batch2.size());

        List<String> idsToDelete = List.of(batch.get(0).messageId(), batch.get(1).messageId());
        int deleted = repo.deleteProcessedByIds(idsToDelete).await().indefinitely();
        assertEquals(2, deleted);

        var batch3 = repo.fetchBatchForProcessing(5, Duration.ofSeconds(1)).await().indefinitely();
        assertEquals(0, batch3.size());
    }

    @Test
    public void testInsertOutboxBatchReturnIds() {
        String conversationId = "conv-3";
        String s = "sender3";
        String r = "recipient3";

        var res = repo.insertOutboxBatch(List.of(
                sample(s, r, conversationId, 10),
                sample(s, r, conversationId, 11)
        )).await().indefinitely();

        assertEquals(2, res.size());
        assertTrue(res.get(0).id() > 0);
    }

    @Test
    public void testLeasePreventsReprocessing() {
        String conversationId = "conv-4";
        String s = "sender4";
        String r = "recipient4";

        repo.insertOutboxBatch(List.of(sample(s, r, conversationId, 1))).await().indefinitely();
        var batch1 = repo.fetchBatchForProcessing(10, Duration.ofSeconds(5)).await().indefinitely();
        assertEquals(1, batch1.size());

        var batch2 = repo.fetchBatchForProcessing(10, Duration.ofSeconds(5)).await().indefinitely();
        assertEquals(0, batch2.size());
    }

    @Test
    public void testProcessingAttemptsIncrement() {
        String conversationId = "conv-5";
        String s = "sender5";
        String r = "recipient5";

        repo.insertOutboxBatch(List.of(sample(s, r, conversationId, 1))).await().indefinitely();
        repo.fetchBatchForProcessing(10, Duration.ofSeconds(5)).await().indefinitely();

        var row = client.query("SELECT processing_attempts FROM outbox LIMIT 1")
                .execute().await().indefinitely()
                .iterator().next();
        assertEquals(1, row.getInteger("processing_attempts"));
    }

    @Test
    public void testTransactionalRollback() {
        String conversationId = "conv-7";
        String s = "sender7";
        String r = "recipient7";

        var h = sampleHistory(conversationId, 1);
        var o = sample(s, null, conversationId, 1); // violates not-null constraint

        try {
            repo.insertHistoryAndOutboxTransactional(List.of(h), List.of(o))
                    .await().indefinitely();
            fail("Should fail");
        } catch (Exception ignored) {
        }

        long countHistory = client.query("SELECT count(*) FROM message_history")
                .execute().await().indefinitely()
                .iterator().next().getLong(0);
        long countOutbox = client.query("SELECT count(*) FROM outbox")
                .execute().await().indefinitely()
                .iterator().next().getLong(0);

        assertEquals(0L, countHistory);
        assertEquals(0L, countOutbox);
    }

    @Test
    public void testDeleteOlderThan() {
        String conversationId = "conv-8";
        String s = "sender8";
        String r = "recipient8";

        List<OutboxMessage> batch = List.of(
                sample(s, r, conversationId, 1),
                sample(s, r, conversationId, 2),
                sample(s, r, conversationId, 3)
        );
        repo.insertOutboxBatch(batch).await().indefinitely();

        long countBefore = client.query("SELECT count(*) FROM outbox")
                .execute().await().indefinitely()
                .iterator().next().getLong(0);
        assertEquals(3L, countBefore);

        int deleted = repo.deleteOlderThan(3L).await().indefinitely();
        assertEquals(2, deleted);

        long countAfter = client.query("SELECT count(*) FROM outbox")
                .execute().await().indefinitely()
                .iterator().next().getLong(0);
        assertEquals(1L, countAfter);

        long remainingId = client.query("SELECT id FROM outbox")
                .execute().await().indefinitely()
                .iterator().next().getLong("id");
        assertEquals(3L, remainingId);
    }

    @Test
    public void testLeaseUntilIsExtendedCorrectly() {
        String conversationId = "conv-lease-1";
        var s = "senderL1";
        var r = "recipientL1";

        List<Inserted> inserted = repo.insertOutboxBatch(List.of(sample(s, r, conversationId, 1))).await().indefinitely();
        assertEquals(1, inserted.size());

        // Capture start time
        Instant before = Instant.now();
        var batch = repo.fetchBatchForProcessing(1, Duration.ofSeconds(1)).await().indefinitely();
        assertEquals(1, batch.size());
        delay(50);
        batch = repo.fetchBatchForProcessing(1, Duration.ofSeconds(1)).await().indefinitely();
        assertEquals(0, batch.size());
        delay(1000);
        batch = repo.fetchBatchForProcessing(1, Duration.ofSeconds(1)).await().indefinitely();

        assertEquals(1, batch.size());
        Instant lease = batch.get(0).leaseUntil();

        Instant after = Instant.now();

        // lease_until must be in [before + 5, after + 5]
        assertTrue(
                !lease.isBefore(before.plusSeconds(1)) &&
                        !lease.isAfter(after.plusSeconds(1)),
                "lease_until should be = now() + 1 seconds"
        );
    }



    private static void delay(int x) {
        try {
            Thread.sleep(x);
        }
        catch (InterruptedException e) {
            //
        }
    }
}
