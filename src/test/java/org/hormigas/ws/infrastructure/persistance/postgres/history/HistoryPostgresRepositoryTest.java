package org.hormigas.ws.infrastructure.persistance.postgres.history;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.pgclient.PgPool;
import jakarta.inject.Inject;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.HistoryRow;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.Inserted;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HistoryPostgresRepositoryTest {

    @Inject
    PgPool client;

    @Inject
    HistoryPostgresRepository repo;

    @Inject
    IdGenerator idGenerator;

    @BeforeAll
    public void setupSchema() {
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
                CREATE INDEX IF NOT EXISTS idx_message_history_message_id ON message_history(message_id);
                CREATE INDEX IF NOT EXISTS idx_message_history_conversation_id ON message_history(conversation_id);
                CREATE INDEX IF NOT EXISTS idx_message_history_sender_id ON message_history(sender_id);
                CREATE INDEX IF NOT EXISTS idx_message_history_recipient_id ON message_history(recipient_id);
                """).execute().await().indefinitely();
    }

    @BeforeEach
    public void cleanup() {
        client.query("TRUNCATE message_history RESTART IDENTITY").execute().await().indefinitely();
    }

    private HistoryRow sample(String messageId, String conversationId, String senderId, String recipientId, int idx) {
        String payload = "{\"kind\":\"text\",\"body\":\"hello " + idx + "\"}";
        return new HistoryRow(messageId, conversationId, senderId, recipientId, payload, Instant.now());
    }

    @Test
    public void testInsertHistoryBatchAndReturnIds() {
        HistoryRow h1 = sample("msg-1", "conv-1", "s1", "r1", 1);
        HistoryRow h2 = sample("msg-2", "conv-1", "s1", "r2", 2);

        List<Inserted> inserted = repo.insertHistoryBatch(List.of(h1, h2)).await().indefinitely();
        assertEquals(2, inserted.size());
        assertTrue(inserted.get(0).id() > 0);
        assertTrue(inserted.get(1).id() > 0);
    }

    @Test
    public void testInsertEmptyBatchReturnsEmptyList() {
        List<Inserted> inserted = repo.insertHistoryBatch(List.of()).await().indefinitely();
        assertTrue(inserted.isEmpty());
    }

    @Test
    public void testDeleteByIdsAndMessageIds() {
        HistoryRow h1 = sample("msg-1", "conv-1", "s1", "r1", 1);
        HistoryRow h2 = sample("msg-2", "conv-1", "s1", "r2", 2);

        List<Inserted> inserted = repo.insertHistoryBatch(List.of(h1, h2)).await().indefinitely();
        assertEquals(2, inserted.size());

        int deletedById = repo.deleteByIds(List.of(inserted.get(0).id())).await().indefinitely();
        assertEquals(1, deletedById);

        int deletedByMsgId = repo.deleteByMessageIds(List.of("msg-2")).await().indefinitely();
        assertEquals(1, deletedByMsgId);

        long count = client.query("SELECT count(*) FROM message_history").execute().await().indefinitely()
                .iterator().next().getLong(0);
        assertEquals(0L, count);
    }

    @Test
    public void testDeleteOlderThan() throws InterruptedException {
        HistoryRow h1 = sample("msg-1", "conv-1", "s1", "r1", 1);
        HistoryRow h2 = sample("msg-2", "conv-1", "s1", "r2", 2);

        repo.insertHistoryBatch(List.of(h1, h2)).await().indefinitely();

        Instant threshold = Instant.now().plusMillis(10);
        Thread.sleep(20); // Ensure created_at is before threshold

        int deleted = repo.deleteOlderThan(threshold).await().indefinitely();
        assertEquals(2, deleted);

        long count = client.query("SELECT count(*) FROM message_history").execute().await().indefinitely()
                .iterator().next().getLong(0);
        assertEquals(0L, count);
    }

    @Test
    public void testFindAllByConversationId() {
        HistoryRow h1 = sample("msg-1", "conv-1", "s1", "r1", 1);
        HistoryRow h2 = sample("msg-2", "conv-2", "s2", "r2", 2);
        repo.insertHistoryBatch(List.of(h1, h2)).await().indefinitely();

        List<HistoryRow> conv1 = repo.findAllByConversationId("conv-1").await().indefinitely();
        assertEquals(1, conv1.size());
        assertEquals("msg-1", conv1.get(0).messageId());
    }

    @Test
    public void testFindAllBySenderId() {
        HistoryRow h1 = sample("msg-1", "conv-1", "s1", "r1", 1);
        HistoryRow h2 = sample("msg-2", "conv-2", "s2", "r2", 2);
        repo.insertHistoryBatch(List.of(h1, h2)).await().indefinitely();

        List<HistoryRow> s1 = repo.findAllBySenderId("s1").await().indefinitely();
        assertEquals(1, s1.size());
        assertEquals("msg-1", s1.get(0).messageId());
    }

    @Test
    public void testFindAllByRecipientId() {
        HistoryRow h1 = sample("msg-1", "conv-1", "s1", "r1", 1);
        HistoryRow h2 = sample("msg-2", "conv-2", "s2", "r2", 2);
        repo.insertHistoryBatch(List.of(h1, h2)).await().indefinitely();

        List<HistoryRow> r2 = repo.findAllByRecipientId("r2").await().indefinitely();
        assertEquals(1, r2.size());
        assertEquals("msg-2", r2.get(0).messageId());
    }

    @Test
    public void testFindAllByParticipantId() {
        HistoryRow h1 = sample("msg-1", "conv-1", "s1", "r1", 1);
        HistoryRow h2 = sample("msg-2", "conv-2", "s1", "r2", 2);
        HistoryRow h3 = sample("msg-3", "conv-3", "s3", "s1", 3);
        repo.insertHistoryBatch(List.of(h1, h2, h3)).await().indefinitely();

        List<HistoryRow> pS1 = repo.findAllByParticipantId("s1").await().indefinitely();
        assertEquals(3, pS1.size());

        List<HistoryRow> pR2 = repo.findAllByParticipantId("r2").await().indefinitely();
        assertEquals(1, pR2.size());
        assertEquals("msg-2", pR2.get(0).messageId());
    }
}
