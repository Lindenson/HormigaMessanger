package org.hormigas.ws.infrastructure.persistance.postgres.adapters;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.HistoryRow;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.Inserted;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxMessage;
import org.hormigas.ws.infrastructure.persistance.postgres.mappers.MessageMapper;
import org.hormigas.ws.infrastructure.persistance.postgres.outbox.OutboxPostgresRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxPostgresAdapterTest {

    OutboxPostgresRepository repo;
    MessageMapper mapper;
    OutboxPostgresAdapter adapter;

    Message validMessage;
    Message invalidMessage;

    @BeforeEach
    void setup() {
        repo = mock(OutboxPostgresRepository.class);
        mapper = mock(MessageMapper.class);
        adapter = new OutboxPostgresAdapter();
        adapter.repo = repo;
        adapter.mapper = mapper;

        validMessage = mock(Message.class);
        when(validMessage.getMessageId()).thenReturn("msg-1");
        when(validMessage.getSenderId()).thenReturn("sender");
        when(validMessage.getRecipientId()).thenReturn("recipient");
        when(validMessage.getPayload()).thenReturn(Message.Payload.builder().kind("text").body("message").build());
        when(validMessage.getCorrelationId()).thenReturn("cor-1");
        when(validMessage.getConversationId()).thenReturn("con-1");

        invalidMessage = mock(Message.class); // all null
    }

    @Test
    void save_successful() {
        when(mapper.toOutboxMessage(validMessage)).thenReturn(mock(OutboxMessage.class));
        when(mapper.toHistoryRow(validMessage)).thenReturn(mock(HistoryRow.class));
        when(repo.insertHistoryAndOutboxTransactional(any(), any()))
                .thenReturn(Uni.createFrom().item(List.of(new Inserted(1, "msg-1"))));

        StageResult<Message> status = adapter.save(validMessage).await().indefinitely();
        assertTrue(status.isSuccess());
    }

    @Test
    void save_mapperReturnsNull() {
        when(mapper.toOutboxMessage(validMessage)).thenReturn(null);
        when(mapper.toHistoryRow(validMessage)).thenReturn(null);

        StageResult<Message>  status = adapter.save(validMessage).await().indefinitely();
        assertTrue(status.isFailed());
    }

    @Test
    void save_invalidMessage() {
        StageResult<Message> status = adapter.save(invalidMessage).await().indefinitely();
        assertTrue(status.isFailed());
    }

    @Test
    void remove_successful() {
        when(repo.deleteProcessedByIds(any())).thenReturn(Uni.createFrom().item(1));

        StageResult<Message> status = adapter.remove(validMessage).await().indefinitely();
        assertTrue(status.isSuccess());
    }

    @Test
    void remove_nullMessage() {
        StageResult<Message> status = adapter.remove(null).await().indefinitely();
        assertTrue(status.isFailed());
    }

    @Test
    void remove_nullCorrelationId() {
        Message msg = mock(Message.class);
        when(msg.getCorrelationId()).thenReturn(null);

        StageResult<Message> status = adapter.remove(msg).await().indefinitely();
        assertTrue(status.isFailed());
    }

    @Test
    void fetch_emptyBatch() {
        when(repo.fetchBatchForProcessing(eq(1), any()))
                .thenReturn(Uni.createFrom().item(List.of()));

        Message msg = adapter.fetch().await().indefinitely();
        assertNull(msg);
    }

    @Test
    void fetch_validMessage() {
        var outboxRow = mock(org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxRow.class);
        when(repo.fetchBatchForProcessing(eq(1), any())).thenReturn(Uni.createFrom().item(List.of(outboxRow)));
        when(mapper.toDomainMessage(outboxRow)).thenReturn(validMessage);

        Message msg = adapter.fetch().await().indefinitely();
        assertEquals(validMessage, msg);
    }

    @Test
    void fetch_invalidMessage() {
        var outboxRow = mock(org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxRow.class);
        when(repo.fetchBatchForProcessing(eq(1), any())).thenReturn(Uni.createFrom().item(List.of(outboxRow)));
        when(mapper.toDomainMessage(outboxRow)).thenReturn(null);

        Message msg = adapter.fetch().await().indefinitely();
        assertNull(msg);
    }

    @Test
    void fetchBatch_filtersNulls() {
        var row1 = mock(org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxRow.class);
        var row2 = mock(org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxRow.class);
        when(repo.fetchBatchForProcessing(eq(2), any()))
                .thenReturn(Uni.createFrom().item(List.of(row1, row2)));
        when(mapper.toDomainMessage(row1)).thenReturn(validMessage);
        when(mapper.toDomainMessage(row2)).thenReturn(null);

        List<Message> messages = adapter.fetchBatch(2).await().indefinitely();
        assertEquals(1, messages.size());
        assertEquals(validMessage, messages.get(0));
    }

    @Test
    void collectGarbage_returnsZero() {
        Integer result = adapter.collectGarbage(1L).await().indefinitely();
        assertEquals(0, result);
    }

    // ------------------------------------------------------------------
    // saveBatch (group-commit / plan B)
    // ------------------------------------------------------------------

    private Message validMessage(String id) {
        Message m = mock(Message.class);
        when(m.getMessageId()).thenReturn(id);
        when(m.getSenderId()).thenReturn("sender");
        when(m.getRecipientId()).thenReturn("recipient");
        when(m.getPayload()).thenReturn(Message.Payload.builder().kind("text").body("b").build());
        when(m.withId(org.mockito.ArgumentMatchers.anyLong())).thenReturn(m);
        return m;
    }

    @Test
    void saveBatch_matchesInsertedByMessageId_notIndex() {
        Message m1 = validMessage("msg-1");
        Message m2 = validMessage("msg-2");
        when(mapper.toOutboxMessage(any())).thenReturn(mock(OutboxMessage.class));
        when(mapper.toHistoryRow(any())).thenReturn(mock(HistoryRow.class));
        // RETURNING order is reversed vs input — matching MUST be by messageId, not position.
        when(repo.insertHistoryAndOutboxTransactional(any(), any()))
                .thenReturn(Uni.createFrom().item(List.of(new Inserted(20, "msg-2"), new Inserted(10, "msg-1"))));

        var results = adapter.saveBatch(List.of(m1, m2)).await().indefinitely();

        assertEquals(2, results.size());
        assertTrue(results.get("msg-1").isUpdated());
        assertTrue(results.get("msg-2").isUpdated());
    }

    @Test
    void saveBatch_invalidMessageFailedUpFront_validStillPersisted() {
        Message valid = validMessage("ok-1");
        Message invalid = mock(Message.class); // all null → fails validation, must NOT reach the DB
        when(invalid.getMessageId()).thenReturn("bad-1");
        when(mapper.toOutboxMessage(valid)).thenReturn(mock(OutboxMessage.class));
        when(mapper.toHistoryRow(valid)).thenReturn(mock(HistoryRow.class));
        when(repo.insertHistoryAndOutboxTransactional(any(), any()))
                .thenReturn(Uni.createFrom().item(List.of(new Inserted(1, "ok-1"))));

        var results = adapter.saveBatch(List.of(valid, invalid)).await().indefinitely();

        assertTrue(results.get("ok-1").isUpdated());
        assertTrue(results.get("bad-1").isFailed());
    }

    @Test
    void saveBatch_transactionFailure_allValidFailed() {
        Message m1 = validMessage("msg-1");
        Message m2 = validMessage("msg-2");
        when(mapper.toOutboxMessage(any())).thenReturn(mock(OutboxMessage.class));
        when(mapper.toHistoryRow(any())).thenReturn(mock(HistoryRow.class));
        when(repo.insertHistoryAndOutboxTransactional(any(), any()))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("tx rolled back")));

        var results = adapter.saveBatch(List.of(m1, m2)).await().indefinitely();

        assertTrue(results.get("msg-1").isFailed());
        assertTrue(results.get("msg-2").isFailed());
    }

    @Test
    void saveBatch_empty_returnsEmptyMap() {
        var results = adapter.saveBatch(List.of()).await().indefinitely();
        assertTrue(results.isEmpty());
    }
}
