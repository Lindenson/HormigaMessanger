package org.hormigas.ws.infrastructure.persistance.postgres.adapters;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.HistoryRow;
import org.hormigas.ws.infrastructure.persistance.postgres.history.HistoryPostgresRepository;
import org.hormigas.ws.infrastructure.persistance.postgres.mappers.MessageMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class HistoryPostgresAdapterTest {

    @InjectMocks
    HistoryPostgresAdapter adapter;

    @Mock
    HistoryPostgresRepository repository;

    @Mock
    MessageMapper mapper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    // ------------------- getByRecipientId -------------------
    @Test
    void getByRecipientId_nullClientId_returnsEmptyList() {
        List<Message> result = adapter.getByRecipientId(null).await().indefinitely();
        assertTrue(result.isEmpty());
    }

    @Test
    void getByRecipientId_validClient_returnsMappedMessages() {
        HistoryRow row1 = new HistoryRow("m1", "c1", "s1", "r1", "{}", Instant.now());
        HistoryRow row2 = new HistoryRow("m2", "c2", "s2", "r2", "{}", Instant.now());
        Message msg1 = Message.builder().messageId("m1").payload(new Message.Payload()).build();
        Message msg2 = Message.builder().messageId("m2").payload(new Message.Payload()).build();

        when(repository.findAllByRecipientId("r1")).thenReturn(Uni.createFrom().item(List.of(row1, row2)));
        when(mapper.fromHistoryRow(row1)).thenReturn(msg1);
        when(mapper.fromHistoryRow(row2)).thenReturn(msg2);

        List<Message> result = adapter.getByRecipientId("r1").await().indefinitely();

        assertEquals(2, result.size());
        assertEquals("m1", result.get(0).getMessageId());
        assertEquals("m2", result.get(1).getMessageId());
    }

    @Test
    void getByRecipientId_mapperReturnsNull_filtersOut() {
        HistoryRow row1 = new HistoryRow("m1", "c1", "s1", "r1", "{}", Instant.now());
        when(repository.findAllByRecipientId("r1")).thenReturn(Uni.createFrom().item(List.of(row1)));
        when(mapper.fromHistoryRow(row1)).thenReturn(null);

        List<Message> result = adapter.getByRecipientId("r1").await().indefinitely();
        assertTrue(result.isEmpty());
    }

    @Test
    void getByRecipientId_repositoryFailure_returnsEmptyList() {
        when(repository.findAllByRecipientId("r1")).thenReturn(Uni.createFrom().failure(new RuntimeException("db fail")));

        List<Message> result = adapter.getByRecipientId("r1").await().indefinitely();
        assertTrue(result.isEmpty());
    }

    // ------------------- getBySenderId -------------------
    @Test
    void getBySenderId_nullClientId_returnsEmptyList() {
        List<Message> result = adapter.getBySenderId(null).await().indefinitely();
        assertTrue(result.isEmpty());
    }

    @Test
    void getBySenderId_successful() {
        HistoryRow row = new HistoryRow("m1", "c1", "s1", "r1", "{}", Instant.now());
        Message msg = Message.builder().messageId("m1").payload(new Message.Payload()).build();
        when(repository.findAllBySenderId("s1")).thenReturn(Uni.createFrom().item(List.of(row)));
        when(mapper.fromHistoryRow(row)).thenReturn(msg);

        List<Message> result = adapter.getBySenderId("s1").await().indefinitely();
        assertEquals(1, result.size());
        assertEquals("m1", result.get(0).getMessageId());
    }

    @Test
    void getBySenderId_mapperReturnsNull_filtersOut() {
        HistoryRow row = new HistoryRow("m1", "c1", "s1", "r1", "{}", Instant.now());
        when(repository.findAllBySenderId("s1")).thenReturn(Uni.createFrom().item(List.of(row)));
        when(mapper.fromHistoryRow(row)).thenReturn(null);

        List<Message> result = adapter.getBySenderId("s1").await().indefinitely();
        assertTrue(result.isEmpty());
    }

    @Test
    void getBySenderId_repositoryFailure_returnsEmptyList() {
        when(repository.findAllBySenderId("s1")).thenReturn(Uni.createFrom().failure(new RuntimeException("db fail")));

        List<Message> result = adapter.getBySenderId("s1").await().indefinitely();
        assertTrue(result.isEmpty());
    }

    // ------------------- getAllBySenderId -------------------
    @Test
    void getAllBySenderId_nullClientId_returnsEmptyList() {
        List<Message> result = adapter.getAllBySenderId(null).await().indefinitely();
        assertTrue(result.isEmpty());
    }

    @Test
    void getAllBySenderId_successful() {
        HistoryRow row = new HistoryRow("m1", "c1", "s1", "r1", "{}", Instant.now());
        Message msg = Message.builder().messageId("m1").payload(new Message.Payload()).build();
        when(repository.findAllByParticipantId("p1")).thenReturn(Uni.createFrom().item(List.of(row)));
        when(mapper.fromHistoryRow(row)).thenReturn(msg);

        List<Message> result = adapter.getAllBySenderId("p1").await().indefinitely();
        assertEquals(1, result.size());
        assertEquals("m1", result.get(0).getMessageId());
    }

    @Test
    void getAllBySenderId_mapperReturnsNull_filtersOut() {
        HistoryRow row = new HistoryRow("m1", "c1", "s1", "r1", "{}", Instant.now());
        when(repository.findAllByParticipantId("p1")).thenReturn(Uni.createFrom().item(List.of(row)));
        when(mapper.fromHistoryRow(row)).thenReturn(null);

        List<Message> result = adapter.getAllBySenderId("p1").await().indefinitely();
        assertTrue(result.isEmpty());
    }

    @Test
    void getAllBySenderId_repositoryFailure_returnsEmptyList() {
        when(repository.findAllByParticipantId("p1")).thenReturn(Uni.createFrom().failure(new RuntimeException("db fail")));

        List<Message> result = adapter.getAllBySenderId("p1").await().indefinitely();
        assertTrue(result.isEmpty());
    }

    // ------------------- addBySenderId -------------------
    @Test
    void addBySenderId_nullClientOrMessage_skipsInsert() {
        adapter.addBySenderId(null, null);
        adapter.addBySenderId("cid", null);
        adapter.addBySenderId(null, Message.builder().messageId("m1").payload(new Message.Payload()).build());

        verifyNoInteractions(repository);
        verifyNoInteractions(mapper);
    }

    @Test
    void addBySenderId_invalidHistoryRow_skipsInsert() {
        Message msg = Message.builder().messageId("m1").payload(new Message.Payload()).build();
        HistoryRow row = new HistoryRow(null, "c1", "s1", "r1", "{}", Instant.now());
        when(mapper.toHistoryRow(msg)).thenReturn(row);

        adapter.addBySenderId("cid", msg);

        verify(repository, never()).insertHistoryBatch(any());
    }

    @Test
    void addBySenderId_validMessage_callsRepository() {
        Message msg = Message.builder().messageId("m1").payload(new Message.Payload()).build();
        HistoryRow row = new HistoryRow("m1", "c1", "s1", "r1", "{}", Instant.now());
        when(mapper.toHistoryRow(msg)).thenReturn(row);
        when(repository.insertHistoryBatch(List.of(row))).thenReturn(Uni.createFrom().item(List.of()));

        adapter.addBySenderId("cid", msg);

        verify(repository).insertHistoryBatch(List.of(row));
    }

    @Test
    void addBySenderId_repositoryFailure_loggedButNotThrown() {
        Message msg = Message.builder().messageId("m1").payload(new Message.Payload()).build();
        HistoryRow row = new HistoryRow("m1", "c1", "s1", "r1", "{}", Instant.now());
        when(mapper.toHistoryRow(msg)).thenReturn(row);
        when(repository.insertHistoryBatch(List.of(row))).thenReturn(Uni.createFrom().failure(new RuntimeException("fail")));

        adapter.addBySenderId("cid", msg);

        verify(repository).insertHistoryBatch(List.of(row));
    }
}
