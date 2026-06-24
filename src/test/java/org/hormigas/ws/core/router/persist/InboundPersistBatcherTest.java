package org.hormigas.ws.core.router.persist;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.Uni;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.outbox.OutboxManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("InboundPersistBatcher — group-commits enqueued messages and resolves each one")
@SuppressWarnings("unchecked")
class InboundPersistBatcherTest {

    OutboxManager<Message> outbox;
    InboundPersistBatcher batcher;

    @BeforeEach
    void setup() {
        outbox = mock(OutboxManager.class);

        MessengerConfig config = mock(MessengerConfig.class);
        MessengerConfig.Inbound inbound = mock(MessengerConfig.Inbound.class);
        MessengerConfig.Inbound.PersistBatch pb = mock(MessengerConfig.Inbound.PersistBatch.class);
        when(config.inbound()).thenReturn(inbound);
        when(inbound.persistBatch()).thenReturn(pb);
        when(pb.maxSize()).thenReturn(64);
        when(pb.lingerMs()).thenReturn(5);
        when(pb.maxConcurrentBatches()).thenReturn(4);

        batcher = new InboundPersistBatcher();
        batcher.outbox = outbox;
        batcher.config = config;
        batcher.meterRegistry = new SimpleMeterRegistry();
        batcher.init();
    }

    private Message msg(String id) {
        Message m = mock(Message.class);
        when(m.getMessageId()).thenReturn(id);
        return m;
    }

    @Test
    @DisplayName("enqueue resolves with the message's own StageResult once its batch commits")
    void enqueueResolvesWithBatchResult() {
        Message m = msg("m1");
        when(outbox.saveBatch(any()))
                .thenReturn(Uni.createFrom().item(Map.of("m1", StageResult.updated(m))));

        StageResult<Message> result = batcher.enqueue(m).await().atMost(Duration.ofSeconds(2));

        assertTrue(result.isUpdated());
        verify(outbox).saveBatch(any());
    }

    @Test
    @DisplayName("a batch failure for a message falls back to an individual save (poison-row isolation)")
    void batchFailureFallsBackToIndividualSave() {
        Message m = msg("m1");
        when(outbox.saveBatch(any()))
                .thenReturn(Uni.createFrom().item(Map.of("m1", StageResult.failed())));
        when(outbox.save(m)).thenReturn(Uni.createFrom().item(StageResult.passed()));

        StageResult<Message> result = batcher.enqueue(m).await().atMost(Duration.ofSeconds(2));

        assertTrue(result.isSuccess(), "fallback save() result should be used");
        verify(outbox).save(m);
    }

    @Test
    @DisplayName("an unexpected flush failure completes the message as FAILED (never hangs)")
    void unexpectedFlushFailureCompletesFailed() {
        Message m = msg("m1");
        when(outbox.saveBatch(any()))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("boom")));

        StageResult<Message> result = batcher.enqueue(m).await().atMost(Duration.ofSeconds(2));

        assertTrue(result.isFailed());
    }
}
