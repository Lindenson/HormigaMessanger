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

import io.micrometer.core.instrument.MeterRegistry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    private InboundPersistBatcher newBatcher(OutboxManager<Message> ob, MeterRegistry reg,
                                             int maxSize, int lingerMs, int concurrency) {
        MessengerConfig config = mock(MessengerConfig.class);
        MessengerConfig.Inbound inbound = mock(MessengerConfig.Inbound.class);
        MessengerConfig.Inbound.PersistBatch pb = mock(MessengerConfig.Inbound.PersistBatch.class);
        when(config.inbound()).thenReturn(inbound);
        when(inbound.persistBatch()).thenReturn(pb);
        when(pb.maxSize()).thenReturn(maxSize);
        when(pb.lingerMs()).thenReturn(lingerMs);
        when(pb.maxConcurrentBatches()).thenReturn(concurrency);
        InboundPersistBatcher b = new InboundPersistBatcher();
        b.outbox = ob;
        b.config = config;
        b.meterRegistry = reg;
        b.init();
        return b;
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

    @Test
    @DisplayName("source faster than sink: whole batches are shed as FAILED and the stream survives "
            + "(regression: the timed group flush must not BackPressureFailure-stall)")
    void shedsUnderSaturationAndSurvives() {
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        OutboxManager<Message> ob = mock(OutboxManager.class);
        AtomicInteger calls = new AtomicInteger();
        CompletableFuture<Map<String, StageResult<Message>>> gate = new CompletableFuture<>();
        when(ob.save(any())).thenReturn(Uni.createFrom().item(StageResult.failed()));
        when(ob.saveBatch(any())).thenAnswer(inv -> {
            if (calls.getAndIncrement() == 0) {
                return Uni.createFrom().completionStage(gate); // first batch holds the only sink slot
            }
            List<Message> ms = inv.getArgument(0);
            Map<String, StageResult<Message>> r = new HashMap<>();
            for (Message mm : ms) r.put(mm.getMessageId(), StageResult.updated(mm));
            return Uni.createFrom().item(r);
        });
        // 1 message per batch, a single in-flight slot → trivially saturable
        InboundPersistBatcher b = newBatcher(ob, reg, 1, 5, 1);

        // burst far past the one slot; the slot-holder hangs (gate open), the rest must overflow
        int n = 30;
        List<CompletableFuture<StageResult<Message>>> fs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            fs.add(b.enqueue(msg("m" + i)).subscribeAsCompletionStage().toCompletableFuture());
        }

        int failed = 0;
        for (CompletableFuture<StageResult<Message>> f : fs) {
            try {
                if (f.get(2, TimeUnit.SECONDS).isFailed()) failed++;
            } catch (Exception pendingOrErr) {
                // the slot-holder stays pending until released — expected for ~1 of them
            }
        }
        assertTrue(failed >= 1, "saturated batches must be shed as FAILED — not hang, not kill the stream");
        assertTrue(reg.get("messenger.persist.batch.shed").counter().count() >= 1, "shed metric increments");

        // release the slot → the SAME stream keeps working (no BackPressureFailure terminated it)
        gate.complete(Map.of());
        StageResult<Message> after = b.enqueue(msg("after")).await().atMost(Duration.ofSeconds(3));
        assertTrue(after.isUpdated(), "stream survived saturation — new work persists once the sink frees");
    }
}
