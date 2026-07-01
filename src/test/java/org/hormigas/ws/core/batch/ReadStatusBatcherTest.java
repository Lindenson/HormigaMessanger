package org.hormigas.ws.core.batch;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.Uni;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.ports.message.ReadReceipts;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("ReadStatusBatcher — group-commits READ marks and sheds gracefully under overload")
@SuppressWarnings("unchecked")
class ReadStatusBatcherTest {

    private ReadStatusBatcher newBatcher(ReadReceipts receipts, MeterRegistry reg,
                                         int maxSize, int lingerMs, int concurrency) {
        MessengerConfig config = mock(MessengerConfig.class);
        MessengerConfig.ReadBatch rb = mock(MessengerConfig.ReadBatch.class);
        when(config.readBatch()).thenReturn(rb);
        when(rb.maxSize()).thenReturn(maxSize);
        when(rb.lingerMs()).thenReturn(lingerMs);
        when(rb.maxConcurrentBatches()).thenReturn(concurrency);
        MessengerConfig.StreamRetry sr = mock(MessengerConfig.StreamRetry.class);
        when(sr.minBackoffMs()).thenReturn(200);
        when(sr.maxBackoffMs()).thenReturn(5000);
        when(config.streamRetry()).thenReturn(sr);
        ReadStatusBatcher b = new ReadStatusBatcher();
        b.receipts = receipts;
        b.config = config;
        b.meterRegistry = reg;
        b.init();
        return b;
    }

    @Test
    @DisplayName("enqueue resolves with the count its batch marked READ")
    void enqueueResolvesWithBatchCount() {
        ReadReceipts receipts = mock(ReadReceipts.class);
        when(receipts.markReadBatch(any())).thenReturn(Uni.createFrom().item(List.of(5)));
        ReadStatusBatcher b = newBatcher(receipts, new SimpleMeterRegistry(), 64, 5, 4);

        int marked = b.enqueue("conv-1", "reader-1").await().atMost(Duration.ofSeconds(2));

        assertEquals(5, marked);
        verify(receipts).markReadBatch(any());
    }

    @Test
    @DisplayName("source faster than sink: marks are shed as 0 and the stream survives (no BackPressureFailure)")
    void shedsUnderSaturationAndSurvives() {
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        ReadReceipts receipts = mock(ReadReceipts.class);
        AtomicInteger calls = new AtomicInteger();
        CompletableFuture<List<Integer>> gate = new CompletableFuture<>();
        when(receipts.markReadBatch(any())).thenAnswer(inv -> {
            if (calls.getAndIncrement() == 0) {
                return Uni.createFrom().completionStage(gate); // first batch holds the only sink slot
            }
            List<ReadReceipts.MarkRead> ops = inv.getArgument(0);
            List<Integer> ones = new ArrayList<>();
            for (int i = 0; i < ops.size(); i++) ones.add(1);
            return Uni.createFrom().item(ones);
        });
        // 1 mark per batch, single in-flight slot → trivially saturable
        ReadStatusBatcher b = newBatcher(receipts, reg, 1, 5, 1);

        int n = 30;
        List<CompletableFuture<Integer>> fs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            fs.add(b.enqueue("conv-" + i, "reader").subscribeAsCompletionStage().toCompletableFuture());
        }

        int shedZeros = 0;
        for (CompletableFuture<Integer> f : fs) {
            try {
                if (f.get(2, TimeUnit.SECONDS) == 0) shedZeros++;
            } catch (Exception pending) {
                // the slot-holder stays pending until released — expected for ~1
            }
        }
        assertTrue(shedZeros >= 1, "saturated marks must be shed as 0 — not hang or kill the stream");
        assertTrue(reg.get("messenger.read.batch.shed").counter().count() >= 1, "shed metric increments");

        // release the slot → the SAME stream keeps working
        gate.complete(List.of(1));
        int after = b.enqueue("conv-after", "reader").await().atMost(Duration.ofSeconds(3));
        assertEquals(1, after, "stream survived saturation — new marks commit once the sink frees");
    }
}
