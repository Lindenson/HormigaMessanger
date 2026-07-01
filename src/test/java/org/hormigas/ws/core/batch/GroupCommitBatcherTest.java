package org.hormigas.ws.core.batch;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("GroupCommitBatcher — generic engine: index-aligned resolve + shed-and-survive under overload")
class GroupCommitBatcherTest {

    private GroupCommitBatcher<String, String> build(
            Function<List<String>, Uni<List<String>>> op, SimpleMeterRegistry reg,
            int maxSize, int lingerMs, int concurrency) {
        return GroupCommitBuilder.<String, String>create()
                .withBatchOp(op)
                .withShedValue("SHED")
                .withMaxSize(maxSize).withLingerMs(lingerMs).withConcurrency(concurrency)
                .withMetrics(reg, "test.batch")
                .build();
    }

    @Test
    @DisplayName("each enqueue resolves with its own result, aligned by position within the batch")
    void resolvesEachItemByPosition() {
        GroupCommitBatcher<String, String> b = build(
                items -> Uni.createFrom().item(items.stream().map(s -> s + "-ok").toList()),
                new SimpleMeterRegistry(), 64, 5, 4);

        assertEquals("a-ok", b.enqueue("a").await().atMost(Duration.ofSeconds(2)));
        assertEquals("b-ok", b.enqueue("b").await().atMost(Duration.ofSeconds(2)));
    }

    @Test
    @DisplayName("source faster than sink: items are shed with the shed value and the stream survives")
    void shedsUnderSaturationAndSurvives() {
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        AtomicInteger calls = new AtomicInteger();
        CompletableFuture<List<String>> gate = new CompletableFuture<>();
        Function<List<String>, Uni<List<String>>> op = items -> {
            if (calls.getAndIncrement() == 0) {
                return Uni.createFrom().completionStage(gate); // first batch holds the only slot
            }
            return Uni.createFrom().item(items.stream().map(s -> s + "-ok").toList());
        };
        GroupCommitBatcher<String, String> b = build(op, reg, 1, 5, 1);

        int n = 30;
        List<CompletableFuture<String>> fs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            fs.add(b.enqueue("m" + i).subscribeAsCompletionStage().toCompletableFuture());
        }

        int shed = 0;
        for (CompletableFuture<String> f : fs) {
            try {
                if ("SHED".equals(f.get(2, TimeUnit.SECONDS))) shed++;
            } catch (Exception pending) {
                // the slot-holder stays pending until released — expected for ~1
            }
        }
        assertTrue(shed >= 1, "saturated items must be shed with the shed value — not hang or kill the stream");
        assertTrue(reg.get("test.batch.shed").counter().count() >= 1, "shed metric increments");

        gate.complete(List.of("held-ok"));
        assertEquals("after-ok", b.enqueue("after").await().atMost(Duration.ofSeconds(3)),
                "stream survived saturation — new items commit once the sink frees");
    }
}
