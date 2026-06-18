package org.hormigas.ws.core.garbage.collector;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.outbox.OutboxManager;
import org.hormigas.ws.ports.tetris.TetrisMarker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@DisplayName("OutboxGarbageCollector — primed gate, rehydrate-on-loss, safe-delete")
@SuppressWarnings("unchecked")
class OutboxGarbageCollectorTest {

    private final OutboxManager<Message> outbox = mock(OutboxManager.class);
    private final TetrisMarker<Message> tetris = mock(TetrisMarker.class);
    private final OutboxGarbageCollector gc = new OutboxGarbageCollector(outbox, tetris);

    @Test
    @DisplayName("when primed: computes the safe id and collects, without rehydrating")
    void primedSkipsRehydrate() {
        when(tetris.isPrimed()).thenReturn(Uni.createFrom().item(true));
        when(tetris.computeGlobalSafeDeleteId()).thenReturn(Uni.createFrom().item(100L));
        when(outbox.collectGarbage(100L)).thenReturn(Uni.createFrom().item(5));

        Integer collected = gc.collect().await().indefinitely();

        assertEquals(5, collected);
        verify(tetris, never()).rehydrate(any());
        verify(outbox, never()).pendingByRecipient();
    }

    @Test
    @DisplayName("when not primed: rehydrates pending from the outbox before computing the safe id")
    void unprimedRehydratesFirst() {
        Map<String, List<Long>> pending = Map.of("client-1", List.of(7L, 8L));
        when(tetris.isPrimed()).thenReturn(Uni.createFrom().item(false));
        when(outbox.pendingByRecipient()).thenReturn(Uni.createFrom().item(pending));
        when(tetris.rehydrate(pending)).thenReturn(Uni.createFrom().voidItem());
        when(tetris.computeGlobalSafeDeleteId()).thenReturn(Uni.createFrom().item(7L));
        when(outbox.collectGarbage(7L)).thenReturn(Uni.createFrom().item(0));

        Integer collected = gc.collect().await().indefinitely();

        assertEquals(0, collected);
        verify(tetris).rehydrate(pending);
        verify(outbox).pendingByRecipient();
    }

    @Test
    @DisplayName("a failure anywhere in the chain recovers to 0 (GC never throws)")
    void failureRecoversToZero() {
        when(tetris.isPrimed()).thenReturn(Uni.createFrom().item(true));
        when(tetris.computeGlobalSafeDeleteId())
                .thenReturn(Uni.createFrom().failure(new RuntimeException("redis down")));

        Integer collected = gc.collect().await().indefinitely();

        assertEquals(0, collected);
        verify(outbox, never()).collectGarbage(anyLong());
    }
}
