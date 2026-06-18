package org.hormigas.ws.scheduler;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.infrastructure.persistance.postgres.HistoryRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@DisplayName("HistoryRetentionScheduler — purge sweeps both retention classes")
class HistoryRetentionSchedulerTest {

    private final HistoryRepository history = mock(HistoryRepository.class);

    private HistoryRetentionScheduler scheduler() {
        HistoryRetentionScheduler s = new HistoryRetentionScheduler();
        s.history = history;
        return s;
    }

    @Test
    @DisplayName("purge runs the normal then the frozen sweep and sums the rows removed")
    void purgeSumsBothSweeps() {
        Instant normalCutoff = Instant.ofEpochMilli(1_000);
        Instant frozenCutoff = Instant.ofEpochMilli(2_000);
        when(history.deleteOlderThan(eq(normalCutoff))).thenReturn(Uni.createFrom().item(3));
        when(history.deleteFrozenOlderThan(eq(frozenCutoff))).thenReturn(Uni.createFrom().item(2));

        Integer total = scheduler().purge(normalCutoff, frozenCutoff).await().indefinitely();

        assertEquals(5, total);
        verify(history).deleteOlderThan(normalCutoff);
        verify(history).deleteFrozenOlderThan(frozenCutoff);
    }
}
