package org.hormigas.ws.scheduler;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.deadletter.DeadLetterStore;
import org.hormigas.ws.ports.deadletter.DeliveryConfirmations;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@DisplayName("DeadLetterCleanupScheduler — retracts confirmed drafts, then clears confirmations (ADR-014)")
@SuppressWarnings("unchecked")
class DeadLetterCleanupSchedulerTest {

    private final DeliveryConfirmations confirmations = mock(DeliveryConfirmations.class);
    private final DeadLetterStore<Message> deadLetter = mock(DeadLetterStore.class);

    private DeadLetterCleanupScheduler scheduler() {
        org.hormigas.ws.config.DeadLetterConfig cfg = mock(org.hormigas.ws.config.DeadLetterConfig.class);
        when(cfg.cleanupBatch()).thenReturn(500);
        DeadLetterCleanupScheduler s = new DeadLetterCleanupScheduler();
        s.confirmations = confirmations;
        s.deadLetter = deadLetter;
        s.config = cfg;
        return s;
    }

    @Test
    @DisplayName("confirmed ids → drafts deleted, then those confirmations cleared (delete before clear)")
    void retractsConfirmed() {
        when(confirmations.peek(anyInt())).thenReturn(Uni.createFrom().item(List.of("a", "b")));
        when(deadLetter.deleteDrafts(List.of("a", "b"))).thenReturn(Uni.createFrom().item(2));
        when(confirmations.clear(any())).thenReturn(Uni.createFrom().voidItem());

        int n = scheduler().cleanup().await().indefinitely();

        assertThat(n).isEqualTo(2);
        InOrder order = inOrder(deadLetter, confirmations);
        order.verify(deadLetter).deleteDrafts(List.of("a", "b"));
        order.verify(confirmations).clear(List.of("a", "b"));
    }

    @Test
    @DisplayName("no confirmations → nothing deleted or cleared")
    void noop() {
        when(confirmations.peek(anyInt())).thenReturn(Uni.createFrom().item(List.of()));
        int n = scheduler().cleanup().await().indefinitely();
        assertThat(n).isZero();
        verify(deadLetter, never()).deleteDrafts(any());
        verify(confirmations, never()).clear(any());
    }
}
