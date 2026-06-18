package org.hormigas.ws.core.poller.outbox;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.backpressure.BackpressurePublisher;
import org.hormigas.ws.core.feedback.Regulator;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.outbox.OutboxManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@DisplayName("OutboxPoller — poll gating and batch publish")
@SuppressWarnings("unchecked")
class OutboxPollerTest {

    private final BackpressurePublisher<Message> publisher = mock(BackpressurePublisher.class);
    private final OutboxManager<Message> outboxManager = mock(OutboxManager.class);
    private final Regulator regulator = mock(Regulator.class);

    private OutboxPoller poller() {
        when(regulator.getCurrentIntervalMs()).thenReturn(Duration.ofMillis(1));
        return OutboxPoller.builder()
                .publisher(publisher)
                .outboxManager(outboxManager)
                .registry(new SimpleMeterRegistry())
                .regulator(regulator)
                .batchSize(10)
                .build();
    }

    @Test
    @DisplayName("skips the fetch when the downstream queue is not empty (backpressure)")
    void skipsFetchWhenQueueBusy() {
        when(publisher.queueIsNotEmpty()).thenReturn(true);

        poller().poll().await().indefinitely();

        verify(outboxManager, never()).fetchBatch(anyInt());
        verify(publisher, never()).publish(any());
    }

    @Test
    @DisplayName("fetches a batch and publishes every message when the queue is drained")
    void fetchesAndPublishesWhenQueueEmpty() {
        Message m1 = Message.builder().messageId("m1").build();
        Message m2 = Message.builder().messageId("m2").build();
        when(publisher.queueIsNotEmpty()).thenReturn(false);
        when(outboxManager.fetchBatch(10)).thenReturn(Uni.createFrom().item(List.of(m1, m2)));

        poller().poll().await().indefinitely();

        verify(outboxManager).fetchBatch(10);
        verify(publisher).publish(m1);
        verify(publisher).publish(m2);
    }
}
