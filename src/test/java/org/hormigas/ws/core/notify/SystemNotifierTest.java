package org.hormigas.ws.core.notify;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.deadletter.DeadLetterStore;
import org.hormigas.ws.ports.outbox.OutboxManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("SystemNotifier — Strategy C: durable draft FIRST, then transient outbox enqueue (ADR-014)")
@SuppressWarnings("unchecked")
class SystemNotifierTest {

    private final OutboxManager<Message> outbox = mock(OutboxManager.class);
    private final DeadLetterStore<Message> deadLetter = mock(DeadLetterStore.class);
    private final IdGenerator idGenerator = mock(IdGenerator.class);
    private SystemNotifier notifier;

    @BeforeEach
    void setup() {
        notifier = new SystemNotifier();
        notifier.outbox = outbox;
        notifier.deadLetter = deadLetter;
        notifier.idGenerator = idGenerator;
        when(idGenerator.generateId()).thenReturn("sysmsg-1");
        when(deadLetter.recordDraft(any())).thenReturn(Uni.createFrom().voidItem());
        when(outbox.saveTransient(any())).thenReturn(Uni.createFrom().item(StageResult.passed()));
    }

    @Test
    @DisplayName("builds a SYSTEM_OUT from server; records the draft before enqueueing the outbox row")
    void emitsDraftThenOutbox() {
        notifier.notify("client1", "event", "over the limit", Map.of("k", "v"), null).await().indefinitely();

        ArgumentCaptor<Message> cap = ArgumentCaptor.forClass(Message.class);
        verify(deadLetter).recordDraft(cap.capture());
        Message m = cap.getValue();
        assertThat(m.getType()).isEqualTo(MessageType.SYSTEM_OUT);
        assertThat(m.getSenderId()).isEqualTo("server");
        assertThat(m.getRecipientId()).isEqualTo("client1");
        assertThat(m.getMessageId()).isEqualTo("sysmsg-1");
        assertThat(m.getConversationId()).isEqualTo("system:client1");   // synthetic when none given
        assertThat(m.getPayload().getKind()).isEqualTo("event");
        assertThat(m.getPayload().getBody()).isEqualTo("over the limit");

        // draft must be durable BEFORE the transient delivery vehicle is enqueued
        InOrder order = inOrder(deadLetter, outbox);
        order.verify(deadLetter).recordDraft(any());
        order.verify(outbox).saveTransient(any());
    }

    @Test
    @DisplayName("an explicit conversationId is preserved")
    void preservesConversationId() {
        notifier.notify("client1", null, "hi", null, "conv-7").await().indefinitely();
        ArgumentCaptor<Message> cap = ArgumentCaptor.forClass(Message.class);
        verify(outbox).saveTransient(cap.capture());
        assertThat(cap.getValue().getConversationId()).isEqualTo("conv-7");
        assertThat(cap.getValue().getPayload().getKind()).isEqualTo("event"); // default kind
    }
}
