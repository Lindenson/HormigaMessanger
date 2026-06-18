package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.domain.stage.StageResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("AckStage — emits a server ACK to the sender once the message is persisted")
@SuppressWarnings("unchecked")
class AckStageTest {

    private final DeliveryStage deliveryStage = mock(DeliveryStage.class);
    private final IdGenerator idGenerator = mock(IdGenerator.class);
    private final AckStage stage = new AckStage(deliveryStage, idGenerator);

    private RouterContext<Message> ctx(StageResult<Message> persisted) {
        RouterContext<Message> c = RouterContext.<Message>builder()
                .payload(Message.builder().type(MessageType.CHAT_OUT)
                        .senderId("master-1").correlationId("client-msg").messageId("server-1").build())
                .pipelineType(PipelineType.INBOUND_PERSISTENT).build();
        c.setPersisted(persisted);
        return c;
    }

    @Test
    @DisplayName("does nothing when the message was not persisted")
    void noAckWhenNotPersisted() {
        RouterContext<Message> out = stage.apply(ctx(StageResult.unknown())).await().indefinitely();
        verify(deliveryStage, never()).apply(any());
        assertTrue(out.getAcknowledged().isUnknown());
    }

    @Test
    @DisplayName("on persist, builds a CHAT_ACK addressed back to the sender and delivers it")
    void buildsAndDeliversAck() {
        when(idGenerator.generateId()).thenReturn("ack-id");
        RouterContext<Message> ackDelivered = RouterContext.<Message>builder()
                .payload(Message.builder().type(MessageType.CHAT_ACK).build())
                .pipelineType(PipelineType.INBOUND_PERSISTENT).build();
        ackDelivered.setAcknowledged(StageResult.passed());
        when(deliveryStage.apply(any())).thenReturn(Uni.createFrom().item(ackDelivered));

        RouterContext<Message> out = stage.apply(ctx(StageResult.passed())).await().indefinitely();

        ArgumentCaptor<RouterContext<Message>> captor = ArgumentCaptor.forClass(RouterContext.class);
        verify(deliveryStage).apply(captor.capture());
        Message ack = captor.getValue().getPayload();
        assertEquals(MessageType.CHAT_ACK, ack.getType());
        assertEquals("master-1", ack.getRecipientId());     // ACK goes back to the original sender
        assertEquals("client-msg", ack.getCorrelationId()); // correlated to the sender's message id
        assertTrue(out.getAcknowledged().isSuccess());
    }
}
