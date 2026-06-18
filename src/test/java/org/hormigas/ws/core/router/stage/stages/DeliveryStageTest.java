package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.channel.DeliveryChannel;
import org.hormigas.ws.ports.idempotency.IdempotencyManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("DeliveryStage — persisted-gating, idempotency, ACK bypass")
@SuppressWarnings("unchecked")
class DeliveryStageTest {

    private final MessengerConfig config = mock(MessengerConfig.class);
    private final DeliveryChannel<Message> channel = mock(DeliveryChannel.class);
    private final IdempotencyManager<Message> idempotency = mock(IdempotencyManager.class);
    private DeliveryStage stage;

    @BeforeEach
    void setUp() {
        MessengerConfig.Channel ch = mock(MessengerConfig.Channel.class);
        when(config.channel()).thenReturn(ch);
        when(ch.retry()).thenReturn(false);
        when(ch.minBackoffMs()).thenReturn(1);
        when(ch.maxBackoffMs()).thenReturn(2);
        when(ch.maxRetries()).thenReturn(1);
        stage = new DeliveryStage(config, channel, idempotency);
        stage.init();
    }

    private RouterContext<Message> ctx(MessageType type, StageResult<Message> persisted) {
        RouterContext<Message> c = RouterContext.<Message>builder()
                .payload(Message.builder().type(type).recipientId("r").messageId("m1").build())
                .pipelineType(PipelineType.INBOUND_PERSISTENT).build();
        c.setPersisted(persisted);
        return c;
    }

    @Test
    @DisplayName("when persistence did not succeed, delivery is skipped and the channel is untouched")
    void skipsWhenNotPersisted() {
        RouterContext<Message> out = stage.apply(ctx(MessageType.CHAT_OUT, StageResult.unknown())).await().indefinitely();
        assertTrue(out.getDelivered().isSkipped());
        verify(channel, never()).deliver(any());
    }

    @Test
    @DisplayName("a persisted chat message that is not in-progress is delivered")
    void deliversWhenAllowed() {
        when(idempotency.isInProgress(any())).thenReturn(Uni.createFrom().item(false));
        when(channel.deliver(any())).thenReturn(Uni.createFrom().item(StageResult.passed()));
        RouterContext<Message> out = stage.apply(ctx(MessageType.CHAT_OUT, StageResult.passed())).await().indefinitely();
        assertTrue(out.getDelivered().isSuccess());
        verify(channel).deliver(any());
    }

    @Test
    @DisplayName("a message already in-progress is skipped (idempotency)")
    void skipsWhenInProgress() {
        when(idempotency.isInProgress(any())).thenReturn(Uni.createFrom().item(true));
        RouterContext<Message> out = stage.apply(ctx(MessageType.CHAT_OUT, StageResult.passed())).await().indefinitely();
        assertTrue(out.getDelivered().isSkipped());
        verify(channel, never()).deliver(any());
    }

    @Test
    @DisplayName("an ACK is delivered without consulting the idempotency manager")
    void ackBypassesIdempotency() {
        when(channel.deliver(any())).thenReturn(Uni.createFrom().item(StageResult.passed()));
        RouterContext<Message> out = stage.apply(ctx(MessageType.CHAT_ACK, StageResult.passed())).await().indefinitely();
        assertTrue(out.getDelivered().isSuccess());
        verify(idempotency, never()).isInProgress(any());
    }
}
