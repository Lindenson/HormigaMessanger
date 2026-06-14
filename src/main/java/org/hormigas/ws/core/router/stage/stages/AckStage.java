package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.domain.message.Message;

import static org.hormigas.ws.domain.message.MessageType.CHAT_ACK;

@Slf4j
@ApplicationScoped
@RequiredArgsConstructor
public class AckStage implements PipelineStage<RouterContext<Message>> {

    private final DeliveryStage deliveryStage;
    private final IdGenerator idGenerator;

    @Override
    public Uni<RouterContext<Message>> apply(RouterContext<Message> ctx) {
        if (ctx.getPersisted().isSuccess()) {
            Message ackMessage = createAck(ctx.getPayload());
            return deliveryStage.apply(ctx.withPayload(ackMessage))
                    .onItem().invoke(ackResult -> ctx.setAcknowledged(ackResult.getAcknowledged()))
                    .replaceWith(ctx);
        }
        return Uni.createFrom().item(ctx);
    }

    private Message createAck(Message original) {
        return Message.builder()
                .messageId(idGenerator.generateId())
                .correlationId(original.getCorrelationId())
                .type(CHAT_ACK)
                .senderId("server")
                .recipientId(original.getSenderId())
                .serverTimestamp(System.currentTimeMillis())
                .build();
    }
}