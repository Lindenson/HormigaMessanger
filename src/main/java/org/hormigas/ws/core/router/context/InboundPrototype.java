package org.hormigas.ws.core.router.context;

import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.domain.generator.IdGenerator;

import static org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import static org.hormigas.ws.domain.message.MessageType.*;


@ApplicationScoped
@RequiredArgsConstructor
public class InboundPrototype {

    private final IdGenerator idGenerator;

    public RouterContext<Message> createOutboundContext(PipelineType pipeline, Message message) {

        return RouterContext.<Message>builder()
                .pipelineType(pipeline)
                .payload(message.toBuilder()
                        .messageId(idGenerator.generateId())
                        .ackId(message.getAckId())
                        .type(switchMesssageType(message))
                        .correlationId(message.getType() == CHAT_ACK? message.getCorrelationId(): message.getMessageId() )
                        .serverTimestamp(System.currentTimeMillis())
                        .build())
                .build();
    }

    private MessageType switchMesssageType(Message message) {
        return switch (message.getType()) {
            case CHAT_IN -> CHAT_OUT;
            case SIGNAL_IN -> SIGNAL_OUT;
            default -> message.getType();
        };
    }
}
