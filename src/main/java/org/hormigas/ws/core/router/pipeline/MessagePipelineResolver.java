package org.hormigas.ws.core.router.pipeline;

import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.router.PipelineResolver;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;

import java.util.EnumMap;
import java.util.Map;

import static org.hormigas.ws.core.router.PipelineResolver.PipelineType.*;
import static org.hormigas.ws.domain.message.MessageType.*;


@Slf4j
@ApplicationScoped
public class MessagePipelineResolver implements PipelineResolver<Message, MessageType> {

    private final Map<MessageType, PipelineType> routingMatrix = new EnumMap<>(MessageType.class);

    public MessagePipelineResolver() {

        // OUT - MESSAGES GENERATED (RECEIVED) FROM SERVICE/OUTBOX
        // CHAT
        routingMatrix.put(CHAT_OUT, OUTBOUND_CACHED);
        // SIGNAL
        routingMatrix.put(SIGNAL_OUT, OUTBOUND_CACHED);
        // SERVICE
        routingMatrix.put(SERVICE_OUT, OUTBOUND_DIRECT);

        // IN - MESSAGES GENERATED (RECEIVED) FROM CHANEL
        // CHAT
        routingMatrix.put(CHAT_IN, INBOUND_PERSISTENT);
        // SIGNAL
        routingMatrix.put(SIGNAL_IN, INBOUND_CACHED);
        // ACK
        routingMatrix.put(SIGNAL_ACK, ACK_CACHED);
        routingMatrix.put(CHAT_ACK, ACK_PERSISTENT);
        // PRESENCE
        routingMatrix.put(PRESENT_INIT, INBOUND_DIRECT);
        routingMatrix.put(PRESENT_JOIN, INBOUND_DIRECT);
        routingMatrix.put(PRESENT_LEAVE, INBOUND_DIRECT);
    }

    @Override
    public PipelineType resolvePipeline(Message message) {
        if (message == null || message.getType() == null) {
            log.error("Skipping invalid message: {}", message);
            return SKIP;
        }

        PipelineType policy = routingMatrix.get(message.getType());
        if (policy == null) {
            log.error("No routing policy defined for message type={}", message.getType());
            return SKIP;
        }

        log.debug("Resolved policy {} for message type={}", policy, message.getType());
        return policy;
    }
}
