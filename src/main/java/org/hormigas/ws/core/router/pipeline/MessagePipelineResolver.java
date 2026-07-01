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
        // TYPING (transient, like signaling)
        routingMatrix.put(TYPING_OUT, OUTBOUND_CACHED);
        // SERVICE
        routingMatrix.put(SERVICE_OUT, OUTBOUND_DIRECT);
        // SYSTEM (Strategy C) — deliver + cache (dedup), NO Tetris mark (stays out of the A watermark)
        routingMatrix.put(SYSTEM_OUT, OUTBOUND_TRANSIENT);

        // IN - MESSAGES GENERATED (RECEIVED) FROM CHANEL
        // CHAT
        routingMatrix.put(CHAT_IN, INBOUND_PERSISTENT);
        // SIGNAL
        routingMatrix.put(SIGNAL_IN, INBOUND_CACHED);
        // TYPING (transient, Strategy S) — send-guard + live delivery to the peer, never persisted
        routingMatrix.put(TYPING_IN, INBOUND_CACHED);
        // ACK
        routingMatrix.put(SIGNAL_ACK, ACK_CACHED);
        routingMatrix.put(CHAT_ACK, ACK_PERSISTENT);
        // READ receipt (UC-U14) — mark read + push READ_OUT to the peer
        routingMatrix.put(READ_IN, READ);
        // System-notice delivery confirmation (Strategy C, ADR-014) — ownership-checked retract
        routingMatrix.put(SYSTEM_ACK, ACK_SYSTEM);
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
