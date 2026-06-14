package org.hormigas.ws.infrastructure.persistance.postgres.mappers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.common.constraint.NotNull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.infrastructure.persistance.postgres.OutboxMapper;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.HistoryRow;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxMessage;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxRow;

import java.time.Instant;
import java.util.Map;

@Slf4j
@ApplicationScoped
public class MessageMapper implements OutboxMapper {

    @Inject
    ObjectMapper mapper;

    private static final TypeReference<Map<String, String>> META_TYPE_REF = new TypeReference<>() {};
    private static final String UNKNOWN = "UNKNOWN";


    /**
     * Domain Message -> OutboxMessage
     */
    @Override
    @Nullable
    public OutboxMessage toOutboxMessage(@Nullable Message msg) {
        if (msg == null) return null;

        final String payloadJson = msg.getPayload() == null ? null : serialize(msg.getPayload());
        final String metaJson = msg.getMeta() == null ? null : serialize(msg.getMeta());

        long serverTs = msg.getServerTimestamp();
        if (serverTs == 0) {
            serverTs = Instant.now().toEpochMilli();
        }

        return new OutboxMessage(
                msg.getType() == null ? UNKNOWN : msg.getType().name(),
                msg.getSenderId(),
                msg.getRecipientId(),
                msg.getConversationId(),
                msg.getMessageId(),
                msg.getCorrelationId(),
                msg.getSenderTimestamp(),
                msg.getSenderTimezone(),
                serverTs,
                payloadJson,
                metaJson
        );
    }


    /**
     * Domain Message -> HistoryRow (full JSON)
     */
    @Override
    @Nullable
    public HistoryRow toHistoryRow(@Nullable Message msg) {
        if (msg == null) return null;

        return new HistoryRow(
                msg.getMessageId(),
                msg.getConversationId(),
                msg.getSenderId(),
                msg.getRecipientId(),
                serialize(msg),
                Instant.now()
        );
    }


    /**
     * OutboxRow -> domain Message
     */
    @Override
    @Nullable
    public Message toDomainMessage(@Nullable OutboxRow row) {
        if (row == null) return null;

        try {
            Message.Payload payload = deserializePayload(row.payloadJson());
            Map<String, String> meta = deserializeMeta(row.metaJson());

            return Message.builder()
                    .id(row.id())
                    .type(typeOrNull(row.type()))
                    .senderId(row.senderId())
                    .recipientId(row.recipientId())
                    .conversationId(row.conversationId())
                    .messageId(row.messageId())
                    .correlationId(row.correlationId())
                    .senderTimestamp(row.senderTs())
                    .senderTimezone(row.senderTz())
                    .serverTimestamp(row.serverTs())
                    .payload(payload)
                    .meta(meta)
                    .build();

        } catch (Exception e) {
            log.error("Failed to map OutboxRow -> Message: {}", e.getMessage(), e);
            return null;
        }
    }


    /**
     * HistoryRow -> Message (deserialize full JSON)
     */
    @Nullable
    @Override
    public Message fromHistoryRow(@Nullable HistoryRow row) {
        if (row == null) return null;

        try {
            return mapper.readValue(row.payloadJson(), Message.class);
        } catch (Exception e) {
            log.error("Failed to map HistoryRow -> Message: {}", e.getMessage(), e);
            return null;
        }
    }


    // ===== Helpers =====

    @NotNull
    String serialize(@NotNull Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            log.error("Failed to serialize payload/meta -> Message: {}", e.getMessage(), e);
            return null;
        }
    }

    @Nullable
    MessageType typeOrNull(@Nullable String typeName) {
        return typeName == null ? null : MessageType.valueOf(typeName);
    }

    @Nullable
    Message.Payload deserializePayload(@Nullable String json) {
        if (json == null || json.isBlank()) return null;

        try {
            return mapper.readValue(json, Message.Payload.class);
        } catch (Exception e) {
            log.error("Failed to deserialize payload -> Message: {}", e.getMessage(), e);
            return null;
        }
    }

    @Nullable
    Map<String, String> deserializeMeta(@Nullable String json) {
        if (json == null || json.isBlank()) return null;

        try {
            return mapper.readValue(json, META_TYPE_REF);
        } catch (Exception e) {
            log.error("Failed to deserialize meta -> Message: {}", e.getMessage(), e);
            return null;
        }
    }
}
