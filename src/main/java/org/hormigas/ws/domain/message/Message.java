package org.hormigas.ws.domain.message;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.*;

import java.util.Map;

@Value
@Builder(toBuilder = true)
@NoArgsConstructor(force = true)
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = Message.MessageBuilder.class)
public class Message {
    @Builder.Default
    @With
    Long id = -1L;
    Long ackId;
    MessageType type;

    String senderId;
    String recipientId;
    String conversationId;

    String messageId;
    String correlationId;

    long senderTimestamp;
    String senderTimezone;
    int creditsAvailable;

    long serverTimestamp;
    String sessionId;
    long sequenceNumber;

    Payload payload;
    Map<String, String> meta;

    @Value
    @Builder(toBuilder = true)
    @NoArgsConstructor(force = true)
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonDeserialize(builder = Payload.PayloadBuilder.class)
    public static class Payload {
        String kind;
        String body;

        @JsonPOJOBuilder(withPrefix = "")
        public static class PayloadBuilder {
        }
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static class MessageBuilder {
    }
}