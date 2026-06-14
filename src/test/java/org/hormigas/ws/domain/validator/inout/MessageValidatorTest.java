package org.hormigas.ws.domain.validator.inout;

import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.domain.validator.ValidationResult;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageValidatorTest {

    private final MessageValidator validator = new MessageValidator();

    private Message.Payload textPayload(String text) {
        return Message.Payload.builder()
                .kind("text")
                .body(text)
                .build();
    }

    private Message baseValidMessage() {
        return Message.builder()
                .type(MessageType.CHAT_IN)
                .senderId("senderA")
                .recipientId("recipientB")
                .conversationId("conv123")
                .messageId("msg123")
                .senderTimestamp(System.currentTimeMillis())
                .senderTimezone("UTC")
                .payload(textPayload("hello"))
                .meta(Map.of())
                .build();
    }

    @Test
    void validMessageShouldPass() {
        Message msg = baseValidMessage();
        ValidationResult r = validator.validate(msg);
        assertTrue(r.isValid());
    }

    @Test
    void nullTypeShouldFail() {
        Message msg = baseValidMessage().toBuilder().type(null).build();
        ValidationResult r = validator.validate(msg);
        assertFalse(r.isValid());
        assertTrue(r.errors().stream().anyMatch(e -> e.contains("type: must not be null")));
    }

    @Test
    void unsupportedTypeShouldFail() {
        Message msg = baseValidMessage().toBuilder().type(MessageType.SIGNAL_OUT).build();
        ValidationResult r = validator.validate(msg);
        assertFalse(r.isValid());
        assertTrue(r.errors().stream().anyMatch(e -> e.contains("unsupported inbound message type")));
    }

    @Test
    void selfMessagingShouldFail() {
        Message msg = baseValidMessage().toBuilder().recipientId("senderA").build();
        ValidationResult r = validator.validate(msg);
        assertFalse(r.isValid());
        assertTrue(r.errors().stream().anyMatch(e -> e.contains("self-messaging")));
    }

    @Test
    void chatAckMustHaveCorrelationId() {
        Message msg = baseValidMessage().toBuilder()
                .type(MessageType.CHAT_ACK)
                .correlationId(null)
                .build();
        ValidationResult r = validator.validate(msg);
        assertFalse(r.isValid());
        assertTrue(r.errors().stream().anyMatch(e -> e.contains("correlationId")));
    }

    @Test
    void chatAckWithValidCorrelationIdShouldPass() {
        Message msg = baseValidMessage().toBuilder()
                .type(MessageType.CHAT_ACK)
                .correlationId("prev-msg-1")
                .build();
        ValidationResult r = validator.validate(msg);
        assertTrue(r.isValid());
    }

    @Test
    void blankSenderIdShouldFail() {
        Message msg = baseValidMessage().toBuilder().senderId("  ").build();
        ValidationResult r = validator.validate(msg);
        assertFalse(r.isValid());
        assertTrue(r.errors().stream().anyMatch(e -> e.contains("senderId")));
    }

    @Test
    void overlyLongMessageIdShouldFail() {
        Message msg = baseValidMessage().toBuilder().messageId("x".repeat(300)).build();
        ValidationResult r = validator.validate(msg);
        assertFalse(r.isValid());
        assertTrue(r.errors().stream().anyMatch(e -> e.contains("length >")));
    }

    @Test
    void negativeSenderTimestampShouldFail() {
        Message msg = baseValidMessage().toBuilder().senderTimestamp(-1).build();
        ValidationResult r = validator.validate(msg);
        assertFalse(r.isValid());
        assertTrue(r.errors().stream().anyMatch(e -> e.contains("senderTimestamp")));
    }

    @Test
    void missingSenderTimezoneShouldFail() {
        Message msg = baseValidMessage().toBuilder().senderTimezone(null).build();
        ValidationResult r = validator.validate(msg);
        assertFalse(r.isValid());
        assertTrue(r.errors().stream().anyMatch(e -> e.contains("senderTimezone")));
    }

    @Test
    void textPayloadEmptyBodyShouldFail() {
        Message msg = baseValidMessage().toBuilder()
                .payload(Message.Payload.builder().kind("text").body("").build())
                .build();
        ValidationResult r = validator.validate(msg);
        assertFalse(r.isValid());
        assertTrue(r.errors().stream().anyMatch(e -> e.contains("payload.body")));
    }

    @Test
    void unknownPayloadKindShouldFail() {
        Message msg = baseValidMessage().toBuilder()
                .payload(Message.Payload.builder().kind("unknown").body("abc").build())
                .build();
        ValidationResult r = validator.validate(msg);
        assertFalse(r.isValid());
        assertTrue(r.errors().stream().anyMatch(e -> e.contains("unknown kind")));
    }

    @Test
    void emptyMetaAllowed() {
        Message msg = baseValidMessage().toBuilder().meta(Map.of()).build();
        ValidationResult r = validator.validate(msg);
        assertTrue(r.isValid());
    }
}
