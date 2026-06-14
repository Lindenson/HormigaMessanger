package org.hormigas.ws.infrastructure.persistance.postgres.mappers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.HistoryRow;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxRow;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageMapperFullTest {

    private MessageMapper mapper;

    @BeforeEach
    void setup() {
        mapper = new MessageMapper();
        mapper.mapper = new ObjectMapper();
    }

    // -----------------------------------------
    // toOutboxMessage
    // -----------------------------------------
    @Test
    void toOutboxMessage_nullMessage_returnsNull() {
        assertNull(mapper.toOutboxMessage(null));
    }

    @Test
    void toOutboxMessage_fullMessage() {
        Map<String, String> meta = new HashMap<>();
        meta.put("foo", "bar");

        Message.Payload payload = Message.Payload.builder().kind("text").body("hello").build();
        Message msg = Message.builder()
                .senderId("s")
                .recipientId("r")
                .conversationId("conv-1")
                .type(MessageType.CHAT_IN)
                .messageId("m1")
                .correlationId("c1")
                .senderTimestamp(100)
                .senderTimezone("UTC")
                .serverTimestamp(200)
                .payload(payload)
                .meta(meta)
                .build();

        OutboxMessage out = mapper.toOutboxMessage(msg);

        assertEquals("CHAT_IN", out.type());
        assertEquals("s", out.senderId());
        assertEquals("r", out.recipientId());
        assertEquals("conv-1", out.conversationId());
        assertEquals("m1", out.messageId());
        assertEquals("c1", out.correlationId());
        assertEquals(100, out.senderTimestamp());
        assertEquals("UTC", out.senderTimezone());
        assertEquals(200, out.serverTimestamp());
        assertNotNull(out.payloadJson());
        assertNotNull(out.metaJson());
    }

    // -----------------------------------------
    // toHistoryRow
    // -----------------------------------------
    @Test
    void toHistoryRow_nullMessage_returnsNull() {
        assertNull(mapper.toHistoryRow(null));
    }

    @Test
    void toHistoryRow_fullMessage() {
        Message msg = Message.builder()
                .senderId("s")
                .recipientId("r")
                .conversationId("conv-1")
                .messageId("m1")
                .build();

        HistoryRow row = mapper.toHistoryRow(msg);

        assertEquals("m1", row.messageId());
        assertEquals("conv-1", row.conversationId());
        assertEquals("s", row.senderId());
        assertEquals("r", row.recipientId());
        assertNotNull(row.payloadJson());
        assertNotNull(row.createdAt());
    }

    // -----------------------------------------
    // toDomainMessage
    // -----------------------------------------
    @Test
    void toDomainMessage_nullRow_returnsNull() {
        assertNull(mapper.toDomainMessage(null));
    }

    @Test
    void toDomainMessage_withPayloadAndMeta() {
        String payloadJson = "{\"kind\":\"text\",\"body\":\"hi\"}";
        String metaJson = "{\"foo\":\"bar\"}";

        OutboxRow row = new OutboxRow(
                1L, "s", "r", "conv-1", "m1", "c1",
                100, "UTC", 200, "CHAT_ACK",
                payloadJson, metaJson, Instant.now(), Instant.now().plusSeconds(60)
        );

        Message msg = mapper.toDomainMessage(row);

        assertEquals("s", msg.getSenderId());
        assertEquals("r", msg.getRecipientId());
        assertEquals("conv-1", msg.getConversationId());
        assertEquals(MessageType.CHAT_ACK, msg.getType());
        assertEquals("m1", msg.getMessageId());
        assertEquals("c1", msg.getCorrelationId());
        assertEquals(100, msg.getSenderTimestamp());
        assertEquals(200, msg.getServerTimestamp());
        assertNotNull(msg.getPayload());
        assertEquals("text", msg.getPayload().getKind());
        assertEquals("hi", msg.getPayload().getBody());
        assertNotNull(msg.getMeta());
        assertEquals("bar", msg.getMeta().get("foo"));
    }

    @Test
    void toDomainMessage_withoutPayloadAndMeta() {
        OutboxRow row = new OutboxRow(
                2L, "s2", "r2", "conv-2", "m2", null,
                101, "UTC", 201, null,
                null, null, Instant.now(), Instant.now().plusSeconds(60)
        );

        Message msg = mapper.toDomainMessage(row);

        assertEquals("s2", msg.getSenderId());
        assertEquals("r2", msg.getRecipientId());
        assertEquals("conv-2", msg.getConversationId());
        assertNull(msg.getType());
        assertEquals("m2", msg.getMessageId());
        assertNull(msg.getCorrelationId());
        assertEquals(101, msg.getSenderTimestamp());
        assertEquals(201, msg.getServerTimestamp());
        assertNull(msg.getPayload());
        assertNull(msg.getMeta());
    }

    // -----------------------------------------
    // fromHistoryRow
    // -----------------------------------------
    @Test
    void fromHistoryRow_nullRow_returnsNull() {
        assertNull(mapper.fromHistoryRow(null));
    }

    @Test
    void fromHistoryRow_validJson() {
        Message msg = Message.builder()
                .senderId("s").recipientId("r").conversationId("conv-1").messageId("m1").build();
        HistoryRow row = new HistoryRow("m1", "conv-1", "s", "r",
                mapper.serialize(msg), Instant.now());

        Message result = mapper.fromHistoryRow(row);
        assertEquals("s", result.getSenderId());
        assertEquals("r", result.getRecipientId());
        assertEquals("conv-1", result.getConversationId());
        assertEquals("m1", result.getMessageId());
    }

    // -----------------------------------------
    // serialize/deserialize edge cases
    // -----------------------------------------
    @Test
    void serialize_andDeserializePayload() {
        Message.Payload payload = Message.Payload.builder().kind("k").body("b").build();
        String json = mapper.serialize(payload);
        Message.Payload deserialized = mapper.deserializePayload(json);
        assertEquals("k", deserialized.getKind());
        assertEquals("b", deserialized.getBody());
    }

    @Test
    void serialize_andDeserializeMeta() {
        Map<String, String> meta = new HashMap<>();
        meta.put("foo", "bar");
        String json = mapper.serialize(meta);
        Map<String, String> deserialized = mapper.deserializeMeta(json);
        assertEquals("bar", deserialized.get("foo"));
    }

    @Test
    void typeOrNull_returnsExpected() {
        assertEquals(MessageType.CHAT_IN, mapper.typeOrNull("CHAT_IN"));
        assertNull(mapper.typeOrNull(null));
    }
}
