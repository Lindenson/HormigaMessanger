package org.hormigas.ws.core.router.context;

import org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("InboundPrototype — builds the outbound router context from an inbound message")
class InboundPrototypeTest {

    private static final String SERVER_ID = "server-generated-id";

    private final IdGenerator idGenerator = mock(IdGenerator.class);
    private final InboundPrototype prototype = new InboundPrototype(idGenerator);

    private Message inbound(MessageType type, String messageId, String correlationId) {
        return Message.builder()
                .type(type)
                .messageId(messageId)
                .correlationId(correlationId)
                .ackId(42L)
                .senderId("s").recipientId("r").conversationId("c")
                .build();
    }

    @ParameterizedTest
    @CsvSource({"CHAT_IN, CHAT_OUT", "SIGNAL_IN, SIGNAL_OUT", "TYPING_IN, TYPING_OUT", "CHAT_ACK, CHAT_ACK", "PRESENT_JOIN, PRESENT_JOIN"})
    @DisplayName("inbound type is switched to its outbound counterpart (CHAT_IN→CHAT_OUT, SIGNAL_IN→SIGNAL_OUT, TYPING_IN→TYPING_OUT, else unchanged)")
    void switchesType(MessageType in, MessageType expectedOut) {
        when(idGenerator.generateId()).thenReturn(SERVER_ID);
        var ctx = prototype.createOutboundContext(PipelineType.INBOUND_PERSISTENT, inbound(in, "client-msg", "corr"));
        assertEquals(expectedOut, ctx.getPayload().getType());
    }

    @Test
    @DisplayName("the server assigns a fresh messageId (the client id is not trusted)")
    void regeneratesMessageId() {
        when(idGenerator.generateId()).thenReturn(SERVER_ID);
        var ctx = prototype.createOutboundContext(PipelineType.INBOUND_PERSISTENT, inbound(MessageType.CHAT_IN, "client-msg", null));
        assertEquals(SERVER_ID, ctx.getPayload().getMessageId());
    }

    @Test
    @DisplayName("for a chat message, correlationId becomes the original (client) messageId")
    void correlationIdFromMessageIdForChat() {
        when(idGenerator.generateId()).thenReturn(SERVER_ID);
        var ctx = prototype.createOutboundContext(PipelineType.INBOUND_PERSISTENT, inbound(MessageType.CHAT_IN, "client-msg", "ignored"));
        assertEquals("client-msg", ctx.getPayload().getCorrelationId());
    }

    @Test
    @DisplayName("for a CHAT_ACK, the incoming correlationId is preserved")
    void correlationIdPreservedForAck() {
        when(idGenerator.generateId()).thenReturn(SERVER_ID);
        var ctx = prototype.createOutboundContext(PipelineType.ACK_PERSISTENT, inbound(MessageType.CHAT_ACK, "ack-msg", "delivered-id"));
        assertEquals("delivered-id", ctx.getPayload().getCorrelationId());
    }

    @Test
    @DisplayName("ackId is carried through and a server timestamp is stamped")
    void carriesAckIdAndStampsServerTs() {
        when(idGenerator.generateId()).thenReturn(SERVER_ID);
        var ctx = prototype.createOutboundContext(PipelineType.INBOUND_PERSISTENT, inbound(MessageType.CHAT_IN, "m", null));
        assertEquals(42L, ctx.getPayload().getAckId());
        assertTrue(ctx.getPayload().getServerTimestamp() > 0);
    }

    @Test
    @DisplayName("the chosen pipeline type is set on the context")
    void setsPipelineType() {
        when(idGenerator.generateId()).thenReturn(SERVER_ID);
        var ctx = prototype.createOutboundContext(PipelineType.INBOUND_CACHED, inbound(MessageType.SIGNAL_IN, "m", null));
        assertEquals(PipelineType.INBOUND_CACHED, ctx.getPipelineType());
    }
}
