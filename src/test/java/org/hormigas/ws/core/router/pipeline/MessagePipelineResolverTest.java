package org.hormigas.ws.core.router.pipeline;

import org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("MessagePipelineResolver — message type → pipeline routing")
class MessagePipelineResolverTest {

    private final MessagePipelineResolver resolver = new MessagePipelineResolver();

    @ParameterizedTest
    @CsvSource({
            "CHAT_IN,       INBOUND_PERSISTENT",
            "CHAT_OUT,      OUTBOUND_CACHED",
            "CHAT_ACK,      ACK_PERSISTENT",
            "SIGNAL_IN,     INBOUND_CACHED",
            "SIGNAL_OUT,    OUTBOUND_CACHED",
            "SIGNAL_ACK,    ACK_CACHED",
            "SERVICE_OUT,   OUTBOUND_DIRECT",
            "PRESENT_INIT,  INBOUND_DIRECT",
            "PRESENT_JOIN,  INBOUND_DIRECT",
            "PRESENT_LEAVE, INBOUND_DIRECT"
    })
    @DisplayName("each routed type maps to its configured pipeline")
    void mapsEachTypeToPipeline(MessageType type, PipelineType expected) {
        Message msg = Message.builder().type(type).build();
        assertEquals(expected, resolver.resolvePipeline(msg));
    }

    @Test
    @DisplayName("a null message resolves to SKIP")
    void nullMessageResolvesToSkip() {
        assertEquals(PipelineType.SKIP, resolver.resolvePipeline(null));
    }

    @Test
    @DisplayName("a message with null type resolves to SKIP")
    void nullTypeResolvesToSkip() {
        Message msg = Message.builder().type(null).build();
        assertEquals(PipelineType.SKIP, resolver.resolvePipeline(msg));
    }

    @ParameterizedTest
    @CsvSource({"READ_IN", "READ_OUT"})
    @DisplayName("types handled outside the router (READ_*) have no policy → SKIP")
    void unmappedTypeResolvesToSkip(MessageType type) {
        Message msg = Message.builder().type(type).build();
        assertEquals(PipelineType.SKIP, resolver.resolvePipeline(msg));
    }
}
