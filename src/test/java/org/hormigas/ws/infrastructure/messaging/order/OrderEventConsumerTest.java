package org.hormigas.ws.infrastructure.messaging.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import io.smallrye.reactive.messaging.memory.InMemorySource;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import org.hormigas.ws.core.conversation.ConversationService;
import org.hormigas.ws.core.conversation.ConversationService.CreateResult;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.ports.message.MessageModeration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Verifies the Order-event inbound adapter maps {@code order.events} envelopes onto the SAME core
 * ops the REST adapter uses (dual-driven). Core ops are mocked — this isolates the adapter mapping;
 * create/freeze behaviour itself is covered by their own unit tests. Driven via the in-memory
 * connector (no Kafka broker), which backs the {@code order-events-in} channel under {@code %test}.
 */
@QuarkusTest
@DisplayName("OrderEventConsumer — Kafka adapter over create-chat (UC-H01) + freeze (UC-H04)")
class OrderEventConsumerTest {

    @Inject
    @Any
    InMemoryConnector connector;

    @Inject
    ObjectMapper mapper;

    @InjectMock
    ConversationService conversations;

    @InjectMock
    MessageModeration moderation;

    private InMemorySource<String> source;

    @BeforeEach
    void setup() {
        source = connector.source("order-events-in");
    }

    @Test
    @DisplayName("master-interested → createChat for the pair, orderId carried as metadata")
    void masterInterestedCreatesChat() throws Exception {
        when(conversations.createChat(eq("c1"), eq("m1"), anyMap()))
                .thenReturn(Uni.createFrom().item(new CreateResult(conv("conv1", "c1", "m1"), true)));

        source.send(envelope("order.master.interested",
                Map.of("clientId", "c1", "masterId", "m1", "orderId", "o1")));

        verify(conversations, timeout(2000))
                .createChat(eq("c1"), eq("m1"), argThat(md -> "o1".equals(md.get("orderId"))));
        verify(moderation, after(200).never()).freezeByOrder(any(), any());
    }

    @Test
    @DisplayName("contract-reached → resolve chat by pair, then freezeByOrder")
    void contractReachedFreezes() throws Exception {
        when(conversations.findByPair("c1", "m1"))
                .thenReturn(Uni.createFrom().item(conv("conv1", "c1", "m1")));
        when(moderation.freezeByOrder("conv1", "o1")).thenReturn(Uni.createFrom().item(3));

        source.send(envelope("order.contract.concluded",
                Map.of("clientId", "c1", "masterId", "m1", "orderId", "o1")));

        verify(moderation, timeout(2000)).freezeByOrder("conv1", "o1");
    }

    @Test
    @DisplayName("contract-reached for an unknown pair → nothing frozen, no crash")
    void contractReachedUnknownPairNoop() throws Exception {
        when(conversations.findByPair("cX", "mX")).thenReturn(Uni.createFrom().nullItem());

        source.send(envelope("order.contract.concluded",
                Map.of("clientId", "cX", "masterId", "mX", "orderId", "o9")));

        verify(conversations, timeout(2000)).findByPair("cX", "mX");
        verify(moderation, after(200).never()).freezeByOrder(any(), any());
    }

    @Test
    @DisplayName("an unrelated order event type is ignored")
    void unrelatedTypeIgnored() throws Exception {
        source.send(envelope("order.cancelled", Map.of("clientId", "c1", "masterId", "m1")));

        verify(conversations, after(300).never()).createChat(any(), any(), any());
        verify(moderation, never()).freezeByOrder(any(), any());
    }

    @Test
    @DisplayName("an unparseable message is acked (logged), not retried into a poison loop")
    void unparseableMessageAcked() {
        source.send("}{ not json");

        verify(conversations, after(300).never()).createChat(any(), any(), any());
        verify(moderation, never()).freezeByOrder(any(), any());
    }

    private String envelope(String type, Map<String, String> payload) throws Exception {
        return mapper.writeValueAsString(Map.of(
                "eventId", "e-" + type,
                "eventType", type,
                "occurredAt", "2026-06-19T10:00:00Z",
                "payload", payload));
    }

    private static Conversation conv(String id, String clientId, String masterId) {
        return new Conversation(id, clientId, masterId, Map.of(), false, false, Instant.now(), Instant.now());
    }
}
