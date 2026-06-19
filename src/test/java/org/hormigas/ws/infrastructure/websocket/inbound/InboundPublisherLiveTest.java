package org.hormigas.ws.infrastructure.websocket.inbound;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.InjectMock;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.hormigas.ws.core.router.InboundRouter;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageEnvelope;
import org.hormigas.ws.domain.message.MessageType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Live behaviour of the backpressure publisher: drive the REAL {@link InboundPublisher} with a
 * gated sink (an unfinished {@code routeIn} Uni) and observe the actual reactive reactions —
 * bounded queue, drop-on-full (publish returns false), drain on completion, and re-accept.
 * Queue cap is shrunk to 3 via a test profile so the bound is reached deterministically.
 */
@QuarkusTest
@TestProfile(InboundPublisherLiveTest.SmallQueue.class)
@DisplayName("InboundPublisher — live backpressure reactions")
class InboundPublisherLiveTest {

    public static class SmallQueue implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("processing.messages.inbound.queue-size", "3");
        }
    }

    @Inject
    InboundPublisher publisher;

    @InjectMock
    InboundRouter<Message> router;

    private Message msg(String id) {
        return Message.builder().type(MessageType.CHAT_IN).messageId(id)
                .senderId("s").recipientId("r").conversationId("c").build();
    }

    @Test
    @DisplayName("accepts up to the queue cap, drops the overflow, then drains and accepts again")
    void boundedThenDropThenDrain() throws InterruptedException {
        // sink that never completes until we release it → the in-flight slot is held open
        CompletableFuture<MessageEnvelope<Message>> gate = new CompletableFuture<>();
        when(router.routeIn(any())).thenAnswer(i -> Uni.createFrom().completionStage(gate));

        // fill the queue to the cap (3) — all accepted
        assertTrue(publisher.publish(msg("m1")));
        assertTrue(publisher.publish(msg("m2")));
        assertTrue(publisher.publish(msg("m3")));

        // the 4th overflows → dropped (publish returns false; sender would be notified by WS layer)
        assertFalse(publisher.publish(msg("m4")), "over-cap message must be dropped");

        // release the sink → the queue drains
        gate.complete(MessageEnvelope.<Message>builder().message(msg("m1")).processed(true).build());

        // after draining the publisher accepts again (bounded wait — drain is async on the stream)
        assertTrue(acceptsAgainWithin(2000), "publisher should accept after the queue drains");
    }

    private boolean acceptsAgainWithin(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (publisher.publish(msg("drain-probe"))) {
                return true;
            }
            Thread.sleep(40);
        }
        return false;
    }
}
