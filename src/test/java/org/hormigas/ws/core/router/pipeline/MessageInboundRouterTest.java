package org.hormigas.ws.core.router.pipeline;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.router.PipelineResolver;
import org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import org.hormigas.ws.core.router.context.InboundPrototype;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.stages.*;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@DisplayName("MessageInboundRouter — assembles the inbound pipeline per resolved type")
@SuppressWarnings("unchecked")
class MessageInboundRouterTest {

    private final PipelineResolver<Message, MessageType> resolver = mock(PipelineResolver.class);
    private final OutboxStage outbox = mock(OutboxStage.class);
    private final DeliveryStage delivery = mock(DeliveryStage.class);
    private final AckStage ack = mock(AckStage.class);
    private final CleanCacheStage cleanCache = mock(CleanCacheStage.class);
    private final CacheStage cache = mock(CacheStage.class);
    private final FinalStage finalStage = mock(FinalStage.class);
    private final InboundPrototype prototype = mock(InboundPrototype.class);
    private final TetrisSentStage tetrisSent = mock(TetrisSentStage.class);
    private final TetrisAckStage tetrisAck = mock(TetrisAckStage.class);

    private final MessageInboundRouter router = new MessageInboundRouter(
            resolver, outbox, delivery, ack, cleanCache, cache, finalStage, prototype, tetrisSent, tetrisAck);

    private final Message msg = Message.builder().type(MessageType.CHAT_IN).messageId("m1").build();

    private static Uni<RouterContext<Message>> echo(InvocationOnMock i) {
        RouterContext<Message> c = i.getArgument(0);
        return Uni.createFrom().item(c);
    }

    @BeforeEach
    void passThrough() {
        when(outbox.apply(any())).thenAnswer(MessageInboundRouterTest::echo);
        when(delivery.apply(any())).thenAnswer(MessageInboundRouterTest::echo);
        when(ack.apply(any())).thenAnswer(MessageInboundRouterTest::echo);
        when(cleanCache.apply(any())).thenAnswer(MessageInboundRouterTest::echo);
        when(cache.apply(any())).thenAnswer(MessageInboundRouterTest::echo);
        when(tetrisSent.apply(any())).thenAnswer(MessageInboundRouterTest::echo);
        when(tetrisAck.apply(any())).thenAnswer(MessageInboundRouterTest::echo);
        when(finalStage.apply(any())).thenAnswer(MessageInboundRouterTest::echo);
    }

    private void resolveAs(PipelineType type) {
        when(resolver.resolvePipeline(msg)).thenReturn(type);
        when(prototype.createOutboundContext(eq(type), eq(msg))).thenReturn(
                RouterContext.<Message>builder().payload(msg).pipelineType(type).build());
    }

    @Test
    @DisplayName("INBOUND_PERSISTENT persists, delivers, then runs ack+cache+tetris-sent and finalizes")
    void inboundPersistent() {
        resolveAs(PipelineType.INBOUND_PERSISTENT);
        router.routeIn(msg).await().indefinitely();
        verify(outbox).apply(any());
        verify(delivery).apply(any());
        verify(ack).apply(any());
        verify(cache).apply(any());
        verify(tetrisSent).apply(any());
        verify(finalStage).apply(any());
    }

    @Test
    @DisplayName("INBOUND_CACHED delivers then caches (no outbox persistence)")
    void inboundCached() {
        resolveAs(PipelineType.INBOUND_CACHED);
        router.routeIn(msg).await().indefinitely();
        verify(delivery).apply(any());
        verify(cache).apply(any());
        verify(outbox, never()).apply(any());
    }

    @Test
    @DisplayName("INBOUND_DIRECT delivers then finalizes only")
    void inboundDirect() {
        resolveAs(PipelineType.INBOUND_DIRECT);
        router.routeIn(msg).await().indefinitely();
        verify(delivery).apply(any());
        verify(cache, never()).apply(any());
        verify(outbox, never()).apply(any());
    }

    @Test
    @DisplayName("ACK_PERSISTENT advances the watermark then cleans the idempotency cache")
    void ackPersistent() {
        resolveAs(PipelineType.ACK_PERSISTENT);
        router.routeIn(msg).await().indefinitely();
        verify(tetrisAck).apply(any());
        verify(cleanCache).apply(any());
        verify(outbox, never()).apply(any());
    }

    @Test
    @DisplayName("ACK_CACHED cleans the idempotency cache only")
    void ackCached() {
        resolveAs(PipelineType.ACK_CACHED);
        router.routeIn(msg).await().indefinitely();
        verify(cleanCache).apply(any());
        verify(tetrisAck, never()).apply(any());
    }

    @Test
    @DisplayName("SKIP runs the final stage only")
    void skip() {
        resolveAs(PipelineType.SKIP);
        router.routeIn(msg).await().indefinitely();
        verify(finalStage).apply(any());
        verify(delivery, never()).apply(any());
    }
}
