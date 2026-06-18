package org.hormigas.ws.core.router.pipeline;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.router.PipelineResolver;
import org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.stages.CacheStage;
import org.hormigas.ws.core.router.stage.stages.DeliveryStage;
import org.hormigas.ws.core.router.stage.stages.FinalStage;
import org.hormigas.ws.core.router.stage.stages.TetrisSentStage;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("MessageOutboundRouter — assembles the outbound pipeline per resolved type")
@SuppressWarnings("unchecked")
class MessageOutboundRouterTest {

    private final PipelineResolver<Message, MessageType> resolver = mock(PipelineResolver.class);
    private final DeliveryStage delivery = mock(DeliveryStage.class);
    private final TetrisSentStage tetrisSent = mock(TetrisSentStage.class);
    private final CacheStage cache = mock(CacheStage.class);
    private final FinalStage finalStage = mock(FinalStage.class);
    private final MessageOutboundRouter router =
            new MessageOutboundRouter(resolver, delivery, tetrisSent, cache, finalStage);

    private final Message msg = Message.builder().type(MessageType.CHAT_OUT).messageId("m1").build();

    private static Uni<RouterContext<Message>> echo(InvocationOnMock i) {
        RouterContext<Message> c = i.getArgument(0);
        return Uni.createFrom().item(c);
    }

    private void allStagesPassThrough() {
        when(delivery.apply(any())).thenAnswer(MessageOutboundRouterTest::echo);
        when(cache.apply(any())).thenAnswer(MessageOutboundRouterTest::echo);
        when(tetrisSent.apply(any())).thenAnswer(MessageOutboundRouterTest::echo);
        when(finalStage.apply(any())).thenAnswer(MessageOutboundRouterTest::echo);
    }

    @Test
    @DisplayName("OUTBOUND_CACHED runs delivery → cache → tetris-sent → final")
    void outboundCachedRunsFullChain() {
        when(resolver.resolvePipeline(msg)).thenReturn(PipelineType.OUTBOUND_CACHED);
        allStagesPassThrough();

        router.routeOut(msg).await().indefinitely();

        verify(delivery).apply(any());
        verify(cache).apply(any());
        verify(tetrisSent).apply(any());
        verify(finalStage).apply(any());
    }

    @Test
    @DisplayName("OUTBOUND_DIRECT runs delivery → final only (no caching / watermark)")
    void outboundDirectSkipsCacheAndTetris() {
        when(resolver.resolvePipeline(msg)).thenReturn(PipelineType.OUTBOUND_DIRECT);
        allStagesPassThrough();

        router.routeOut(msg).await().indefinitely();

        verify(delivery).apply(any());
        verify(finalStage).apply(any());
        verify(cache, never()).apply(any());
        verify(tetrisSent, never()).apply(any());
    }

    @Test
    @DisplayName("SKIP runs the final stage only")
    void skipRunsFinalOnly() {
        when(resolver.resolvePipeline(msg)).thenReturn(PipelineType.SKIP);
        when(finalStage.apply(any())).thenAnswer(MessageOutboundRouterTest::echo);

        router.routeOut(msg).await().indefinitely();

        verify(finalStage).apply(any());
        verify(delivery, never()).apply(any());
    }
}
