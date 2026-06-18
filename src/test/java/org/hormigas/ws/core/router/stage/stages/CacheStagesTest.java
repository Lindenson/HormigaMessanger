package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.idempotency.IdempotencyManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("CacheStage / CleanCacheStage — idempotency-cache writes around delivery")
@SuppressWarnings("unchecked")
class CacheStagesTest {

    private final IdempotencyManager<Message> manager = mock(IdempotencyManager.class);
    private final Message payload = Message.builder().messageId("m1").build();

    private RouterContext<Message> ctx() {
        return RouterContext.<Message>builder().payload(payload).pipelineType(PipelineType.INBOUND_PERSISTENT).build();
    }

    @Test
    @DisplayName("CacheStage skips caching when the message was not delivered")
    void cacheSkipsWhenNotDelivered() {
        CacheStage stage = new CacheStage(manager);
        RouterContext<Message> out = stage.apply(ctx()).await().indefinitely();
        assertTrue(out.getCached().isSkipped());
        verify(manager, never()).add(any());
    }

    @Test
    @DisplayName("CacheStage records the message in the idempotency cache after a successful delivery")
    void cacheAddsWhenDelivered() {
        when(manager.add(payload)).thenReturn(Uni.createFrom().item(StageResult.passed()));
        CacheStage stage = new CacheStage(manager);
        RouterContext<Message> c = ctx();
        c.setDelivered(StageResult.passed());
        RouterContext<Message> out = stage.apply(c).await().indefinitely();
        assertTrue(out.getCached().isSuccess());
        verify(manager).add(payload);
    }

    @Test
    @DisplayName("CleanCacheStage removes the message from the idempotency cache")
    void cleanRemovesFromCache() {
        when(manager.remove(payload)).thenReturn(Uni.createFrom().item(StageResult.passed()));
        CleanCacheStage stage = new CleanCacheStage(manager);
        RouterContext<Message> out = stage.apply(ctx()).await().indefinitely();
        assertTrue(out.getCached().isSuccess());
        verify(manager).remove(payload);
    }

    @Test
    @DisplayName("a cache failure is captured as a context error, not thrown")
    void cacheFailureCaptured() {
        when(manager.remove(any())).thenReturn(Uni.createFrom().failure(new RuntimeException("redis down")));
        CleanCacheStage stage = new CleanCacheStage(manager);
        RouterContext<Message> out = stage.apply(ctx()).await().indefinitely();
        assertTrue(out.hasError());
    }
}
