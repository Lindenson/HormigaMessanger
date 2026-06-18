package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.outbox.OutboxManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("OutboxStage — persists the message and records the outcome on the context")
@SuppressWarnings("unchecked")
class OutboxStageTest {

    private final OutboxManager<Message> outbox = mock(OutboxManager.class);
    private final OutboxStage stage = new OutboxStage(outbox);
    private final Message payload = Message.builder().messageId("m1").build();

    private RouterContext<Message> ctx() {
        return RouterContext.<Message>builder().payload(payload).pipelineType(PipelineType.INBOUND_PERSISTENT).build();
    }

    @Test
    @DisplayName("a successful save marks the context persisted")
    void successMarksPersisted() {
        when(outbox.save(payload)).thenReturn(Uni.createFrom().item(StageResult.passed()));
        RouterContext<Message> out = stage.apply(ctx()).await().indefinitely();
        assertTrue(out.getPersisted().isSuccess());
        assertSame(payload, out.getPayload());
    }

    @Test
    @DisplayName("an UPDATED save swaps in the db-stamped payload and marks persisted")
    void updatedSwapsPayload() {
        Message stamped = Message.builder().id(99L).messageId("m1").build();
        when(outbox.save(payload)).thenReturn(Uni.createFrom().item(StageResult.updated(stamped)));
        RouterContext<Message> out = stage.apply(ctx()).await().indefinitely();
        assertSame(stamped, out.getPayload());
        assertTrue(out.getPersisted().isUpdated());
    }

    @Test
    @DisplayName("a save failure is captured as a context error (never thrown)")
    void failureCapturedAsError() {
        when(outbox.save(any())).thenReturn(Uni.createFrom().failure(new RuntimeException("db down")));
        RouterContext<Message> out = stage.apply(ctx()).await().indefinitely();
        assertTrue(out.hasError());
    }
}
