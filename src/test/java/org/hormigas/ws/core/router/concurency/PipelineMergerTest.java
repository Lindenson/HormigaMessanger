package org.hormigas.ws.core.router.concurency;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PipelineMerger — runs stages in parallel and merges into one context")
class PipelineMergerTest {

    private final PipelineMerger<Message> merger = new PipelineMerger<>();
    private final Message payload = Message.builder().messageId("m1").build();

    private RouterContext<Message> baseCtx() {
        return RouterContext.<Message>builder().payload(payload).pipelineType(PipelineType.INBOUND_PERSISTENT).build();
    }

    private PipelineStage<RouterContext<Message>> stageReturning(RouterContext<Message> out) {
        return ctx -> Uni.createFrom().item(out);
    }

    @Test
    @DisplayName("preserves the payload and pipeline type of the source context")
    void preservesPayloadAndPipeline() {
        RouterContext<Message> merged = merger.runParallel(baseCtx(),
                List.of(stageReturning(baseCtx()), stageReturning(baseCtx()))).await().indefinitely();

        assertSame(payload, merged.getPayload());
        assertEquals(PipelineType.INBOUND_PERSISTENT, merged.getPipelineType());
    }

    @Test
    @DisplayName("propagates a stage error into the merged context")
    void propagatesStageError() {
        RouterContext<Message> erroring = baseCtx();
        RuntimeException boom = new RuntimeException("stage boom");
        erroring.setError(boom);

        RouterContext<Message> merged = merger.runParallel(baseCtx(),
                List.of(stageReturning(baseCtx()), stageReturning(erroring))).await().indefinitely();

        assertTrue(merged.hasError());
        assertSame(boom, merged.getError());
    }

    @Test
    @DisplayName("a stage that fails reactively is recovered — the merge still completes")
    void recoversFromReactiveFailure() {
        PipelineStage<RouterContext<Message>> failing = ctx -> Uni.createFrom().failure(new RuntimeException("async boom"));

        RouterContext<Message> merged = merger.runParallel(baseCtx(),
                List.of(failing, stageReturning(baseCtx()))).await().indefinitely();

        assertNotNull(merged);
        assertFalse(merged.hasError()); // recovered to the original ctx, which carried no error
    }

    @Test
    @DisplayName("if every stage reports a failed result, the aggregate is failed")
    void aggregatesFailedWhenAStageFails() {
        RouterContext<Message> failedPersist = baseCtx();
        failedPersist.setPersisted(StageResult.failed());

        RouterContext<Message> merged = merger.runParallel(baseCtx(),
                List.of(stageReturning(failedPersist))).await().indefinitely();

        assertTrue(merged.getPersisted().isFailed());
    }
}
