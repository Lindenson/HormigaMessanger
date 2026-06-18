package org.hormigas.ws.core.router.stage.stages;

import org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("FinalStage — computes the done flag from the pipeline's terminal result")
class FinalStageTest {

    private final FinalStage stage = new FinalStage();

    private RouterContext<Message> ctx(PipelineType type) {
        return RouterContext.<Message>builder()
                .payload(Message.builder().messageId("m1").build()).pipelineType(type).build();
    }

    @Test
    @DisplayName("a context carrying an error is not done")
    void errorIsNotDone() {
        RouterContext<Message> c = ctx(PipelineType.INBOUND_PERSISTENT);
        c.setError(new RuntimeException("x"));
        assertFalse(stage.apply(c).await().indefinitely().isDone());
    }

    @Test
    @DisplayName("INBOUND_PERSISTENT is done when persisted succeeded")
    void inboundPersistentDoneOnPersist() {
        RouterContext<Message> c = ctx(PipelineType.INBOUND_PERSISTENT);
        c.setPersisted(StageResult.passed());
        assertTrue(stage.apply(c).await().indefinitely().isDone());
    }

    @Test
    @DisplayName("OUTBOUND_CACHED is done when delivery succeeded")
    void outboundDoneOnDelivery() {
        RouterContext<Message> c = ctx(PipelineType.OUTBOUND_CACHED);
        c.setDelivered(StageResult.passed());
        assertTrue(stage.apply(c).await().indefinitely().isDone());
    }

    @Test
    @DisplayName("ACK_PERSISTENT is done when acknowledged; ACK_CACHED when cached")
    void ackPipelinesDone() {
        RouterContext<Message> ackP = ctx(PipelineType.ACK_PERSISTENT);
        ackP.setAcknowledged(StageResult.passed());
        assertTrue(stage.apply(ackP).await().indefinitely().isDone());

        RouterContext<Message> ackC = ctx(PipelineType.ACK_CACHED);
        ackC.setCached(StageResult.passed());
        assertTrue(stage.apply(ackC).await().indefinitely().isDone());
    }

    @Test
    @DisplayName("an unsatisfied terminal result is not done")
    void unsatisfiedNotDone() {
        assertFalse(stage.apply(ctx(PipelineType.INBOUND_PERSISTENT)).await().indefinitely().isDone());
    }
}
