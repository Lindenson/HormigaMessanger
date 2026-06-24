package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import org.hormigas.ws.core.router.persist.InboundPersistBatcher;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.message.Message;

@ApplicationScoped
@RequiredArgsConstructor
public class OutboxStage implements PipelineStage<RouterContext<Message>> {

    // Group-commit (plan B): enqueue into the batcher rather than persisting one row per call.
    // The batcher coalesces concurrent inbound persists into one transaction per batch and
    // completes this message's Uni when its batch commits. Everything below this stage is unchanged.
    private final InboundPersistBatcher batcher;

    @Override
    public Uni<RouterContext<Message>> apply(RouterContext<Message> ctx) {
        return batcher.enqueue(ctx.getPayload())
                .onItem().transform(result -> {
                    var updatedContext = result.isUpdated() ? ctx.withPayload(result.payload()) : ctx;
                    updatedContext.setPersisted(result);
                    return updatedContext;
                })
                .onFailure().invoke(ctx::setError)
                .onFailure().recoverWithItem(ctx);
    }
}
