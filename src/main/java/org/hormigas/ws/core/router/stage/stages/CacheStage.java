package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.idempotency.IdempotencyManager;

@ApplicationScoped
@RequiredArgsConstructor
public class CacheStage implements PipelineStage<RouterContext<Message>> {

    private final IdempotencyManager<Message> manager;

    @Override
    public Uni<RouterContext<Message>> apply(RouterContext<Message> ctx) {
        // don't save in idempotent storage if not delivered
        if (!ctx.getDelivered().isSuccess()) {
            ctx.setCached(StageResult.skipped());
            return Uni.createFrom().item(ctx);
        }

        return manager.add(ctx.getPayload())
                .onItem().invoke(ctx::setCached)
                .replaceWith(ctx)
                .onFailure().invoke(ctx::setError)
                .onFailure().recoverWithItem(ctx);
    }
}