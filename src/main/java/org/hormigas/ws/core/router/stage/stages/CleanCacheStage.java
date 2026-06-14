package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import org.hormigas.ws.ports.idempotency.IdempotencyManager;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.message.Message;

@ApplicationScoped
@RequiredArgsConstructor
public class CleanCacheStage implements PipelineStage<RouterContext<Message>> {

    private final IdempotencyManager<Message> idempotencyManager;

    @Override
    public Uni<RouterContext<Message>> apply(RouterContext<Message> ctx) {
            return idempotencyManager.remove(ctx.getPayload())
                    .onItem().invoke(ctx::setCached)
                    .replaceWith(ctx)
                    .onFailure().invoke(ctx::setError)
                    .onFailure().recoverWithItem(ctx);
    }
}
