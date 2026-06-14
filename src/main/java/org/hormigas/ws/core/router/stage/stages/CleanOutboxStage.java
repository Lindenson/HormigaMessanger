package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import org.hormigas.ws.ports.outbox.OutboxManager;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.message.Message;

@ApplicationScoped
@RequiredArgsConstructor
public class CleanOutboxStage implements PipelineStage<RouterContext<Message>> {

    private final OutboxManager<Message> outboxManager;

    @Override
    public Uni<RouterContext<Message>> apply(RouterContext<Message> ctx) {
            return outboxManager.remove(ctx.getPayload())
                    .onItem().invoke(ctx::setPersisted)
                    .replaceWith(ctx)
                    .onFailure().invoke(ctx::setError)
                    .onFailure().recoverWithItem(ctx);
    }
}

