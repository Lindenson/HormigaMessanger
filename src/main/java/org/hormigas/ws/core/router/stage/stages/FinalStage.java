package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.message.Message;


@ApplicationScoped
@RequiredArgsConstructor
public class FinalStage implements PipelineStage<RouterContext<Message>> {

    @Override
    public Uni<RouterContext<Message>> apply(RouterContext<Message> ctx) {
        return Uni.createFrom().item(() -> {
            boolean done;

            if (ctx.hasError()) {
                ctx.setDone(false);
                return ctx;
            }

            var pipelineType = ctx.getPipelineType();

            switch (pipelineType) {
                case INBOUND_PERSISTENT -> done = ctx.getPersisted().isSuccess();
                case INBOUND_CACHED, INBOUND_DIRECT -> done = ctx.getDelivered().isSuccess();
                case OUTBOUND_CACHED, OUTBOUND_DIRECT, OUTBOUND_TRANSIENT -> done = ctx.getDelivered().isSuccess();
                case ACK_PERSISTENT -> done = ctx.getAcknowledged().isSuccess();
                case ACK_CACHED -> done = ctx.getCached().isSuccess();
                default -> done = false;
            }

            ctx.setDone(done);
            return ctx;
        });
    }
}
