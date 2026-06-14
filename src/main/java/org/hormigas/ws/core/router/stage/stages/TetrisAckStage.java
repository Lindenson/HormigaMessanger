package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.RequiredArgsConstructor;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.tetris.TetrisMarker;


@ApplicationScoped
@RequiredArgsConstructor
public class TetrisAckStage implements PipelineStage<RouterContext<Message>> {

    @Inject
    TetrisMarker<Message> tetrisMarker;

    @Override
    public Uni<RouterContext<Message>> apply(RouterContext<Message> ctx) {
        return tetrisMarker.onAck(ctx.getPayload())
                .onItem().invoke(ctx::setAcknowledged)
                .replaceWith(ctx)
                .onFailure().invoke(ctx::setError)
                .onFailure().recoverWithItem(ctx);
    }
}