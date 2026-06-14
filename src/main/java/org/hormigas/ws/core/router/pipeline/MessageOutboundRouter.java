package org.hormigas.ws.core.router.pipeline;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.router.OutboundRouter;
import org.hormigas.ws.core.router.PipelineResolver;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.logger.RouterLogger;
import org.hormigas.ws.core.router.logger.inout.OutboundRouterLogger;
import org.hormigas.ws.core.router.stage.stages.CacheStage;
import org.hormigas.ws.core.router.stage.stages.DeliveryStage;
import org.hormigas.ws.core.router.stage.stages.FinalStage;
import org.hormigas.ws.core.router.stage.stages.TetrisSentStage;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageEnvelope;
import org.hormigas.ws.domain.message.MessageType;

@Slf4j
@ApplicationScoped
@RequiredArgsConstructor
public class MessageOutboundRouter implements OutboundRouter<Message> {

    private final PipelineResolver<Message, MessageType> pipelineResolver;
    private final DeliveryStage deliveryStage;
    private final TetrisSentStage tetrisSentStage;
    private final CacheStage cacheStage;
    private final FinalStage finalStage;

    private final RouterLogger<Message> logger = new OutboundRouterLogger();

    @Override
    public Uni<MessageEnvelope<Message>> routeOut(Message message) {

        var pipeline = pipelineResolver.resolvePipeline(message);

        message = message.toBuilder().serverTimestamp(System.currentTimeMillis()).build();
        logger.logRoutingStart(message, pipeline);
        var context = RouterContext.<Message>builder()
                .pipelineType(pipeline)
                .payload(message).build();

        Uni<RouterContext<Message>> processed = switch (pipeline) {
            case OUTBOUND_CACHED -> deliveryStage.apply(context)
                    .onItem().transformToUni(cacheStage::apply)
                    .onItem().transformToUni(tetrisSentStage::apply)
                    .onItem().transformToUni(finalStage::apply);

            case OUTBOUND_DIRECT -> deliveryStage.apply(context)
                    .onItem().transformToUni(finalStage::apply);

            case SKIP -> finalStage.apply(context);

            default -> {
                log.error("Unhandled pipeline: {} for message: {}", pipeline, message);
                yield finalStage.apply(context.withError(new IllegalStateException("Unhandled pipeline: " + pipeline)));
            }
        };

        return logAndEnvelope(processed);
    }

    private Uni<MessageEnvelope<Message>> logAndEnvelope(Uni<RouterContext<Message>> processed) {
        return processed.onItem()
                .invoke(logger::logRoutingResult).onItem()
                .transform(pc ->
                        MessageEnvelope.<Message>builder()
                                .message(pc.getPayload())
                                .processed(pc.isDone())
                                .build());
    }
}
