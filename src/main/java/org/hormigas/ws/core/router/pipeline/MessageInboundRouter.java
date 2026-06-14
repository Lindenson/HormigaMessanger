package org.hormigas.ws.core.router.pipeline;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.router.InboundRouter;
import org.hormigas.ws.core.router.concurency.PipelineMerger;
import org.hormigas.ws.core.router.context.InboundPrototype;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.logger.RouterLogger;
import org.hormigas.ws.core.router.logger.inout.InboundRouterLogger;
import org.hormigas.ws.core.router.stage.stages.*;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageEnvelope;
import org.hormigas.ws.domain.message.MessageType;

import java.util.List;

@Slf4j
@ApplicationScoped
@RequiredArgsConstructor
public class MessageInboundRouter implements InboundRouter<Message> {

    private final org.hormigas.ws.core.router.PipelineResolver<Message, MessageType> pipelineResolver;
    private final OutboxStage outboxStage;
    private final DeliveryStage deliveryStage;
    private final AckStage ackStage;
    private final CleanCacheStage cleanCacheStage;
    private final CacheStage cacheStage;
    private final FinalStage finalStage;
    private final InboundPrototype prototype;
    private final TetrisSentStage tetrisSentStage;
    private final TetrisAckStage tetrisAckStage;


    private final RouterLogger<Message> logger = new InboundRouterLogger();
    private final PipelineMerger<Message> merger = new PipelineMerger<>();

    @Override
    public Uni<MessageEnvelope<Message>> routeIn(Message message) {

        var pipeline = pipelineResolver.resolvePipeline(message);
        logger.logRoutingStart(message, pipeline);
        var context = prototype.createOutboundContext(pipeline, message);

        Uni<RouterContext<Message>> processed = switch (pipeline) {
            case INBOUND_PERSISTENT -> outboxStage.apply(context)
                    .onItem().transformToUni(deliveryStage::apply)
                    .onItem().transformToUni(ctx ->
                            merger.runParallel(ctx,
                            List.of(ackStage, cacheStage, tetrisSentStage)
                    ))
                    .onItem().transformToUni(finalStage::apply);

            case INBOUND_CACHED -> deliveryStage.apply(context)
                    .onItem().transformToUni(cacheStage::apply)
                    .onItem().transformToUni(finalStage::apply);

            case INBOUND_DIRECT -> deliveryStage.apply(context)
                    .onItem().transformToUni(finalStage::apply);

            case ACK_PERSISTENT -> tetrisAckStage.apply(context)
                    .onItem().transformToUni(cleanCacheStage::apply)
                    .onItem().transformToUni(finalStage::apply);

            case ACK_CACHED -> cleanCacheStage.apply(context)
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
                .transform(pc -> MessageEnvelope.<Message>builder()
                        .message(pc.getPayload())
                        .processed(pc.isDone())
                        .build());
    }
}
