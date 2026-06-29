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
    private final AuthorizationStage authorizationStage;
    private final OutboxStage outboxStage;
    private final DeliveryStage deliveryStage;
    private final AckStage ackStage;
    private final CleanCacheStage cleanCacheStage;
    private final CacheStage cacheStage;
    private final FinalStage finalStage;
    private final InboundPrototype prototype;
    private final TetrisSentStage tetrisSentStage;
    private final TetrisAckStage tetrisAckStage;
    private final ReadStage readStage;
    private final SystemAckStage systemAckStage;


    private final RouterLogger<Message> logger = new InboundRouterLogger();
    private final PipelineMerger<Message> merger = new PipelineMerger<>();

    @Override
    public Uni<MessageEnvelope<Message>> routeIn(Message message) {

        var pipeline = pipelineResolver.resolvePipeline(message);
        logger.logRoutingStart(message, pipeline);
        var context = prototype.createOutboundContext(pipeline, message);

        Uni<RouterContext<Message>> processed = switch (pipeline) {
            // Persistent chat (CHAT_IN): authorize (member + not blocked, stamp recipient) FIRST;
            // a rejected send short-circuits to FinalStage (no persist/deliver).
            case INBOUND_PERSISTENT -> authorizationStage.apply(context)
                    .onItem().transformToUni(ctx -> ctx.hasError()
                            ? finalStage.apply(ctx)
                            : outboxStage.apply(ctx)
                                    .onItem().transformToUni(deliveryStage::apply)
                                    .onItem().transformToUni(c ->
                                            merger.runParallel(c, List.of(ackStage, cacheStage, tetrisSentStage)))
                                    .onItem().transformToUni(finalStage::apply));

            // Signaling (SIGNAL_IN): same send-guard, then live (cached) delivery.
            case INBOUND_CACHED -> authorizationStage.apply(context)
                    .onItem().transformToUni(ctx -> ctx.hasError()
                            ? finalStage.apply(ctx)
                            : deliveryStage.apply(ctx)
                                    .onItem().transformToUni(cacheStage::apply)
                                    .onItem().transformToUni(finalStage::apply));

            case INBOUND_DIRECT -> deliveryStage.apply(context)
                    .onItem().transformToUni(finalStage::apply);

            case ACK_PERSISTENT -> tetrisAckStage.apply(context)
                    .onItem().transformToUni(cleanCacheStage::apply)
                    .onItem().transformToUni(finalStage::apply);

            case ACK_CACHED -> cleanCacheStage.apply(context)
                    .onItem().transformToUni(finalStage::apply);

            // Read receipt (READ_IN): mark read + push READ_OUT to the peer.
            case READ -> readStage.apply(context)
                    .onItem().transformToUni(finalStage::apply);

            // System-notice confirmation (SYSTEM_ACK): ownership-checked retract of the dead-letter draft.
            case ACK_SYSTEM -> systemAckStage.apply(context)
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
