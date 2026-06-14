package org.hormigas.ws.core.router.logger.inout;

import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.logger.RouterLogger;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;


@Slf4j
public class OutboundRouterLogger implements RouterLogger<Message> {

    @Override
    public void logRoutingStart(Message message, Object pipelineType) {
        log.debug("Routing outbound message {} to pipeline {}", message.getMessageId(), pipelineType);
    }

    @Override
    public void logRoutingResult(RouterContext<Message> ctx) {
        String messageId = ctx.getPayload().getMessageId();

        if (ctx.hasError()) {
            log.error("Outbound message {} failed: {}", messageId, ctx.getError().getMessage(), ctx.getError());
            return;
        }

        switch (ctx.getPipelineType()) {
            case OUTBOUND_CACHED -> {
                logStage(ctx.getCached(), messageId, "cached");
                logStage(ctx.getDelivered(), messageId, "delivered");
            }
            case OUTBOUND_DIRECT -> logStage(ctx.getDelivered(), messageId, "delivered");
        }

        log.debug("Outbound message {} processed successfully via {}", messageId, ctx.getPipelineType());
    }

    private void logStage(StageResult<?> stageResult, String messageId, String operation) {
        if (!stageResult.isSuccess()) {
            log.debug("Outbound message {} was not {}", messageId, operation);
        }
    }
}
