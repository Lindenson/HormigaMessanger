package org.hormigas.ws.core.router.logger.inout;

import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.logger.RouterLogger;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;


@Slf4j
public class InboundRouterLogger implements RouterLogger<Message> {

    @Override
    public void logRoutingStart(Message message, Object pipelineType) {
        log.debug("Routing inbound message {} to pipeline {}", message.getMessageId(), pipelineType);
    }

    @Override
    public void logRoutingResult(RouterContext<Message> ctx) {
        String messageId = ctx.getPayload().getMessageId();

        if (ctx.hasError()) {
            log.error("Inbound message {} failed: {}", messageId, ctx.getError().getMessage(), ctx.getError());
            return;
        }

        switch (ctx.getPipelineType()) {
            case INBOUND_PERSISTENT -> logPersistenceFlow(ctx, messageId);
            case INBOUND_CACHED -> logCachedFlow(ctx, messageId);
            case ACK_PERSISTENT -> logPersistenceCleanup(ctx, messageId);
            case ACK_CACHED -> logCacheCleanup(ctx, messageId);
        }

        log.debug("Inbound message {} processed successfully via {}", messageId, ctx.getPipelineType());
    }

    private void logPersistenceFlow(RouterContext<Message> ctx, String messageId) {
        logStage(ctx.getPersisted(), messageId, "persisted");
        logStage(ctx.getDelivered(), messageId, "delivered");
        logStage(ctx.getCached(), messageId, "cached");
    }

    private void logCachedFlow(RouterContext<Message> ctx, String messageId) {
        logStage(ctx.getDelivered(), messageId, "delivered");
        logStage(ctx.getCached(), messageId, "cached");
    }

    private void logPersistenceCleanup(RouterContext<Message> ctx, String messageId) {
        logStage(ctx.getPersisted(), messageId, "outbox cleaned");
        logStage(ctx.getCached(), messageId, "cache cleaned");
    }

    private void logCacheCleanup(RouterContext<Message> ctx, String messageId) {
        logStage(ctx.getCached(), messageId, "cache cleaned");
    }

    private void logStage(StageResult<?> stageResult, String messageId, String operation) {
        if (!stageResult.isSuccess()) {
            log.debug("Inbound message {} was not {}", messageId, operation);
        }
    }
}
