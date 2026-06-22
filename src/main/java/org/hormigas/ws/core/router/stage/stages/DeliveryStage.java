package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.channel.DeliveryChannel;
import org.hormigas.ws.ports.idempotency.IdempotencyManager;
import org.hormigas.ws.core.router.PipelineResolver.PipelineType;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;

import java.time.Duration;


@Slf4j
@ApplicationScoped
@RequiredArgsConstructor
public class DeliveryStage implements PipelineStage<RouterContext<Message>> {

    private final MessengerConfig messengerConfig;
    private final DeliveryChannel<Message> channel;
    private final IdempotencyManager<Message> idempotencyManager;

    private Duration minBackoff;
    private Duration maxBackoff;
    private int maxRetries;

    @PostConstruct
    void init() {
        minBackoff = Duration.ofMillis(messengerConfig.channel().minBackoffMs());
        maxBackoff = Duration.ofMillis(messengerConfig.channel().maxBackoffMs());
        maxRetries = messengerConfig.channel().maxRetries();
    }

    @Override
    public Uni<RouterContext<Message>> apply(RouterContext<Message> ctx) {
        // Persistence gate: never deliver a message that was supposed to be persisted but wasn't.
        // The non-persistent INBOUND pipelines (signaling = INBOUND_CACHED, presence = INBOUND_DIRECT)
        // legitimately have no outbox stage, so `persisted` stays UNKNOWN — they MUST still deliver
        // live (this is what makes WebRTC signaling work; the gate previously dropped it silently).
        // All other pipelines keep the original gate (deliver only on persist success), so the
        // outbound/offline-redelivery behaviour is unchanged.
        boolean nonPersistentInbound = ctx.getPipelineType() == PipelineType.INBOUND_CACHED
                || ctx.getPipelineType() == PipelineType.INBOUND_DIRECT;
        if (!nonPersistentInbound && !ctx.getPersisted().isSuccess()) {
            ctx.setDelivered(StageResult.skipped());
            return Uni.createFrom().item(ctx);
        }

        return canDeliver(ctx)
                .flatMap(canDeliver -> canDeliver ? deliverWithRetry(ctx)
                        : Uni.createFrom().item(StageResult.<Message>skipped()))
                .onItem().invoke(ctx::setDelivered)
                .replaceWith(ctx)
                .onFailure().invoke(ctx::setError)
                .onFailure().recoverWithItem(ctx);
    }

    private Uni<Boolean> canDeliver(RouterContext<Message> ctx) {
        Message msg = ctx.getPayload();
        if (msg.getType() == MessageType.CHAT_ACK
                || msg.getType() == MessageType.PRESENT_JOIN
                || msg.getType() == MessageType.PRESENT_INIT
        ) {
            return Uni.createFrom().item(Boolean.TRUE);
        }
        return idempotencyManager.isInProgress(msg)
                .map(inProgress -> !inProgress);
    }

    private Uni<StageResult<Message>> deliverWithRetry(RouterContext<Message> ctx) {
        Uni<StageResult<Message>> delivery = channel.deliver(ctx.getPayload());
        if (messengerConfig.channel().retry()) {
            return delivery.onFailure().retry()
                    .withBackOff(minBackoff, maxBackoff)
                    .atMost(maxRetries);
        }
        return delivery;
    }
}
