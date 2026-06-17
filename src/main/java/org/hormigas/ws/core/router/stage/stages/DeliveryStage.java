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
import org.hormigas.ws.ports.message.ReadReceipts;
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
    private final ReadReceipts receipts;

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
        if (!ctx.getPersisted().isSuccess()) {
            ctx.setDelivered(StageResult.skipped());
            return Uni.createFrom().item(ctx);
        }

        return canDeliver(ctx)
                .flatMap(canDeliver -> canDeliver ? deliverWithRetry(ctx)
                        : Uni.createFrom().item(StageResult.<Message>skipped()))
                .onItem().invoke(ctx::setDelivered)
                .call(result -> markDelivered(ctx.getPayload(), result))
                .replaceWith(ctx)
                .onFailure().invoke(ctx::setError)
                .onFailure().recoverWithItem(ctx);
    }

    /**
     * SENT→DELIVERED once a chat message is actually pushed to the recipient (UC-U13, decision #2).
     * Only on PASSED (genuinely delivered, not SKIPPED/held-offline) and only for CHAT_OUT; the
     * status write is best-effort and must never fail the delivery.
     */
    private Uni<?> markDelivered(Message msg, StageResult<Message> result) {
        if (msg.getType() != MessageType.CHAT_OUT || !result.isPassed()) {
            return Uni.createFrom().voidItem();
        }
        return receipts.markDelivered(msg.getMessageId())
                .onFailure().recoverWithItem(0);
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
