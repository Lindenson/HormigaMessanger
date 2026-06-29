package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.deadletter.DeadLetterStore;
import org.hormigas.ws.ports.deadletter.DeliveryConfirmations;

/**
 * SYSTEM_ACK pipeline (Strategy C, ADR-014): records the delivery confirmation so the cleanup sweep
 * retracts the dead-letter DRAFT. Ownership-guarded — only the notice's own recipient (the
 * authenticated sender) may confirm/retract it, otherwise any client could replay a correlationId.
 * {@code correlationId} is the notice's messageId (preserved by the inbound prototype).
 */
@Slf4j
@ApplicationScoped
@RequiredArgsConstructor
public class SystemAckStage implements PipelineStage<RouterContext<Message>> {

    private final DeadLetterStore<Message> deadLetters;
    private final DeliveryConfirmations confirmations;

    @Override
    public Uni<RouterContext<Message>> apply(RouterContext<Message> ctx) {
        Message msg = ctx.getPayload();
        String confirmer = msg.getSenderId();
        String noticeId = msg.getCorrelationId();
        return deadLetters.isRecipient(noticeId, confirmer)
                .flatMap(owned -> {
                    if (!owned) {
                        log.warn("SYSTEM_ACK ignored: {} is not the recipient of notice {}", confirmer, noticeId);
                        return Uni.createFrom().item(ctx);
                    }
                    return confirmations.confirm(noticeId).replaceWith(ctx);
                })
                .onFailure().recoverWithItem(err -> {
                    log.error("SYSTEM_ACK handling failed for notice {}: {}", noticeId, err.getMessage());
                    return ctx;
                });
    }
}
