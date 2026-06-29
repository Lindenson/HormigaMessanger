package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.conversation.Chats;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.domain.conversation.SendCheck;
import org.hormigas.ws.domain.message.Message;

/**
 * Front stage of the persistent/cached inbound pipelines (CHAT_IN / SIGNAL_IN). Enforces the
 * conversation send-guard (member + not blocked, UC-H07/FR-MSG-01) and stamps the authentic
 * {@code recipientId} from the conversation — the sender is the authenticated id stamped by the WS
 * transport, never the client field. A rejected send is short-circuited via {@code error}, so the
 * router skips persistence/delivery and {@link FinalStage} marks it not-done.
 */
@Slf4j
@ApplicationScoped
@RequiredArgsConstructor
public class AuthorizationStage implements PipelineStage<RouterContext<Message>> {

    private final Chats chats;

    @Override
    public Uni<RouterContext<Message>> apply(RouterContext<Message> ctx) {
        Message msg = ctx.getPayload();
        String sender = msg.getSenderId();
        return chats.evaluateSend(msg.getConversationId(), sender)
                .map(decision -> {
                    if (decision.check() == SendCheck.ALLOW) {
                        Conversation conv = decision.conversation();
                        String recipient = sender.equals(conv.clientId()) ? conv.masterId() : conv.clientId();
                        return ctx.withPayload(msg.toBuilder().recipientId(recipient).build());
                    }
                    log.warn("{} {} rejected ({}) conv={} sender={}", msg.getType(), msg.getMessageId(),
                            decision.check(), msg.getConversationId(), sender);
                    return ctx.withError(new SecurityException("send rejected: " + decision.check()));
                })
                .onFailure().recoverWithItem(err -> {
                    log.error("Authorization failed for {}: {}", msg.getMessageId(), err.getMessage());
                    return ctx.withError(err);
                });
    }
}
