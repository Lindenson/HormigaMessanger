package org.hormigas.ws.core.router.stage.stages;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.conversation.Chats;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.channel.DeliveryChannel;
import org.hormigas.ws.core.router.persist.ReadStatusBatcher;

import static org.hormigas.ws.domain.message.MessageType.READ_OUT;

/**
 * READ_IN pipeline (UC-U14): the reader (the authenticated sender) marks the messages addressed to
 * them as READ, then a {@code READ_OUT} is pushed to the peer (the original sender). Membership is
 * checked here in core; the WS transport only relays the event.
 */
@Slf4j
@ApplicationScoped
@RequiredArgsConstructor
public class ReadStage implements PipelineStage<RouterContext<Message>> {

    private final Chats chats;
    private final ReadStatusBatcher readStatus;
    private final DeliveryChannel<Message> channel;

    @Override
    public Uni<RouterContext<Message>> apply(RouterContext<Message> ctx) {
        Message msg = ctx.getPayload();
        String reader = msg.getSenderId();
        String conversationId = msg.getConversationId();
        return chats.findById(conversationId)
                .flatMap(conv -> {
                    if (conv == null || !conv.hasParticipant(reader)) {
                        log.warn("READ_IN ignored: conv={} reader={} (missing/not a member)", conversationId, reader);
                        return Uni.createFrom().item(ctx);
                    }
                    return readStatus.enqueue(conversationId, reader).flatMap(marked -> marked > 0
                            ? channel.deliver(readReceiptFor(conv, reader))
                                    .onItem().invoke(ctx::setDelivered).replaceWith(ctx)
                            : Uni.createFrom().item(ctx));
                })
                .onFailure().recoverWithItem(err -> {
                    log.error("READ_IN handling failed conv={} reader={}: {}", conversationId, reader, err.getMessage());
                    return ctx;
                });
    }

    /** READ_OUT pushed to the peer (the participant who is not the reader). */
    private Message readReceiptFor(Conversation conv, String reader) {
        String peer = reader.equals(conv.clientId()) ? conv.masterId() : conv.clientId();
        return Message.builder()
                .type(READ_OUT)
                .conversationId(conv.id())
                .senderId(reader)
                .recipientId(peer)
                .messageId("read-" + conv.id() + "-" + reader)
                .serverTimestamp(System.currentTimeMillis())
                .build();
    }
}
