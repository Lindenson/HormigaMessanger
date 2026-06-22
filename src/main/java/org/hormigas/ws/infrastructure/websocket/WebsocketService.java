package org.hormigas.ws.infrastructure.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.websockets.next.*;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpClosedException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.conversation.ConversationService;
import org.hormigas.ws.core.credits.ChannelFilter;
import org.hormigas.ws.core.credits.filter.InboundMessageFilter;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.domain.session.ClientSession;
import org.hormigas.ws.domain.validator.ValidationResult;
import org.hormigas.ws.domain.validator.Validator;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.infrastructure.websocket.inbound.InboundPublisher;
import org.hormigas.ws.infrastructure.websocket.utils.WebSocketUtils;
import org.hormigas.ws.ports.channel.DeliveryChannel;
import org.hormigas.ws.ports.message.ReadReceipts;
import org.hormigas.ws.ports.notifier.Coordinator;
import org.hormigas.ws.ports.session.SessionRegistry;

@Slf4j
@WebSocket(path = "/ws")
@ApplicationScoped
public class WebsocketService {

    @Inject
    InboundPublisher incomingPublisher;

    @Inject
    WebSocketUtils webSocketUtils;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    Validator<Message> validator;

    @Inject
    SessionRegistry<WebSocketConnection> registry;

    @Inject
    Coordinator<WebSocketConnection> coordinator;

    @Inject
    ConversationService conversations;

    @Inject
    ReadReceipts receipts;

    @Inject
    DeliveryChannel<Message> deliveryChannel;


    private final ChannelFilter<Message, WebSocketConnection> channelFilter = new InboundMessageFilter<>();


    @OnOpen
    public void onOpen(WebSocketConnection connection) {
        webSocketUtils.getValidatedClientData(connection)
                .ifPresentOrElse(client -> coordinator.join(client, connection),
                        () -> connection.closeAndAwait(webSocketUtils.getCloseReason())
                );
    }

    @OnClose
    public void onClose(WebSocketConnection connection) {
        coordinator.leave(connection);
    }

    /**
     * Liveness: the server auto-pings (see {@code auto-ping-interval}); a live client replies pong.
     * Refreshing activity here lets the session reaper distinguish a silent-but-alive client from a
     * dead connection (which stops ponging and is force-closed after the idle timeout).
     */
    @OnPongMessage
    public void onPong(io.vertx.core.buffer.Buffer data, WebSocketConnection connection) {
        coordinator.active(connection);
    }


    @OnTextMessage
    public Uni<Void> onMessage(String rawJson, WebSocketConnection connection) {
        log.debug("Receiving message {}", rawJson);
        try {
            ClientSession<WebSocketConnection> clientSession = registry.getSessionByConnection(connection);
            if (clientSession == null) {
                log.warn("Received message from unregistered connection: {}", connection.id());
                return Uni.createFrom().voidItem();
            }
            Message message = objectMapper.readValue(rawJson, Message.class);
            if (!channelFilter.filter(message, clientSession)) {
                log.warn("Message (id={}): {} filtered out", message.getMessageId(), message);
                return Uni.createFrom().voidItem();
            }
            ValidationResult validated = validator.validate(message);
            if (!validated.isValid()) {
                log.warn("Invalid message (id={}): {}", message.getMessageId(), validated.errors());
                return Uni.createFrom().voidItem();
            }
            coordinator.active(connection);

            // Authenticated writes into a conversation (persistent chat AND real-time signaling) must
            // enforce membership + blacklist before acceptance (UC-U61, UC-H06). Signaling is between
            // the master and client of a chat exactly like a chat message — only the delivery strategy
            // differs (persistent vs cached), and that is decided downstream by the pipeline resolver.
            // Trust the authenticated session id as the sender, not the client-supplied field.
            if (message.getType() == MessageType.CHAT_IN || message.getType() == MessageType.SIGNAL_IN) {
                final String sender = clientSession.getClientId();
                return conversations.canSend(message.getConversationId(), sender)
                        .map(check -> {
                            if (check == ConversationService.SendCheck.ALLOW) {
                                // F3: a message dropped at ingress (queue full) must not be silently
                                // lost — tell the sender so it can retry.
                                if (!incomingPublisher.publish(message)) {
                                    notifyOverloaded(connection, message);
                                }
                            } else {
                                log.warn("{} message {} rejected ({}) conv={} sender={}",
                                        message.getType(), message.getMessageId(), check,
                                        message.getConversationId(), sender);
                            }
                            return (Void) null;
                        })
                        .onFailure().recoverWithItem(err -> {
                            log.error("Conversation guard failed for message {}: {}",
                                    message.getMessageId(), err.getMessage());
                            return null;
                        });
            }

            // Read receipt over WS (UC-U14): mark the reader's received messages READ, then push a
            // READ_OUT to the peer (the original sender) — fire-and-forget. REST /read is the fallback.
            if (message.getType() == MessageType.READ_IN) {
                return markReadAndNotify(message.getConversationId(), clientSession.getClientId());
            }

            if (!incomingPublisher.publish(message)) {
                notifyOverloaded(connection, message);
            }
        } catch (Exception e) {
            log.error("Invalid message format: {}", rawJson, e);
        }
        return Uni.createFrom().voidItem();
    }

    /**
     * Tell the sender its message was rejected at ingress (queue full / overload) so it can retry —
     * a SERVICE_OUT carrying the rejected messageId as correlationId. Fire-and-forget (UC-H08).
     */
    private void notifyOverloaded(WebSocketConnection connection, Message rejected) {
        Message notice = Message.builder()
                .type(MessageType.SERVICE_OUT)
                .correlationId(rejected.getMessageId())
                .payload(Message.Payload.builder().kind("event").body("overloaded").build())
                .serverTimestamp(System.currentTimeMillis())
                .build();
        webSocketUtils.encodeMessage(notice).ifPresent(json ->
                connection.sendText(json).subscribe().with(
                        x -> {},
                        e -> log.debug("Failed to send overload notice: {}", e.getMessage())));
    }

    private Uni<Void> markReadAndNotify(String conversationId, String reader) {
        return conversations.findById(conversationId)
                .flatMap(conv -> {
                    if (conv == null || !conv.hasParticipant(reader)) {
                        log.warn("READ_IN ignored: conv={} reader={} (missing/not a member)", conversationId, reader);
                        return Uni.createFrom().voidItem();
                    }
                    return receipts.markRead(conversationId, reader)
                            .flatMap(marked -> marked > 0
                                    ? deliveryChannel.deliver(readReceiptFor(conv, reader)).replaceWithVoid()
                                    : Uni.createFrom().voidItem());
                })
                .onFailure().recoverWithItem(() -> {
                    log.error("READ_IN handling failed for conv={} reader={}", conversationId, reader);
                    return null;
                });
    }

    /** READ_OUT pushed to the peer (the participant who is not the reader). */
    private Message readReceiptFor(Conversation conv, String reader) {
        String peer = reader.equals(conv.clientId()) ? conv.masterId() : conv.clientId();
        return Message.builder()
                .type(MessageType.READ_OUT)
                .conversationId(conv.id())
                .senderId(reader)        // who read
                .recipientId(peer)       // notified party (original sender)
                .messageId("read-" + conv.id() + "-" + reader)
                .serverTimestamp(System.currentTimeMillis())
                .build();
    }

    @OnError
    public void onError(WebSocketConnection connection, Throwable throwable) {
        if (throwable instanceof HttpClosedException) {
            log.warn("WebSocket connection closed: {}", connection.id());
        } else {
            log.error("WebSocket connection error for {}: {}", connection.id(), throwable.getMessage(), throwable);
        }
    }
}
