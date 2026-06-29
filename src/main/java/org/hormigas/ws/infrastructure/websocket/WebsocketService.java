package org.hormigas.ws.infrastructure.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.websockets.next.*;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpClosedException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.credits.ChannelFilter;
import org.hormigas.ws.core.credits.filter.InboundMessageFilter;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.domain.session.ClientSession;
import org.hormigas.ws.domain.validator.ValidationResult;
import org.hormigas.ws.domain.validator.Validator;
import org.hormigas.ws.infrastructure.websocket.inbound.InboundPublisher;
import org.hormigas.ws.infrastructure.websocket.utils.WebSocketUtils;
import org.hormigas.ws.ports.notifier.Coordinator;
import org.hormigas.ws.ports.session.SessionRegistry;

/**
 * Pure transport adapter for the WebSocket channel. It does only adapter-level work: deserialize the
 * frame, stamp the authenticated sender from the session (transport-auth translation — never trust the
 * client's {@code senderId}), apply the synchronous credit filter, validate, and publish into the
 * inbound pipeline. All routing and use cases (membership/recipient resolution, read receipts,
 * system-ack confirmation) live downstream in the pipeline stages, keyed by {@code MessageType} — this
 * class holds NO business logic and reaches the core only through the {@link InboundPublisher}.
 */
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
            // Stamp the authenticated sender from the session — the client-supplied senderId is never
            // trusted (anti-impersonation, S1). Recipient resolution + membership/block checks happen
            // downstream in the pipeline (AuthorizationStage), keyed by MessageType.
            Message message = objectMapper.readValue(rawJson, Message.class)
                    .toBuilder().senderId(clientSession.getClientId()).build();

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

            // F3: a message dropped at ingress (queue full) must not be silently lost — tell the sender.
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

    @OnError
    public void onError(WebSocketConnection connection, Throwable throwable) {
        if (throwable instanceof HttpClosedException) {
            log.warn("WebSocket connection closed: {}", connection.id());
        } else {
            log.error("WebSocket connection error for {}: {}", connection.id(), throwable.getMessage(), throwable);
        }
    }
}
