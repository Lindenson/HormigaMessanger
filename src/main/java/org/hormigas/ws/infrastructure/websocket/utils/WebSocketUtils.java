package org.hormigas.ws.infrastructure.websocket.utils;

import io.quarkus.websockets.next.CloseReason;
import io.quarkus.websockets.next.WebSocketConnection;
import io.vertx.core.json.Json;
import jakarta.inject.Singleton;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.infrastructure.security.IdentityHeaders;
import org.hormigas.ws.domain.credentials.ClientData;
import org.slf4j.LoggerFactory;

import java.util.Optional;

@Singleton
public class WebSocketUtils {

    private final static CloseReason closeReason = new CloseReason(1000, "Unauthenticated");
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(WebSocketUtils.class);

    public Optional<String> encodeMessage(Message message) {
        try {
            return Optional.of(Json.encode(message));
        } catch (Exception e) {
            log.error("Invalid message format: {}", message, e);
            return Optional.empty();
        }
    }

    /**
     * Identity comes from the edge (Ory Oathkeeper) headers on the WS handshake — no token
     * validation here (the edge already authenticated). Missing identity → close the connection.
     */
    public Optional<ClientData> getValidatedClientData(WebSocketConnection connection) {
        var req = connection.handshakeRequest();
        Optional<ClientData> client = IdentityHeaders.fromHeaders(
                req.header(IdentityHeaders.USER_ID),
                req.header(IdentityHeaders.USER_NAME),
                req.header(IdentityHeaders.USER_ROLE),
                req.header(IdentityHeaders.USER_EMAIL));
        if (client.isEmpty()) {
            log.warn("Missing identity headers on WS handshake — closing");
            connection.closeAndAwait(closeReason);
        }
        return client;
    }

    public CloseReason getCloseReason() {
        return closeReason;
    }
}
