package org.hormigas.ws.infrastructure.websocket.utils;

import io.quarkus.websockets.next.CloseReason;
import io.quarkus.websockets.next.WebSocketConnection;
import io.vertx.core.json.Json;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.infrastructure.websocket.security.JwtValidator;
import org.hormigas.ws.domain.credentials.ClientData;
import org.slf4j.LoggerFactory;

import java.util.Optional;

@Singleton
public class WebSocketUtils {

    @Inject
    JwtValidator jwtValidator;


    public static final String AUTHORIZATION = "Authorization";
    private final static CloseReason closeReason = new CloseReason(1000, "Invalid token");
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(WebSocketUtils.class);

    public Optional<String> encodeMessage(Message message) {
        try {
            return Optional.of(Json.encode(message));
        } catch (Exception e) {
            log.error("Invalid message format: {}", message, e);
            return Optional.empty();
        }
    }

    public Optional<ClientData> getValidatedClientData(WebSocketConnection connection) {
        String token = connection.handshakeRequest().header(AUTHORIZATION);

        log.debug("Checking token {}", token);

        if (token == null || !token.startsWith("Bearer")) {
            connection.closeAndAwait(closeReason);
            log.warn("Empty token");
            return Optional.empty();
        }

        try {
            token = token.substring(7);
        } catch (Exception e) {
            log.error("Invalid token format: {}", token, e);
            return Optional.empty();
        }
        return jwtValidator.validate(token);
    }

    public CloseReason getCloseReason() {
        return closeReason;
    }
}
