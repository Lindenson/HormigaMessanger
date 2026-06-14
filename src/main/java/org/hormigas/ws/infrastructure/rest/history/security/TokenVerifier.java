package org.hormigas.ws.infrastructure.rest.history.security;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.infrastructure.websocket.security.JwtValidator;

import java.util.Optional;


@ApplicationScoped
public class TokenVerifier {

    @Inject
    JwtValidator jwtValidator;

    public Optional<ClientData> verifyBearerToken(String authorization) {
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            return Optional.empty();
        }

        String token = authorization.substring(7);
        return jwtValidator.validate(token);
    }
}
