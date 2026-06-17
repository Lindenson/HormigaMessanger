package org.hormigas.ws.infrastructure.rest.history.security;

import jakarta.enterprise.context.ApplicationScoped;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.infrastructure.security.IdentityHeaders;

import java.util.Optional;

/**
 * REST identity resolver. Reads the Ory Oathkeeper-injected headers (the edge already
 * authenticated); no token validation here.
 */
@ApplicationScoped
public class TokenVerifier {

    public Optional<ClientData> fromHeaders(String userId, String userName, String role, String email) {
        return IdentityHeaders.fromHeaders(userId, userName, role, email);
    }
}
