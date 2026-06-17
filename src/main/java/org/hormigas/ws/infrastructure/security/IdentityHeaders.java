package org.hormigas.ws.infrastructure.security;

import org.hormigas.ws.domain.credentials.ClientData;

import java.util.Optional;

/**
 * The Ory Oathkeeper proxy-header identity contract, shared by the WS handshake and REST.
 * The edge authenticates the Ory Kratos session and injects these trusted headers; the service
 * trusts them (no local token validation). Same trust model as the other Hormigas services.
 */
public final class IdentityHeaders {

    public static final String USER_ID = "X-User-Id";
    public static final String USER_NAME = "X-User";
    public static final String USER_ROLE = "X-Role";
    public static final String USER_EMAIL = "X-User-Email";

    private IdentityHeaders() {}

    /**
     * Build identity from header values. Present iff a non-blank user id is supplied; name falls
     * back to email then id. Returns empty when unauthenticated (no id).
     */
    public static Optional<ClientData> fromHeaders(String id, String name, String role, String email) {
        if (id == null || id.isBlank()) {
            return Optional.empty();
        }
        String displayName = (name != null && !name.isBlank()) ? name
                : (email != null && !email.isBlank()) ? email
                : id;
        return Optional.of(new ClientData(id, displayName, role, email));
    }
}
