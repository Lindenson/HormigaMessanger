package org.hormigas.ws.domain.credentials;

/**
 * Identity of a chat participant, sourced from the edge (Ory Oathkeeper) headers:
 * {@code X-User-Id} → id, {@code X-User} → name, {@code X-Role} → role (MASTER/CLIENT),
 * {@code X-User-Email} → email. {@code role}/{@code email} may be null where not yet known
 * (e.g. session-reconstructed presence entries).
 */
public record ClientData(String id, String name, String role, String email) { }
