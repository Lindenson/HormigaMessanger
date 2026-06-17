package org.hormigas.ws.infrastructure.security;

import org.hormigas.ws.domain.credentials.ClientData;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for the Ory proxy-header identity contract (FR-SEC-01). Pure, no infra.
 */
class IdentityHeadersTest {

    @Test
    void empty_when_no_user_id() {
        assertTrue(IdentityHeaders.fromHeaders(null, "n", "MASTER", "e@x.com").isEmpty());
        assertTrue(IdentityHeaders.fromHeaders("  ", "n", "MASTER", "e@x.com").isEmpty());
    }

    @Test
    void maps_all_fields_when_present() {
        Optional<ClientData> id = IdentityHeaders.fromHeaders("u-1", "Alice", "MASTER", "a@x.com");
        assertTrue(id.isPresent());
        assertEquals("u-1", id.get().id());
        assertEquals("Alice", id.get().name());
        assertEquals("MASTER", id.get().role());
        assertEquals("a@x.com", id.get().email());
    }

    @Test
    void name_falls_back_to_email_then_id() {
        assertEquals("a@x.com", IdentityHeaders.fromHeaders("u-1", null, "CLIENT", "a@x.com").get().name());
        assertEquals("u-1", IdentityHeaders.fromHeaders("u-1", " ", "CLIENT", null).get().name());
    }
}
