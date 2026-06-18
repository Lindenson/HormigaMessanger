package org.hormigas.ws.core.session;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.core.session.tidy.CleanupStrategy;
import org.hormigas.ws.domain.credentials.ClientData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("LocalSessionRegistry — connection/client indexing (multi-connection per client)")
@SuppressWarnings("unchecked")
class LocalSessionRegistryTest {

    private LocalSessionRegistry<String> registry;

    private static final ClientData ALICE = new ClientData("A", "Alice", "CLIENT", "a@x");

    @BeforeEach
    void setUp() {
        MessengerConfig config = mock(MessengerConfig.class);
        MessengerConfig.Credits credits = mock(MessengerConfig.Credits.class);
        when(config.credits()).thenReturn(credits);
        when(credits.maxValue()).thenReturn(100);
        when(credits.refillRatePerS()).thenReturn(10);
        registry = new LocalSessionRegistry<>(config, new SimpleMeterRegistry(), mock(CleanupStrategy.class));
    }

    @Test
    @DisplayName("a registered connection is found by client id and counted")
    void registersAndFinds() {
        registry.register(ALICE, "c1");
        assertTrue(registry.isClientConnected("A"));
        assertEquals(1, registry.streamSessionsByClientId("A").count());
        assertEquals(1, registry.countAllClients());
    }

    @Test
    @DisplayName("the same client may hold several connections (multi-device capable at the registry level)")
    void allowsMultipleConnectionsPerClient() {
        registry.register(ALICE, "c1");
        registry.register(ALICE, "c2");
        assertEquals(2, registry.streamSessionsByClientId("A").count());
        assertEquals(2, registry.countAllClients());
    }

    @Test
    @DisplayName("deregistering the last connection clears the client")
    void deregisterClearsClient() {
        registry.register(ALICE, "c1");
        registry.deregister("c1");
        assertFalse(registry.isClientConnected("A"));
        assertNull(registry.getSessionByConnection("c1"));
        assertEquals(0, registry.streamSessionsByClientId("A").count());
    }

    @Test
    @DisplayName("re-registering the same connection does not create a duplicate")
    void reRegisterSameConnectionIsIdempotent() {
        registry.register(ALICE, "c1");
        registry.register(ALICE, "c1");
        assertEquals(1, registry.streamSessionsByClientId("A").count());
        assertEquals(1, registry.countAllClients());
    }

    @Test
    @DisplayName("an unknown client is reported as not connected")
    void unknownClientNotConnected() {
        assertFalse(registry.isClientConnected("ghost"));
        assertEquals(0, registry.streamSessionsByClientId("ghost").count());
    }
}
