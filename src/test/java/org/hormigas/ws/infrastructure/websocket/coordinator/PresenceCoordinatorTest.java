package org.hormigas.ws.infrastructure.websocket.coordinator;

import io.quarkus.websockets.next.WebSocketConnection;
import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.presence.AsyncPresence;
import org.hormigas.ws.core.watermark.AsyncWatermark;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.domain.session.ClientSession;
import org.hormigas.ws.ports.session.SessionRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("PresenceCoordinator — join/leave, single-active-session eviction")
@SuppressWarnings("unchecked")
class PresenceCoordinatorTest {

    private final PresencePublisher publisher = mock(PresencePublisher.class);
    private final AsyncPresence presence = mock(AsyncPresence.class);
    private final SessionRegistry<WebSocketConnection> registry = mock(SessionRegistry.class);
    private final AsyncWatermark watermark = mock(AsyncWatermark.class);
    private PresenceCoordinator coord;

    private static final ClientData ALICE = new ClientData("A", "Alice", "CLIENT", "a@x");

    @BeforeEach
    void setUp() {
        coord = new PresenceCoordinator();
        coord.publisher = publisher;
        coord.presence = presence;
        coord.registry = registry;
        coord.watermark = watermark;
    }

    @Test
    @DisplayName("join registers the connection and announces presence")
    void joinRegistersAndAnnounces() {
        when(registry.streamSessionsByClientId("A")).thenReturn(Stream.empty());
        WebSocketConnection conn = mock(WebSocketConnection.class);

        coord.join(ALICE, conn);

        verify(registry).register(ALICE, conn);
        verify(presence).add(eq("A"), eq("Alice"), anyLong());
        verify(publisher).publishInit(ALICE, registry);
        verify(publisher).publishJoin(ALICE);
    }

    @Test
    @DisplayName("join evicts a prior session for the same client (single-active-session)")
    void joinEvictsPriorSession() {
        WebSocketConnection oldConn = mock(WebSocketConnection.class);
        when(oldConn.close()).thenReturn(Uni.createFrom().voidItem());
        ClientSession<WebSocketConnection> old = ClientSession.<WebSocketConnection>builder()
                .clientId("A").clientName("Alice").session(oldConn).build();
        when(registry.streamSessionsByClientId("A")).thenReturn(Stream.of(old));
        WebSocketConnection newConn = mock(WebSocketConnection.class);

        coord.join(ALICE, newConn);

        verify(registry).deregister(oldConn);
        verify(oldConn).close();
        verify(registry).register(ALICE, newConn);
    }

    @Test
    @DisplayName("leave cleans presence and watermark for the disconnected client")
    void leaveCleansUp() {
        WebSocketConnection conn = mock(WebSocketConnection.class);
        ClientSession<WebSocketConnection> s = ClientSession.<WebSocketConnection>builder()
                .clientId("A").clientName("Alice").session(conn).build();
        when(registry.deregister(conn)).thenReturn(s);

        coord.leave(conn);

        verify(watermark).remove("A");
        verify(presence).remove(eq("A"), anyLong());
        verify(publisher).publishLeave(any());
    }

    @Test
    @DisplayName("leave on an already-deregistered connection is a no-op (idempotent takeover)")
    void leaveOnUnknownIsNoop() {
        WebSocketConnection conn = mock(WebSocketConnection.class);
        when(registry.deregister(conn)).thenReturn(null);

        coord.leave(conn);

        verify(watermark, never()).remove(anyString());
        verify(presence, never()).remove(anyString(), anyLong());
    }
}
