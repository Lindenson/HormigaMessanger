package org.hormigas.ws.scheduler;

import io.quarkus.websockets.next.WebSocketConnection;
import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.session.ClientSession;
import org.hormigas.ws.ports.session.SessionRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.mockito.Mockito.*;

@DisplayName("SessionLivenessScheduler — reaps connections idle past the timeout")
@SuppressWarnings("unchecked")
class SessionLivenessSchedulerTest {

    private final SessionRegistry<WebSocketConnection> registry = mock(SessionRegistry.class);

    private SessionLivenessScheduler scheduler() {
        SessionLivenessScheduler s = new SessionLivenessScheduler();
        s.registry = registry;
        s.idleTimeoutMs = 35_000;
        return s;
    }

    private ClientSession<WebSocketConnection> session(WebSocketConnection conn, long lastActiveAt) {
        return ClientSession.<WebSocketConnection>builder()
                .clientId("c").session(conn).lastActiveAt(lastActiveAt).build();
    }

    @Test
    @DisplayName("a stale, open connection is force-closed; a fresh one is left alone")
    void closesStaleKeepsFresh() {
        WebSocketConnection stale = mock(WebSocketConnection.class);
        WebSocketConnection fresh = mock(WebSocketConnection.class);
        when(stale.isOpen()).thenReturn(true);
        when(stale.close()).thenReturn(Uni.createFrom().voidItem());
        when(fresh.isOpen()).thenReturn(true);

        long now = System.currentTimeMillis();
        when(registry.streamAllOnlineClients()).thenReturn(Stream.of(
                session(stale, now - 1_000_000),  // long idle
                session(fresh, now)));             // active

        scheduler().reap();

        verify(stale).close();
        verify(fresh, never()).close();
    }

    @Test
    @DisplayName("an already-closed stale connection is not closed again")
    void skipsAlreadyClosed() {
        WebSocketConnection dead = mock(WebSocketConnection.class);
        when(dead.isOpen()).thenReturn(false);
        when(registry.streamAllOnlineClients())
                .thenReturn(Stream.of(session(dead, System.currentTimeMillis() - 1_000_000)));

        scheduler().reap();

        verify(dead, never()).close();
    }
}
