package org.hormigas.ws.infrastructure.websocket.sessions;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.websockets.next.WebSocketConnection;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.core.session.LocalSessionRegistry;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.domain.session.ClientSession;
import org.hormigas.ws.core.session.tidy.CleanupStrategy;
import org.hormigas.ws.core.session.tidy.FractionalCleanupStrategy;
import org.hormigas.ws.ports.session.SessionRegistry;

import java.util.List;
import java.util.stream.Stream;

@ApplicationScoped
public class WebSocketSessionRegistry implements SessionRegistry<WebSocketConnection> {

    @Inject
    MessengerConfig messengerConfig;

    @Inject
    MeterRegistry meterRegistry;

    CleanupStrategy<WebSocketConnection> cleanupStrategy = new FractionalCleanupStrategy<>(
            WebSocketConnection::getOpenConnections,
            WebSocketConnection::isOpen
    );

    private LocalSessionRegistry<WebSocketConnection> delegate;

    @PostConstruct
    void init() {
        delegate = new LocalSessionRegistry<>(messengerConfig, meterRegistry, cleanupStrategy);
    }

    @Override
    public ClientSession<WebSocketConnection> deregister(WebSocketConnection connection) {
        return delegate.deregister(connection);
    }

    @Override
    public void register(ClientData clientData, WebSocketConnection connection) {
        delegate.register(clientData, connection);
    }

    @Override
    public Stream<ClientSession<WebSocketConnection>> streamSessionsByClientId(String id) {
        return delegate.streamSessionsByClientId(id);
    }

    @Override
    public Stream<ClientSession<WebSocketConnection>> streamAllOnlineClients() {
        return delegate.streamAllOnlineClients();
    }

    @Override
    public ClientSession<WebSocketConnection> getSessionByConnection(WebSocketConnection connection) {
        return delegate.getSessionByConnection(connection);
    }

    @Override
    public long countAllClients() {
        return delegate.countAllClients();
    }

    @Override
    public List<ClientData> getAllOnlineClients() {
        return delegate.getAllOnlineClients();
    }

    @Override
    public boolean isClientConnected(String clientId) {
        return delegate.isClientConnected(clientId);
    }

    @Override
    public void touch(ClientSession<WebSocketConnection> clientSession) {
        delegate.touch(clientSession);
    }

    @Override
    public boolean cleanUnused(WebSocketConnection connection) {
        return delegate.cleanUnused(connection);
    }
}
