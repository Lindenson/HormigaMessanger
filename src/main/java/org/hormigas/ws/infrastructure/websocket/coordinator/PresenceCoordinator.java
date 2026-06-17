package org.hormigas.ws.infrastructure.websocket.coordinator;

import io.quarkus.websockets.next.WebSocketConnection;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.presence.AsyncPresence;
import org.hormigas.ws.core.watermark.AsyncWatermark;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.ports.notifier.Coordinator;
import org.hormigas.ws.ports.session.SessionRegistry;

import java.util.Optional;


@Slf4j
@ApplicationScoped
public class PresenceCoordinator implements Coordinator<WebSocketConnection> {

    @Inject
    PresencePublisher publisher;

    @Inject
    AsyncPresence presence;

    @Inject
    SessionRegistry<WebSocketConnection> registry;

    @Inject
    AsyncWatermark watermark;

    @Override
    public void join(ClientData newClient, WebSocketConnection connection) {
        try {
            registry.register(newClient, connection);
            presence.add(newClient.id(), newClient.name(), System.currentTimeMillis());
            publisher.publishInit(newClient, registry);
            publisher.publishJoin(newClient);
            log.debug("Presence INIT coordinated for {}", newClient.id());
        } catch (Exception e) {
            log.error("Failed to coordinate INIT presence for {}", newClient.id(), e);
        }
    }

    @Override
    public void leave(WebSocketConnection connection) {
        try {
            Optional.ofNullable(registry.deregister(connection))
                    .map(s -> new ClientData(s.getClientId(), s.getClientName(), null, null))
                    .ifPresent(client -> {
                        watermark.remove(client.id());
                        publisher.publishLeave(client);
                        long timestamp = System.currentTimeMillis();
                        presence.remove(client.id(), timestamp);
                        log.debug("Presence LEAVE coordinated for {}", client.id());
                    });
        } catch (Exception e) {
            log.error("Failed to coordinate LEAVE presence for {}", connection, e);
        }
    }

    @Override
    public void active(WebSocketConnection connection) {
        var session = registry.getSessionByConnection(connection);
        if (session != null) {
            session.updateActivity();
            session.updateSequence();
        }
    }

    @Override
    public void passive(String clientId) {
        log.debug("Absence coordinated for {}", clientId);
        long timestamp = System.currentTimeMillis();
        presence.remove(clientId, timestamp);
    }
}
