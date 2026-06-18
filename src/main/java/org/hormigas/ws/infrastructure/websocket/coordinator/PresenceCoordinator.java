package org.hormigas.ws.infrastructure.websocket.coordinator;

import io.quarkus.websockets.next.WebSocketConnection;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.presence.AsyncPresence;
import org.hormigas.ws.core.watermark.AsyncWatermark;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.domain.session.ClientSession;
import org.hormigas.ws.ports.notifier.Coordinator;
import org.hormigas.ws.ports.session.SessionRegistry;

import java.util.List;
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
            evictPriorSessions(newClient.id(), connection);
            registry.register(newClient, connection);
            presence.add(newClient.id(), newClient.name(), System.currentTimeMillis());
            publisher.publishInit(newClient, registry);
            publisher.publishJoin(newClient);
            log.debug("Presence INIT coordinated for {}", newClient.id());
        } catch (Exception e) {
            log.error("Failed to coordinate INIT presence for {}", newClient.id(), e);
        }
    }

    /**
     * Single active session per user: when a new connection arrives for a client, evict any prior
     * ones. We deregister each old connection FIRST so its later {@code @OnClose → leave} is a no-op
     * (deregister returns null) — that keeps the takeover clean: the new session's presence and the
     * client's GC watermark are NOT torn down by the old socket closing.
     */
    private void evictPriorSessions(String clientId, WebSocketConnection keep) {
        List<WebSocketConnection> stale = registry.streamSessionsByClientId(clientId)
                .map(ClientSession::getSession)
                .filter(c -> !c.equals(keep))
                .toList();
        for (WebSocketConnection old : stale) {
            registry.deregister(old);
            old.close().subscribe().with(
                    x -> log.debug("Evicted prior session for {}", clientId),
                    e -> log.debug("Error closing prior session for {}: {}", clientId, e.getMessage()));
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
