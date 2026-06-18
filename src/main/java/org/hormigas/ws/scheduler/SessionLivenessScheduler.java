package org.hormigas.ws.scheduler;

import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.websockets.next.WebSocketConnection;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.hormigas.ws.domain.session.ClientSession;
import org.hormigas.ws.ports.session.SessionRegistry;

import java.util.List;

/**
 * Heartbeat liveness reaper (FR-PRES-03). The server auto-pings; a live client replies pong, which
 * refreshes {@code lastActiveAt}. A connection that stops responding goes stale and is force-closed
 * here — closing fires {@code @OnClose → leave}, which makes presence honest and unblocks GC
 * (no phantom-online clients).
 */
@Slf4j
@ApplicationScoped
@IfBuildProfile("prod")
public class SessionLivenessScheduler {

    @Inject
    SessionRegistry<WebSocketConnection> registry;

    @ConfigProperty(name = "processing.session.idle-timeout-ms", defaultValue = "35000")
    long idleTimeoutMs;

    @Scheduled(every = "${processing.session.reaper-every:15s}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void reap() {
        long cutoff = System.currentTimeMillis() - idleTimeoutMs;
        List<WebSocketConnection> stale = registry.streamAllOnlineClients()
                .filter(s -> s.getLastActiveAt() < cutoff)
                .map(ClientSession::getSession)
                .filter(WebSocketConnection::isOpen)
                .toList();

        for (WebSocketConnection conn : stale) {
            log.warn("Reaping stale WS connection {} (idle > {} ms)", conn.id(), idleTimeoutMs);
            conn.close().subscribe().with(
                    x -> {},
                    e -> log.debug("Error closing stale connection {}: {}", conn.id(), e.getMessage()));
        }
    }
}
