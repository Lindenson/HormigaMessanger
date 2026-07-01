package org.hormigas.ws.scheduler;

import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.websockets.next.WebSocketConnection;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.SessionConfig;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.session.ClientSession;
import org.hormigas.ws.ports.session.SessionRegistry;
import org.hormigas.ws.ports.tetris.TetrisMarker;

import java.util.List;

/**
 * Session reapers (prod).
 * <ul>
 *   <li><b>Liveness</b> (FR-PRES-03): the server auto-pings; a live client replies pong → refreshes
 *       {@code lastActiveAt}. A connection that stops responding is force-closed → {@code @OnClose →
 *       leave} → honest presence, GC unblocked (no phantom-online clients).</li>
 *   <li><b>Overload</b> (T2): a client whose un-ACKed backlog exceeds {@code max-pending} is
 *       force-disconnected. {@code onDisconnect} wipes its pending set and frees the GC watermark; the
 *       client recovers via REST history-sync on reconnect. This caps the per-recipient pending ZSET
 *       so a connected non-ACKing client can't grow it (or the outbox) without bound.</li>
 * </ul>
 */
@Slf4j
@ApplicationScoped
@IfBuildProfile("prod")
public class SessionLivenessScheduler {

    @Inject
    SessionRegistry<WebSocketConnection> registry;

    @Inject
    TetrisMarker<Message> tetrisMarker;

    @Inject
    SessionConfig config;

    // Resolved from config at startup (kept as fields so tests can set them directly).
    long idleTimeoutMs;
    int maxPending;
    int overloadKickBatch;

    @PostConstruct
    void init() {
        idleTimeoutMs = config.idleTimeoutMs();
        maxPending = config.maxPending();
        overloadKickBatch = config.overloadKickBatch();
    }

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

    @Scheduled(every = "${processing.session.overload-reaper-every:20s}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void reapOverloaded() {
        tetrisMarker.findHeavyClients(maxPending, overloadKickBatch)
                .subscribe().with(
                        heavy -> heavy.forEach(this::forceOffline),
                        err -> log.error("Overload reaper failed", err));
    }

    /** Force-disconnect every session of an overloaded client → onDisconnect wipes its pending state. */
    private void forceOffline(String clientId) {
        List<WebSocketConnection> conns = registry.streamSessionsByClientId(clientId)
                .map(ClientSession::getSession)
                .filter(WebSocketConnection::isOpen)
                .toList();
        for (WebSocketConnection conn : conns) {
            log.warn("Reaping overloaded client {} (> {} un-ACKed pending) — connection {}",
                    clientId, maxPending, conn.id());
            conn.close().subscribe().with(
                    x -> {},
                    e -> log.debug("Error closing overloaded connection {}: {}", conn.id(), e.getMessage()));
        }
    }
}
