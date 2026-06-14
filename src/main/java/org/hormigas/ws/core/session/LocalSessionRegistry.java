package org.hormigas.ws.core.session;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.core.credits.lazy.LazyCreditsBuket;
import org.hormigas.ws.core.session.tidy.CleanupStrategy;
import org.hormigas.ws.domain.credentials.ClientData;
import org.hormigas.ws.domain.session.ClientSession;
import org.hormigas.ws.ports.session.SessionRegistry;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class LocalSessionRegistry<T> implements SessionRegistry<T> {

    private final MessengerConfig messengerConfig;
    private final MeterRegistry meterRegistry;
    private final CleanupStrategy<T> cleanupStrategy;

    private final ConcurrentMap<T, ClientSession<T>> connectionIndex = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<T>> clientIndex = new ConcurrentHashMap<>();

    public LocalSessionRegistry(MessengerConfig messengerConfig,
                                MeterRegistry meterRegistry,
                                CleanupStrategy<T> cleanupStrategy) {

        this.messengerConfig = messengerConfig;
        this.meterRegistry = meterRegistry;
        this.cleanupStrategy = cleanupStrategy;

        Gauge.builder("websocket_clients_registered", this, LocalSessionRegistry::size)
                .description("Number of currently active WebSocket client connections")
                .register(this.meterRegistry);
    }

    @Override
    public void register(@Nonnull ClientData clientData, @Nonnull T connection) {
        Objects.requireNonNull(clientData.id(), "Client id must not be null");
        Objects.requireNonNull(clientData.name(), "Client name must not be null");

        deregister(connection);

        var clientSession = ClientSession.<T>builder()
                .clientId(clientData.id())
                .clientName(clientData.name())
                .credits(new LazyCreditsBuket(messengerConfig.credits().maxValue(),
                        messengerConfig.credits().refillRatePerS()))
                .session(connection)
                .build();

        connectionIndex.put(connection, clientSession);
        clientIndex.computeIfAbsent(clientData.id(), k -> ConcurrentHashMap.newKeySet()).add(connection);

        log.debug("Client connected: {} ({})", clientData.name(), clientData.id());
    }

    @Override
    @Nullable
    public ClientSession<T> deregister(@Nonnull T connection) {
        var removed = connectionIndex.remove(connection);
        if (removed == null) return null;

        clientIndex.computeIfPresent(removed.getClientId(), (id, connections) -> {
            connections.remove(connection);
            return connections.isEmpty() ? null : connections;
        });

        log.debug("Client disconnected: {}", removed.getClientId());
        return removed;
    }

    @Override
    @Nonnull
    public Stream<ClientSession<T>> streamSessionsByClientId(@Nonnull String id) {
        var connections = clientIndex.get(id);
        if (connections == null || connections.isEmpty()) return Stream.empty();
        return connections.stream()
                .map(connectionIndex::get)
                .filter(Objects::nonNull);
    }

    @Override
    @Nonnull
    public Stream<ClientSession<T>> streamAllOnlineClients() {
        return connectionIndex.values().stream();
    }

    @Override
    @Nullable
    public ClientSession<T> getSessionByConnection(@Nonnull T connection) {
        ClientSession<T> clientSession = connectionIndex.get(connection);
        if (clientSession != null) touch(clientSession);
        return clientSession;
    }

    @Override
    public long countAllClients() {
        return connectionIndex.size();
    }

    @Override
    @Nonnull
    public List<ClientData> getAllOnlineClients() {
        return connectionIndex.values().stream()
                .collect(Collectors.toMap(ClientSession::getClientId, s ->
                        s, (a, b) -> a))
                .values().stream()
                .map(s -> new ClientData(s.getClientId(), s.getClientName()))
                .toList();
    }

    @Override
    public boolean isClientConnected(@Nonnull String clientId) {
        var set = clientIndex.get(clientId);
        return set != null && !set.isEmpty();
    }

    @Override
    public void touch(@Nonnull ClientSession<T> clientSession) {
        clientSession.updateActivity();
        log.debug("Touch connection {} prolonged", clientSession);
    }

    @Override
    public boolean cleanUnused(T connection) {
        return cleanupStrategy.clean(
                connection,
                connectionIndex.keySet(),
                this::deregister
        );
    }

    private int size() {
        return connectionIndex.size();
    }
}
