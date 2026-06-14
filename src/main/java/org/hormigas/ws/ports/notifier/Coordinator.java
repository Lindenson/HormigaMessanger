package org.hormigas.ws.ports.notifier;

import org.hormigas.ws.domain.credentials.ClientData;

public interface Coordinator<T> {
    void join(ClientData newClient, T connection);
    void leave(T connection);
    void active(T connection);
    void passive(String clientId);
}
