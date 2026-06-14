package org.hormigas.ws.ports.session;

import org.hormigas.ws.domain.session.ClientSession;
import org.hormigas.ws.domain.credentials.ClientData;

import java.util.List;
import java.util.stream.Stream;

public interface SessionRegistry<T> {
    ClientSession<T>  deregister(T connection);
    void register(ClientData clientData, T connection);
    Stream<ClientSession<T>> streamSessionsByClientId(String id);
    Stream<ClientSession<T>> streamAllOnlineClients();
    ClientSession<T> getSessionByConnection(T connection);
    long countAllClients();
    List<ClientData> getAllOnlineClients();
    boolean isClientConnected(String clientId);
    void touch(ClientSession<T> clientSession);
    boolean cleanUnused(T connection);
}
