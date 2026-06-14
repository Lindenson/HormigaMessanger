package org.hormigas.ws.ports.history;

import io.smallrye.mutiny.Uni;

import java.util.List;

public interface History<T> {
    Uni<List<T>> getByRecipientId(String clientId);
    Uni<List<T>> getBySenderId(String clientId);
    Uni<List<T>> getAllBySenderId(String clientId);
    void addBySenderId(String clientId, T message);
}
