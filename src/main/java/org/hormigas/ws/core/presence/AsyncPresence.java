package org.hormigas.ws.core.presence;

public interface AsyncPresence {
    void add(String userId, String name, long timestamp);
    void remove(String userId, long timestamp);
}
