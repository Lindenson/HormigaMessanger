package org.hormigas.ws.ports.presence;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.presence.OnlineClient;

import java.util.List;
public interface PresenceManager {
    Uni<Void> add(String id, String name, long timestamp);
    Uni<Void> remove(String id, long timestamp);
    Uni<List<OnlineClient>> getAll();
}
