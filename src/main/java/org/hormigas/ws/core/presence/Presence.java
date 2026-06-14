package org.hormigas.ws.core.presence;

import io.smallrye.mutiny.Uni;

public interface Presence {
    Uni<Void> add(String userId, String name, long timestamp);
    Uni<Void> remove(String userId, long timestamp);
}
