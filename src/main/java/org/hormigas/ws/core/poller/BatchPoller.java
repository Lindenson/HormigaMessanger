package org.hormigas.ws.core.poller;

import io.smallrye.mutiny.Uni;

public interface BatchPoller {
    Uni<Void> poll();
}
