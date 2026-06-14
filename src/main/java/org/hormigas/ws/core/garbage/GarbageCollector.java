package org.hormigas.ws.core.garbage;

import io.smallrye.mutiny.Uni;

public interface GarbageCollector {
    Uni<Integer> collect();
}
