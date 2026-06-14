package org.hormigas.ws.core.watermark;

import io.smallrye.mutiny.Uni;

public interface Watermark {
    Uni<Void> remove(String userId);
}
