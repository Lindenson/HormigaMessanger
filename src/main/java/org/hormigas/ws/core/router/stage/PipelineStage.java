package org.hormigas.ws.core.router.stage;

import io.smallrye.mutiny.Uni;

public interface PipelineStage<T> {
    Uni<T> apply(T ctx);
}
