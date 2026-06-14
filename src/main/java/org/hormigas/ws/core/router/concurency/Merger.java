package org.hormigas.ws.core.router.concurency;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;

import java.util.List;

public interface Merger<T> {
    Uni<RouterContext<T>> runParallel(RouterContext<T> ctx,
                                      List<PipelineStage<RouterContext<T>>> stages);
}
