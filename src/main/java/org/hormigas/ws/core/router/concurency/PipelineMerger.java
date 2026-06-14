package org.hormigas.ws.core.router.concurency;

import io.smallrye.mutiny.Uni;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.core.router.PipelineResolver;
import org.hormigas.ws.core.router.context.RouterContext;
import org.hormigas.ws.core.router.stage.PipelineStage;
import org.hormigas.ws.domain.stage.StageResult;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class PipelineMerger<T> implements Merger<T> {

    /**
     * Run multiple stages in parallel, recover from errors, and merge results into one context.
     */
    @Override
    public Uni<RouterContext<T>> runParallel(RouterContext<T> ctx,
                                             List<PipelineStage<RouterContext<T>>> stages) {

        List<Uni<RouterContext<T>>> safeStages = stages.stream()
                .map(stage -> stage.apply(ctx)
                        .onFailure().invoke(f -> log.error("Stage {} failed",
                                stage.getClass().getSimpleName(), f))
                        .onFailure().recoverWithItem(ctx)) // fire-and-forget safety
                .collect(Collectors.toList());

        return Uni.combine().all().unis(safeStages)
                .with(results -> {
                    RouterContext.RouterContextBuilder<T> builder = RouterContext.builder();
                    final T payload = ctx.getPayload();
                    final PipelineResolver.PipelineType pipelineType = ctx.getPipelineType();

                    StageResult<T> aggregatedCached = StageResult.unknown();
                    StageResult<T> aggregatedAck = StageResult.unknown();
                    StageResult<T> aggregatedDelivered = StageResult.unknown();
                    StageResult<T> aggregatedPersisted = StageResult.unknown();
                    List<Throwable> errors = new ArrayList<>();

                    for (Object obj : results) {
                        @SuppressWarnings("unchecked")
                        RouterContext<T> stageCtx = (RouterContext<T>) obj;

                        aggregatedCached = mergeStageResult(aggregatedCached, stageCtx.getCached());
                        aggregatedAck = mergeStageResult(aggregatedAck, stageCtx.getAcknowledged());
                        aggregatedDelivered = mergeStageResult(aggregatedDelivered, stageCtx.getDelivered());
                        aggregatedPersisted = mergeStageResult(aggregatedPersisted, stageCtx.getPersisted());

                        if (stageCtx.getError() != null) errors.add(stageCtx.getError());
                    }

                    builder.payload(payload)
                            .pipelineType(pipelineType)
                            .cached(aggregatedCached)
                            .acknowledged(aggregatedAck)
                            .delivered(aggregatedDelivered)
                            .persisted(aggregatedPersisted);

                    if (!errors.isEmpty()) {
                        // getting the first, to be improved later
                        builder.error(errors.get(0));
                    }

                    return builder.build();
                });
    }

    private StageResult<T> mergeStageResult(StageResult<T> a, StageResult<T> b) {
        if (a.isSuccess() && b.isSuccess()) return StageResult.passed();
        if (a.isFailed() || b.isFailed()) return StageResult.failed();
        return StageResult.unknown();
    }
}
