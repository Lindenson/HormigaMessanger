package org.hormigas.ws.core.router.context;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.With;
import org.hormigas.ws.core.router.PipelineResolver;
import org.hormigas.ws.domain.stage.StageResult;

@Data
@Builder
public class RouterContext<T> {
    @Builder.Default
    private StageResult<T> delivered = StageResult.unknown();
    @Builder.Default
    private StageResult<T> persisted = StageResult.unknown();
    @Builder.Default
    private StageResult<T> cached = StageResult.unknown();
    @Builder.Default
    private StageResult<T> acknowledged = StageResult.unknown();
    @Builder.Default
    private boolean done = false;

    private final PipelineResolver.PipelineType pipelineType;
    @With
    private Throwable error;

    @With
    @Getter
    private final T payload;

    public boolean hasError() {
        return error != null;
    }
}