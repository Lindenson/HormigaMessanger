package org.hormigas.ws.core.router;

public interface PipelineResolver<T, D> {
    enum PipelineType {
        INBOUND_PERSISTENT,
        INBOUND_CACHED,
        INBOUND_DIRECT,
        OUTBOUND_CACHED,
        OUTBOUND_DIRECT,
        ACK_PERSISTENT,
        ACK_CACHED,
        SKIP
    }
    PipelineType resolvePipeline(T message);
}
