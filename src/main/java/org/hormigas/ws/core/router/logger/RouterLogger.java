package org.hormigas.ws.core.router.logger;

import org.hormigas.ws.core.router.context.RouterContext;

public interface RouterLogger<T> {
    void logRoutingStart(T message, Object pipelineType);
    void logRoutingResult(RouterContext<T> ctx);
}
