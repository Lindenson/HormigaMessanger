package org.hormigas.ws.core.router;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.message.MessageEnvelope;

public interface InboundRouter<T> {
    Uni<MessageEnvelope<T>> routeIn(T message);
}
