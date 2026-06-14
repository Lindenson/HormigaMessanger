package org.hormigas.ws.core.router;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.message.MessageEnvelope;

public interface OutboundRouter<T> {
    Uni<MessageEnvelope<T>> routeOut(T message);
}
