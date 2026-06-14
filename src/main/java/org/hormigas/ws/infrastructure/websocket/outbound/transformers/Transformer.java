package org.hormigas.ws.infrastructure.websocket.outbound.transformers;

import org.hormigas.ws.domain.session.ClientSession;

public interface Transformer<T, M> {
    T apply(T m, ClientSession<M> w);
}
