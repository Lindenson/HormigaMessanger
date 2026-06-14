package org.hormigas.ws.core.credits;

import org.hormigas.ws.domain.session.ClientSession;

public interface ChannelFilter<T, M>{
    boolean filter(T message, ClientSession<M> clientSession);
}
