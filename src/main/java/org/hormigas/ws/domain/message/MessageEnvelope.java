package org.hormigas.ws.domain.message;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder
@Getter
@EqualsAndHashCode
public class MessageEnvelope<T> {
    private final T message;
    private final boolean processed;
}
