package org.hormigas.ws.infrastructure.cache.inmemory;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.idempotency.IdempotencyManager;


@Slf4j
@ApplicationScoped
@IfBuildProperty(name = "processing.messages.storage.service", stringValue = "memory")
public class IdempotencyManagerInMemory implements IdempotencyManager<Message> {

    private final int MAX_IDEM_SIZE = 5000;

    private enum Status {SENT, ACK}

    private final ConcurrentInsertionOrderMap<String, Status> messages =
            new ConcurrentInsertionOrderMap<>(MAX_IDEM_SIZE, e -> e == Status.ACK);


    @Override
    public Uni<StageResult<Message>> add(@Nullable Message message) {
        if (message == null || message.getMessageId() == null) return Uni.createFrom().item(StageResult.failed());

        log.debug("Adding message {}", message);
        return Uni.createFrom().item(messages.put(message.getMessageId(), Status.SENT))
                .replaceWith(StageResult.passed());
    }

    @Override
    public Uni<StageResult<Message>> remove(@Nullable Message message) {
        if (message == null || message.getCorrelationId() == null) return Uni.createFrom().item(StageResult.failed());

        log.debug("Removing message {}", message);
        return Uni.createFrom().item(messages.replace(message.getCorrelationId(), Status.ACK))
                .replaceWith(StageResult.passed());
    }

    @Override
    public Uni<Boolean> isInProgress(@Nullable Message message) {
        if (message == null || message.getMessageId() == null) {
            log.error("Message is null or empty {}", message);
            return Uni.createFrom().item(Boolean.FALSE);
        }

        log.debug("Checking message {}", message);
        if (messages.size() > MAX_IDEM_SIZE + 1) log.warn("TOO MUCH IDEM SIZE: {}", messages.size());
        return Uni.createFrom().item(messages.get(message.getMessageId()) != null)
                .onItem().invoke(found -> {
                    if (found) log.debug("Double sent {}", message.getMessageId());
                });
    }
}
