package org.hormigas.ws.infrastructure.persistance.inmemory.outbox;

import io.smallrye.mutiny.Uni;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.outbox.OutboxManager;

import java.util.List;
import java.util.function.Predicate;


@Slf4j
public class OutboxManagerInMemory implements OutboxManager<Message> {


    private final TimeOrderedStringKeyMap<Message> messages = new TimeOrderedStringKeyMap<>(Message::getSenderTimestamp);
    private final int MAX_OUTBOX_SIZE = 5000;


    @Override
    public Uni<StageResult<Message>> save(@Nullable Message message) {
        if (message == null || message.getMessageId() == null) return Uni.createFrom().item(StageResult.failed());

        if (messages.size() > MAX_OUTBOX_SIZE) log.warn("TOO MUCH OUTBOX SIZE: {}", messages.size());
        log.debug("Saving message {}", message);
        return Uni.createFrom().item(messages.putIfAbsent(message.getMessageId(), message))
                .onItem().transform(it -> it == null ? StageResult.passed() : StageResult.skipped());
    }

    @Override
    public Uni<StageResult<Message>> remove(@Nullable Message message) {
        if (message == null || message.getCorrelationId() == null) return Uni.createFrom().item(StageResult.failed());

        log.debug("Removing message {}", message);
        return Uni.createFrom().item(messages.remove(message.getCorrelationId()))
                .onItem().transform(it -> it != null ? StageResult.passed() : StageResult.skipped());
    }

    @Override
    public Uni<Message> fetch() {
        Message first = messages.peekFirst();
        return first != null
                ? Uni.createFrom().item(first)
                : Uni.createFrom().nothing();
    }


    @Override
    public Uni<List<Message>> fetchBatch(int batchSize) {
        List<Message> batch = messages.peekFirstN(batchSize);

        log.debug("Batched {} messages", batch.size());
        return batch.isEmpty()
                ? Uni.createFrom().nothing()
                : Uni.createFrom().item(batch);
    }


    @Override
    public Uni<Integer> collectGarbage(Long from) {
        return Uni.createFrom().item(() -> {
            int collected = messages.collectGarbageOptimized(it ->
                    it.getId().longValue() < from.longValue());
            log.debug("Garbage collected {}", collected);
            return collected;
        });
    }

    @Override
    public Uni<java.util.Map<String, List<Long>>> pendingByRecipient() {
        // In-memory outbox is not the durable rehydration source; recovery is a Postgres concern.
        return Uni.createFrom().item(java.util.Map.of());
    }
}
