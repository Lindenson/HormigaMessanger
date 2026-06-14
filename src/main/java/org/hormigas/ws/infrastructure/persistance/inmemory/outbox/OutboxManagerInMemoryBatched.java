package org.hormigas.ws.infrastructure.persistance.inmemory.outbox;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.ports.history.History;
import org.hormigas.ws.ports.outbox.OutboxManager;

import java.util.List;

@Slf4j
@ApplicationScoped
@IfBuildProperty(name = "processing.messages.storage.service", stringValue = "memory")
public class OutboxManagerInMemoryBatched implements OutboxManager<Message> {

    @Inject
    History<Message> messageHistory;

    @PostConstruct
    public void close() {
        if (batchBuffer != null) {
            batchBuffer.shutdown();
        }
    }

    OutboxManagerInMemory delegate = new OutboxManagerInMemory();
    OutboxBatchBuffer batchBuffer = new OutboxBatchBuffer(delegate);

    @Override
    public Uni<StageResult<Message>> save(Message message) {
        messageHistory.addBySenderId(message.getRecipientId(), message);
        return delegate.save(message);
    }

    @Override
    public Uni<StageResult<Message>> remove(Message message) {
        return Uni.createFrom().item(batchBuffer.add(message));
    }

    @Override
    public Uni<Message> fetch() {
        return delegate.fetch();
    }

    @Override
    public Uni<List<Message>> fetchBatch(int batchSize) {
        return delegate.fetchBatch(batchSize);
    }

    @Override
    public Uni<Integer> collectGarbage(Long from) {
        return delegate.collectGarbage(from);
    }
}
