package org.hormigas.ws.infrastructure.persistance.postgres.adapters;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.stage.StageResult;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.HistoryRow;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.Inserted;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxMessage;
import org.hormigas.ws.infrastructure.persistance.postgres.mappers.MessageMapper;
import org.hormigas.ws.infrastructure.persistance.postgres.outbox.OutboxPostgresRepository;
import org.hormigas.ws.ports.outbox.OutboxManager;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Adapter layer between immutable domain {@link Message} and {@link OutboxPostgresRepository}.
 *
 * <p>
 * This class provides a reactive bridge between domain messages and PostgreSQL outbox storage.
 * It maps immutable {@link Message} to DTOs ({@link OutboxMessage}, {@link HistoryRow}) via {@link MessageMapper}
 * and delegates persistence and retrieval operations to the repository.
 * </p>
 *
 * <p><b>Key behavior:</b></p>
 * <ul>
 *     <li>Validates minimal required fields before persistence.</li>
 *     <li>Filters out invalid messages on fetch (null payload, null domain object).</li>
 *     <li>All write operations return a {@link StageResult} indicating success, failure, or skipped.</li>
 *     <li>Reactive methods never throw exceptions; errors are mapped to {@link StageResult.StageStatus#FAILED} or empty results.</li>
 *     <li>Domain messages are immutable; adapter does not mutate them, but returns updated instances with DB-generated ids.</li>
 * </ul>
 */
@Slf4j
@ApplicationScoped
@IfBuildProperty(name = "processing.messages.storage.service", stringValue = "redis")
public class OutboxPostgresAdapter implements OutboxManager<Message> {

    @Inject
    OutboxPostgresRepository repo;

    @Inject
    MessageMapper mapper;

    // ----------------------------------------------------------------------
    // Save a message to outbox and history
    // ----------------------------------------------------------------------
    @Override
    public Uni<StageResult<Message>> save(Message message) {
        if (!isValidForSave(message)) {
            log.warn("Message failed minimal validation and will not be saved: {}", message);
            return Uni.createFrom().item(StageResult.failed());
        }

        OutboxMessage outboxMessage = mapper.toOutboxMessage(message);
        HistoryRow historyRow = mapper.toHistoryRow(message);

        if (outboxMessage == null || historyRow == null) {
            log.warn("Mapper returned null for message, cannot save: {}", message);
            return Uni.createFrom().item(StageResult.failed());
        }

        return repo.insertHistoryAndOutboxTransactional(
                        List.of(historyRow),
                        List.of(outboxMessage)
                )
                .onItem().transform(inserted -> toStageResult(message, inserted))
                .onFailure().recoverWithItem(er -> {
                    log.error("Error saving message: {}", message, er);
                    return StageResult.failed();
                });
    }

    // ----------------------------------------------------------------------
    // Remove a message by correlationId
    // ----------------------------------------------------------------------
    @Override
    public Uni<StageResult<Message>> remove(Message message) {
        if (message == null || message.getCorrelationId() == null) {
            log.warn("Cannot remove message; correlationId is null: {}", message);
            return Uni.createFrom().item(StageResult.failed());
        }

        return repo.deleteProcessedByIds(List.of(message.getCorrelationId()))
                .onItem().transform(rows -> StageResult.updated(message))
                .onFailure().recoverWithItem(er -> {
                    log.error("Error removing message: {}", message, er);
                    return StageResult.failed();
                });
    }

    // ----------------------------------------------------------------------
    // Fetch a single message from outbox
    // ----------------------------------------------------------------------
    @Override
    public Uni<Message> fetch() {
        return repo.fetchBatchForProcessing(1, Duration.ofSeconds(5))
                .onItem().transformToUni(batch -> {
                    if (batch.isEmpty()) return Uni.createFrom().nullItem();
                    return Uni.createFrom().item(mapper.toDomainMessage(batch.get(0)));
                })
                .onItem().transform(msg -> {
                    if (msg == null || msg.getPayload() == null) {
                        log.warn("Fetched invalid message: {}", msg);
                        return null;
                    }
                    return msg;
                })
                .onFailure().recoverWithItem(er -> {
                    log.error("Error fetching single message", er);
                    return null;
                });
    }

    // ----------------------------------------------------------------------
    // Fetch batch of messages from outbox
    // ----------------------------------------------------------------------
    @Override
    public Uni<List<Message>> fetchBatch(int batchSize) {
        return repo.fetchBatchForProcessing(batchSize, Duration.ofSeconds(5))
                .onItem().transform(batch ->
                        batch.stream()
                                .map(mapper::toDomainMessage)
                                .filter(m -> m != null && m.getPayload() != null)
                                .collect(Collectors.toList())
                )
                .onFailure().recoverWithItem(er -> {
                    log.error("Error fetching batch of messages", er);
                    return List.of();
                });
    }

    // ----------------------------------------------------------------------
    // Collect garbage (not implemented yet)
    // ----------------------------------------------------------------------
    @Override
    public Uni<Integer> collectGarbage(Long from) {
        if (from == null || from <= 0) return Uni.createFrom().item(0);
        return repo.deleteOlderThan(from)
                .onFailure().recoverWithItem(er -> {
                    log.error("Error collecting garbage", er);
                    return 0;
                });
    }

    // ----------------------------------------------------------------------
    // Minimal validation for save
    // ----------------------------------------------------------------------
    private boolean isValidForSave(Message msg) {
        return msg != null
                && msg.getMessageId() != null
                && msg.getSenderId() != null
                && msg.getRecipientId() != null
                && msg.getPayload() != null;
    }

    // ----------------------------------------------------------------------
    // Transform repository insertion result into StageResult
    // ----------------------------------------------------------------------
    private StageResult<Message> toStageResult(Message original, List<Inserted> inserted) {
        if (inserted == null || inserted.size() != 1) return StageResult.failed();
        Inserted updated = inserted.getFirst();
        if (!updated.messageId().equals(original.getMessageId())) return StageResult.failed();
        return StageResult.updated(original.withId(updated.id()));
    }
}
