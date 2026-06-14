package org.hormigas.ws.infrastructure.persistance.postgres.adapters;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.HistoryRow;
import org.hormigas.ws.infrastructure.persistance.postgres.history.HistoryPostgresRepository;
import org.hormigas.ws.infrastructure.persistance.postgres.mappers.MessageMapper;
import org.hormigas.ws.ports.history.History;

import java.util.List;


/**
 * Adapter layer between immutable domain {@link Message} and {@link HistoryPostgresRepository}.
 *
 * <p>
 * This class provides a reactive, fault-tolerant bridge between domain messages and the
 * underlying PostgreSQL message history repository. It maps messages to and from database
 * DTOs ({@link HistoryRow}) via {@link MessageMapper} and ensures safe handling of invalid
 * data or repository errors.
 * </p>
 *
 * <p><b>Key responsibilities and behavior:</b></p>
 * <ul>
 *     <li>Maps between domain {@link Message} and {@link HistoryRow} for persistence and retrieval.</li>
 *     <li>Filters out invalid or incomplete messages:
 *         <ul>
 *             <li>Null messages or messages with null payloads are ignored during fetch.</li>
 *             <li>Messages with missing critical fields are not inserted into the history.</li>
 *         </ul>
 *     </li>
 *     <li>Handles repository failures gracefully:
 *         <ul>
 *             <li>Fetch methods return an empty list on failure rather than propagating exceptions.</li>
 *             <li>Insertions are fire-and-forget, with errors logged but not propagated.</li>
 *         </ul>
 *     </li>
 *     <li>All fetch methods return {@link Uni} reactive streams to allow safe, non-blocking consumption.</li>
 * </ul>
 *
 * <p><b>Minimal validation rules:</b></p>
 * <ul>
 *     <li>Messages must have non-null messageId, conversationId, senderId, recipientId, payload, and timestamp for insertion.</li>
 *     <li>Fetched messages with null payload are excluded from the returned list.</li>
 * </ul>
 *
 * <p><b>Important notes and potential risks:</b></p>
 * <ul>
 *     <li>This adapter does not throw exceptions for repository failures; it always returns safe defaults.</li>
 *     <li>Concurrent insertions are supported by the underlying repository, which manages batching and transactions.</li>
 *     <li>Domain messages are immutable; this adapter never modifies existing message data.</li>
 *     <li>All reactive methods handle failures explicitly with {@code onFailure().recoverWithItem(...)}.</li>
 * </ul>
 *
 * <p>
 * Overall, this adapter ensures safe, fault-tolerant access to message history for reactive
 * downstream consumers, providing consistent behavior even in the presence of invalid data
 * or repository errors.
 * </p>
 */
@Slf4j
@ApplicationScoped
@IfBuildProperty(name = "processing.messages.storage.service", stringValue = "redis")
public class HistoryPostgresAdapter implements History<Message> {

    @Inject
    HistoryPostgresRepository repository;

    @Inject
    MessageMapper mapper;

    // ----------------------------------------------------------------------
    // Fetch messages by recipient, map to domain, filter nulls, handle failures
    // ----------------------------------------------------------------------
    @Override
    public Uni<List<Message>> getByRecipientId(String clientId) {
        if (clientId == null) return Uni.createFrom().item(List.of());

        return repository.findAllByRecipientId(clientId)
                .onItem().transform(rows -> rows.stream()
                        .map(mapper::fromHistoryRow)
                        .filter(m -> m != null && m.getPayload() != null)
                        .toList())
                .onFailure().recoverWithItem(er -> {
                    log.error("Error fetching history for recipientId {}: {}", clientId, er.getMessage(), er);
                    return List.of();
                });
    }

    // ----------------------------------------------------------------------
    // Fetch messages by sender, map to domain, filter nulls, handle failures
    // ----------------------------------------------------------------------
    @Override
    public Uni<List<Message>> getBySenderId(String clientId) {
        if (clientId == null) return Uni.createFrom().item(List.of());

        return repository.findAllBySenderId(clientId)
                .onItem().transform(rows -> rows.stream()
                        .map(mapper::fromHistoryRow)
                        .filter(m -> m != null && m.getPayload() != null)
                        .toList())
                .onFailure().recoverWithItem(er -> {
                    log.error("Error fetching history for senderId {}: {}", clientId, er.getMessage(), er);
                    return List.of();
                });
    }

    // ----------------------------------------------------------------------
    // Fetch messages by participant (sender OR recipient), map to domain, filter nulls, handle failures
    // ----------------------------------------------------------------------
    @Override
    public Uni<List<Message>> getAllBySenderId(String clientId) {
        if (clientId == null) return Uni.createFrom().item(List.of());

        return repository.findAllByParticipantId(clientId)
                .onItem().transform(rows -> rows.stream()
                        .map(mapper::fromHistoryRow)
                        .filter(m -> m != null && m.getPayload() != null)
                        .toList())
                .onFailure().recoverWithItem(er -> {
                    log.error("Error fetching history for participantId {}: {}", clientId, er.getMessage(), er);
                    return List.of();
                });
    }

    // ----------------------------------------------------------------------
    // Insert a single message into history (fire-and-forget)
    // ----------------------------------------------------------------------
    @Override
    public void addBySenderId(String clientId, Message message) {
        if (clientId == null || message == null) {
            log.warn("Cannot add history: clientId or message is null");
            return;
        }

        HistoryRow row = mapper.toHistoryRow(message);
        if (!isValid(row)) {
            log.warn("Invalid HistoryRow, skipping insert: {}", row);
            return;
        }

        repository.insertHistoryBatch(List.of(row))
                .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                .subscribe().with(
                        inserted -> log.debug("Inserted history rows: {}", inserted),
                        failure -> log.error("Failed to insert history row: {}", row, failure)
                );
    }

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------
    private boolean isValid(HistoryRow row) {
        return row != null
                && row.messageId() != null
                && row.conversationId() != null
                && row.senderId() != null
                && row.recipientId() != null
                && row.payloadJson() != null
                && row.createdAt() != null;
    }
}
