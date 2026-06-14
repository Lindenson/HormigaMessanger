package org.hormigas.ws.infrastructure.persistance.postgres;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.HistoryRow;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.Inserted;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxMessage;
import org.hormigas.ws.infrastructure.persistance.postgres.dto.OutboxRow;

import java.time.Duration;
import java.util.List;


/**
 * Repository implementation for managing messages in the Outbox and Message History tables
 * using PostgreSQL. This class handles batched inserts, transactional history+outbox inserts,
 * fetching messages for processing with optimistic leasing, and deletion operations.
 *
 * <p><b>Data assumptions and requirements:</b></p>
 * <ul>
 *     <li>All {@link OutboxMessage} and {@link HistoryRow} objects must be non-null and
 *         contain valid, immutable message data before being passed to repository methods.</li>
 *     <li>Message IDs, conversation IDs, sender/recipient IDs, timestamps, and payload JSON
 *         must follow application constraints and be consistent with the domain model.</li>
 *     <li>Messages are considered immutable after construction; the repository does not perform
 *         any field modifications except for outbox control fields (server_ts, lease_until,
 *         processing_attempts, status).</li>
 *     <li>Null or incomplete messages may break SQL insert statements or transactional integrity,
 *         causing runtime exceptions.</li>
 *     <li>The repository relies on proper JSON serialization for payload and meta fields.
 *         Invalid JSON or null payload/meta may result in SQL errors.</li>
 * </ul>
 *
 * <p><b>Potential runtime risks:</b></p>
 * <ul>
 *     <li>Passing null lists to batch insert methods is handled and returns an empty result,
 *         but passing null elements inside the list will throw runtime exceptions.</li>
 *     <li>Passing messages with null mandatory fields (IDs, timestamps, payload) may corrupt
 *         inserts and lead to database constraint violations.</li>
 *     <li>Incorrect senderTimestamp or senderTimezone are not validated here; upstream validation
 *         should ensure correctness.</li>
 *     <li>Concurrent access is handled by Postgres row-level locking for fetchBatchForProcessing,
 *         but misuse of leaseDuration or batchSize may lead to empty results or high contention.</li>
 *     <li>Immutable objects: this class assumes that {@link OutboxMessage} and {@link HistoryRow}
 *         instances are not mutated during the insert/fetch lifecycle.</li>
 * </ul>
 *
 * <p><b>Method overview:</b></p>
 * <ul>
 *     <li>{@link #insertOutboxBatch(List)} – Inserts a batch of outbox messages into the database,
 *         returns their generated IDs.</li>
 *     <li>{@link #fetchBatchForProcessing(int, java.time.Duration)} – Retrieves a batch of pending
 *         messages for processing, marking them as leased and incrementing attempts.</li>
 *     <li>{@link #insertHistoryAndOutboxTransactional(List, List)} – Inserts history and outbox
 *         messages in a single transaction, ensuring atomicity.</li>
 *     <li>{@link #deleteProcessedByIds(List)} – Deletes outbox messages that have already been processed
 *         based on their message IDs.</li>
 *     <li>{@link #deleteOlderThan(long)} – Deletes outbox messages older than the given ID threshold.</li>
 * </ul>
 *
 * <p><b>Validation guidance:</b></p>
 * <ul>
 *     <li>This repository performs minimal validation; it assumes upstream services or validators
 *         have ensured message correctness.</li>
 *     <li>It is strongly recommended to validate messages for null, blank IDs, payload consistency,
 *         and correct timestamps before invoking repository methods.</li>
 * </ul>
 *
 * <p>In summary, this class is a thin persistence layer with a strong expectation that messages
 * passed in are valid and immutable. Any nulls or malformed data may break transactional operations
 * or violate database constraints.</p>
 */
public interface OutboxRepository {
    Uni<List<Inserted>> insertOutboxBatch(List<OutboxMessage> batch);
    Uni<List<OutboxRow>> fetchBatchForProcessing(int batchSize, Duration leaseDuration);
    Uni<Integer> deleteProcessedByIds(List<String> ids);
    Uni<List<Inserted>> insertHistoryAndOutboxTransactional(List<HistoryRow> historyRows, List<OutboxMessage> outboxBatch);
    Uni<Integer> deleteOlderThan(long idThreshold);
}
