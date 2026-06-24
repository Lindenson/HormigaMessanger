package org.hormigas.ws.ports.deadletter;

import io.smallrye.mutiny.Uni;

import java.util.List;

/**
 * Durable dead-letter record for Strategy C (ADR-014, eager-draft model). A draft is written when a
 * system notice is emitted; it is deleted (retracted) once delivery is confirmed. What remains as a
 * draft = the genuinely undelivered notices.
 */
public interface DeadLetterStore<T> {

    /** Insert a {@code DRAFT} record for a just-emitted system notice (the durable safety record). */
    Uni<Void> recordDraft(T message);

    /** Retract confirmed drafts by messageId; idempotent. Returns the number of rows removed. */
    Uni<Integer> deleteDrafts(List<String> messageIds);
}
