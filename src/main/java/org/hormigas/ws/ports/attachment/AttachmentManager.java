package org.hormigas.ws.ports.attachment;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.attachment.Attachment;

import java.time.Instant;
import java.util.List;

/** Driven port for attachment persistence (the PENDING→CONFIRMED→DELIVERED / ORPHANED lifecycle). */
public interface AttachmentManager {

    Uni<Void> insertPending(Attachment attachment);

    Uni<Attachment> findById(String id);

    /** Mark CONFIRMED (idempotent). Returns the updated row, or null if the id is unknown. */
    Uni<Attachment> markConfirmed(String id, Instant confirmedAt);

    /** Mark DELIVERED once the attachment message was emitted (CONFIRMED→DELIVERED, idempotent). */
    Uni<Void> markDelivered(String id);

    /** PENDING rows created before {@code cutoff} — the orphan-reclaim candidates. */
    Uni<List<Attachment>> findStalePending(Instant cutoff, int limit);

    /** Mark ORPHANED (terminal) after the MinIO object has been reclaimed. */
    Uni<Void> markOrphaned(String id);
}
