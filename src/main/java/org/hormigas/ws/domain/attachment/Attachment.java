package org.hormigas.ws.domain.attachment;

import java.time.Instant;

/**
 * An uploaded-file reference in a conversation (concept §10). The service stores the MinIO
 * {@code objectKey}, never a URL (ADR-010); presigned URLs are minted on demand at read time.
 * Lifecycle: {@code PENDING} (upload-url issued) → {@code CONFIRMED} (object present, chat message
 * emitted) ; an unconfirmed row is reclaimed to {@code ORPHANED}. Framework-free domain record.
 */
public record Attachment(
        String id,
        String conversationId,
        String uploaderId,
        String objectKey,
        String fileName,
        String contentType,
        Long sizeBytes,
        AttachmentStatus status,
        Instant createdAt,
        Instant confirmedAt
) {
    public enum AttachmentStatus { PENDING, CONFIRMED, ORPHANED }
}
