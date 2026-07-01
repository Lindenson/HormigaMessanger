package org.hormigas.ws.core.attachment;

import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.AttachmentsConfig;
import org.hormigas.ws.config.MinioConfig;
import org.hormigas.ws.core.conversation.Conversations;
import org.hormigas.ws.domain.attachment.*;
import org.hormigas.ws.domain.attachment.Attachment.AttachmentStatus;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.ports.attachment.AttachmentManager;
import org.hormigas.ws.ports.emit.ChatMessageEmitter;
import org.hormigas.ws.ports.storage.ObjectStorage;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Two-phase presigned-upload attachments (concept §10, ADR-010), trigger-agnostic core op.
 * <ol>
 *   <li>{@link #requestUpload} — membership + size checks → persist a PENDING row → presigned PUT URL.</li>
 *   <li>client uploads bytes DIRECTLY to MinIO (never through the service).</li>
 *   <li>{@link #confirmUpload} — verify the object exists → mark CONFIRMED → return a persistent
 *       {@code CHAT_IN} message referencing the objectKey; the REST adapter feeds it into the SAME
 *       delivery pipeline as a normal chat message (uniform persistence + live delivery + Tetris GC).</li>
 *   <li>{@link #resolveDownload} — membership check → presigned GET URL.</li>
 * </ol>
 * The service stores the {@code objectKey}, never a URL; URLs are minted on demand.
 */
@Slf4j
@ApplicationScoped
public class Attachments implements Uploads {

    /**
     * Payload kind for an attachment message (validator recognises "attachment").
     */
    public static final String KIND_ATTACHMENT = "attachment";
    public static final String META_ATTACHMENT_ID = "attachmentId";
    public static final String META_OBJECT_KEY = "objectKey";
    public static final String META_FILE_NAME = "fileName";
    public static final String META_CONTENT_TYPE = "contentType";
    public static final String META_SIZE = "sizeBytes";

    @Inject
    AttachmentManager attachments;

    @Inject
    ObjectStorage storage;

    @Inject
    Conversations conversations;

    @Inject
    ChatMessageEmitter emitter;

    @Inject
    IdGenerator idGenerator;

    @Inject
    AttachmentsConfig attachmentsConfig;

    @Inject
    MinioConfig minioConfig;

    // Resolved from config at startup (kept as fields so tests can set them directly).
    long maxSizeBytes;
    java.util.Optional<java.util.List<String>> allowedContentTypes = java.util.Optional.empty();
    int uploadTtlSeconds;
    int downloadTtlSeconds;

    @PostConstruct
    void init() {
        maxSizeBytes = attachmentsConfig.maxSizeBytes();
        allowedContentTypes = attachmentsConfig.allowedContentTypes();
        uploadTtlSeconds = minioConfig.uploadTtlSeconds();
        downloadTtlSeconds = minioConfig.downloadTtlSeconds();
    }

    // Result/value types (UploadStatus, UploadTicket, UploadResult, ConfirmResult, DownloadResult)
    // are domain types (domain.attachment); the Uploads port references them — the core never leaks
    // its own types outward.

    /**
     * Phase 1: validate membership/size, persist PENDING, return a presigned PUT URL.
     */
    public Uni<UploadResult> requestUpload(String conversationId, String uploaderId,
                                           String fileName, String contentType, Long sizeBytes) {
        // Declared size must be present and within the limit (a cheap early reject; the ACTUAL size is
        // re-checked against MinIO at confirm, since a client can lie about the declared size).
        if (sizeBytes == null || sizeBytes <= 0 || sizeBytes > maxSizeBytes) {
            return Uni.createFrom().item(UploadResult.fail(UploadStatus.TOO_LARGE));
        }
        if (!contentTypeAllowed(contentType)) {
            return Uni.createFrom().item(UploadResult.fail(UploadStatus.UNSUPPORTED_TYPE));
        }
        return conversations.findById(conversationId).flatMap(conv -> {
            if (conv == null) return Uni.createFrom().item(UploadResult.fail(UploadStatus.NOT_FOUND));
            if (!conv.hasParticipant(uploaderId))
                return Uni.createFrom().item(UploadResult.fail(UploadStatus.FORBIDDEN));
            // Uploading is sending — blocked pairs cannot upload (the emitted message would be rejected
            // by the send-guard anyway; reject early so we don't mint a URL or a PENDING row).
            if (conv.isBlocked())
                return Uni.createFrom().item(UploadResult.fail(UploadStatus.FORBIDDEN));

            String attachmentId = idGenerator.generateId();
            String objectKey = conversationId + "/" + attachmentId;
            Attachment pending = new Attachment(attachmentId, conversationId, uploaderId, objectKey,
                    fileName, contentType, sizeBytes, AttachmentStatus.PENDING, Instant.now(), null);

            return attachments.insertPending(pending)
                    .replaceWith(storage.presignPut(objectKey, contentType, uploadTtlSeconds))
                    .map(url -> new UploadResult(UploadStatus.OK, new UploadTicket(attachmentId, objectKey, url)));
        });
    }

    /**
     * Phase 3: verify the object, mark CONFIRMED, build the persistent attachment chat message.
     */
    public Uni<ConfirmResult> confirmUpload(String attachmentId, String callerId) {
        return attachments.findById(attachmentId).flatMap(att -> {
            if (att == null) return done(UploadStatus.NOT_FOUND);
            if (!att.uploaderId().equals(callerId)) return done(UploadStatus.FORBIDDEN);
            return switch (att.status()) {
                // Terminal: message already emitted — a re-confirm is a no-op (no duplicate).
                case DELIVERED -> done(UploadStatus.ALREADY_CONFIRMED);
                // Confirmed but the emit never landed (ingress was overloaded) — re-emit on retry.
                case CONFIRMED -> emitAndDeliver(att);
                // First confirm: verify the object exists AND its real size, then CONFIRMED → emit.
                case PENDING -> storage.exists(att.objectKey()).flatMap(present -> {
                    if (!present) return done(UploadStatus.NOT_UPLOADED);
                    return storage.size(att.objectKey()).flatMap(actual -> {
                        if (actual > maxSizeBytes) return done(UploadStatus.TOO_LARGE);
                        return attachments.markConfirmed(attachmentId, Instant.now())
                                .flatMap(x -> emitAndDeliver(att));
                    });
                });
                // Reclaimed — the object is gone.
                case ORPHANED -> done(UploadStatus.NOT_FOUND);
            };
        });
    }

    /**
     * Emit the attachment message into the pipeline (the single router) and, on acceptance, flip
     * CONFIRMED→DELIVERED so a later retry won't re-emit. On ingress overload the row stays CONFIRMED
     * (not DELIVERED), so the client's retried confirm re-emits — no lost message, no duplicate.
     */
    private Uni<ConfirmResult> emitAndDeliver(Attachment att) {
        return conversations.findById(att.conversationId()).flatMap(conv -> {
            if (conv == null) return done(UploadStatus.NOT_FOUND);
            if (!emitter.emit(toMessage(att, conv))) return done(UploadStatus.OVERLOADED);
            return attachments.markDelivered(att.id()).replaceWith(done(UploadStatus.OK));
        });
    }

    private static Uni<ConfirmResult> done(UploadStatus status) {
        return Uni.createFrom().item(ConfirmResult.of(status));
    }

    /** True iff the content-type is allowed. Empty whitelist = allow all; supports "type/*" wildcards. */
    private boolean contentTypeAllowed(String contentType) {
        if (allowedContentTypes == null || allowedContentTypes.isEmpty() || allowedContentTypes.get().isEmpty()) {
            return true; // no whitelist configured = allow all
        }
        if (contentType == null || contentType.isBlank()) return false;
        String ct = contentType.trim().toLowerCase();
        for (String allowed : allowedContentTypes.get()) {
            String a = allowed.trim().toLowerCase();
            if (a.equals(ct)) return true;
            if (a.endsWith("/*") && ct.startsWith(a.substring(0, a.length() - 1))) return true;
        }
        return false;
    }

    /**
     * Read time: membership check → presigned GET URL for a CONFIRMED attachment.
     */
    public Uni<DownloadResult> resolveDownload(String attachmentId, String callerId) {
        return attachments.findById(attachmentId).flatMap(att -> {
            // Downloadable once the object is verified — CONFIRMED or DELIVERED (not PENDING/ORPHANED).
            // Read path, so membership-only (block doesn't gate reads, mirroring history).
            if (att == null
                    || (att.status() != AttachmentStatus.CONFIRMED && att.status() != AttachmentStatus.DELIVERED)) {
                return Uni.createFrom().item(DownloadResult.fail(UploadStatus.NOT_FOUND));
            }
            return conversations.findById(att.conversationId()).flatMap(conv -> {
                if (conv == null) return Uni.createFrom().item(DownloadResult.fail(UploadStatus.NOT_FOUND));
                if (!conv.hasParticipant(callerId))
                    return Uni.createFrom().item(DownloadResult.fail(UploadStatus.FORBIDDEN));
                return storage.presignGet(att.objectKey(), downloadTtlSeconds)
                        .map(url -> new DownloadResult(UploadStatus.OK, url));
            });
        });
    }

    /**
     * Build the persistent CHAT_IN message that references the uploaded object.
     */
    private Message toMessage(Attachment att, Conversation conv) {
        String recipientId = conv.clientId().equals(att.uploaderId()) ? conv.masterId() : conv.clientId();
        Map<String, String> meta = new LinkedHashMap<>();
        meta.put(META_ATTACHMENT_ID, att.id());
        meta.put(META_OBJECT_KEY, att.objectKey());
        if (att.fileName() != null) meta.put(META_FILE_NAME, att.fileName());
        if (att.contentType() != null) meta.put(META_CONTENT_TYPE, att.contentType());
        if (att.sizeBytes() != null) meta.put(META_SIZE, String.valueOf(att.sizeBytes()));

        return Message.builder()
                .type(MessageType.CHAT_IN)
                .senderId(att.uploaderId())
                .recipientId(recipientId)
                .conversationId(att.conversationId())
                .senderTimestamp(System.currentTimeMillis())
                .senderTimezone("UTC")
                .payload(Message.Payload.builder()
                        .kind(KIND_ATTACHMENT)
                        .body(att.fileName() != null ? att.fileName() : att.objectKey())
                        .build())
                .meta(meta)
                .build();
    }
}
