package org.hormigas.ws.core.attachment;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;
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

    @ConfigProperty(name = "processing.attachments.max-size-bytes", defaultValue = "26214400")
    long maxSizeBytes;

    @ConfigProperty(name = "minio.upload-ttl-seconds", defaultValue = "600")
    int uploadTtlSeconds;

    @ConfigProperty(name = "minio.download-ttl-seconds", defaultValue = "300")
    int downloadTtlSeconds;

    // Result/value types (UploadStatus, UploadTicket, UploadResult, ConfirmResult, DownloadResult)
    // are domain types (domain.attachment); the Uploads port references them — the core never leaks
    // its own types outward.

    /**
     * Phase 1: validate membership/size, persist PENDING, return a presigned PUT URL.
     */
    public Uni<UploadResult> requestUpload(String conversationId, String uploaderId,
                                           String fileName, String contentType, Long sizeBytes) {
        if (sizeBytes != null && sizeBytes > maxSizeBytes) {
            return Uni.createFrom().item(UploadResult.fail(UploadStatus.TOO_LARGE));
        }
        return conversations.findById(conversationId).flatMap(conv -> {
            if (conv == null) return Uni.createFrom().item(UploadResult.fail(UploadStatus.NOT_FOUND));
            if (!conv.hasParticipant(uploaderId))
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
            if (att == null) return Uni.createFrom().item(ConfirmResult.of(UploadStatus.NOT_FOUND));
            if (!att.uploaderId().equals(callerId))
                return Uni.createFrom().item(ConfirmResult.of(UploadStatus.FORBIDDEN));
            if (att.status() == AttachmentStatus.CONFIRMED) {
                // Idempotent: a re-confirm does NOT emit a second message.
                return Uni.createFrom().item(ConfirmResult.of(UploadStatus.ALREADY_CONFIRMED));
            }
            return storage.exists(att.objectKey()).flatMap(present -> {
                if (!present) return Uni.createFrom().item(ConfirmResult.of(UploadStatus.NOT_UPLOADED));
                return attachments.markConfirmed(attachmentId, Instant.now())
                        .replaceWith(conversations.findById(att.conversationId()))
                        .map(conv -> {
                            if (conv == null) return ConfirmResult.of(UploadStatus.NOT_FOUND);
                            // Emit through the router (the single message pipeline) — same as a WS chat
                            // message: persisted, delivered live, GC'd uniformly. false = ingress overloaded.
                            boolean emitted = emitter.emit(toMessage(att, conv));
                            return ConfirmResult.of(emitted ? UploadStatus.OK : UploadStatus.OVERLOADED);
                        });
            });
        });
    }

    /**
     * Read time: membership check → presigned GET URL for a CONFIRMED attachment.
     */
    public Uni<DownloadResult> resolveDownload(String attachmentId, String callerId) {
        return attachments.findById(attachmentId).flatMap(att -> {
            if (att == null || att.status() != AttachmentStatus.CONFIRMED) {
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
