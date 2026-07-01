package org.hormigas.ws.core.attachment;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.attachment.ConfirmResult;
import org.hormigas.ws.domain.attachment.DownloadResult;
import org.hormigas.ws.domain.attachment.UploadStatus;
import org.hormigas.ws.domain.attachment.UploadResult;
import org.hormigas.ws.core.conversation.Conversations;
import org.hormigas.ws.domain.attachment.Attachment;
import org.hormigas.ws.domain.attachment.Attachment.AttachmentStatus;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.ports.attachment.AttachmentManager;
import org.hormigas.ws.ports.emit.ChatMessageEmitter;
import org.hormigas.ws.ports.storage.ObjectStorage;
import org.hormigas.ws.domain.storage.PresignedUrl;
import org.mockito.ArgumentCaptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("Attachments — two-phase presigned upload (concept §10, ADR-010)")
class AttachmentsTest {

    private final AttachmentManager repo = mock(AttachmentManager.class);
    private final ObjectStorage storage = mock(ObjectStorage.class);
    private final Conversations conversations = mock(Conversations.class);
    private final ChatMessageEmitter emitter = mock(ChatMessageEmitter.class);
    private final IdGenerator idGen = mock(IdGenerator.class);

    private Attachments svc;

    private static final Conversation CONV =
            new Conversation("conv1", "client1", "master1", Map.of(), false, false, Instant.now(), Instant.now());

    @BeforeEach
    void setup() {
        svc = new Attachments();
        svc.attachments = repo;
        svc.storage = storage;
        svc.conversations = conversations;
        svc.emitter = emitter;
        svc.idGenerator = idGen;
        svc.maxSizeBytes = 1000;
        svc.uploadTtlSeconds = 600;
        svc.downloadTtlSeconds = 300;
    }

    private Attachment pending() {
        return new Attachment("att1", "conv1", "client1", "conv1/att1", "f.png", "image/png", 10L,
                AttachmentStatus.PENDING, Instant.now(), null);
    }

    // ---- requestUpload -------------------------------------------------------

    @Test
    @DisplayName("member → PENDING persisted + presigned PUT URL returned")
    void requestUploadOk() {
        when(conversations.findById("conv1")).thenReturn(Uni.createFrom().item(CONV));
        when(idGen.generateId()).thenReturn("att1");
        when(repo.insertPending(any())).thenReturn(Uni.createFrom().voidItem());
        when(storage.presignPut("conv1/att1", "image/png", 600))
                .thenReturn(Uni.createFrom().item(new PresignedUrl("http://minio/put", "PUT", Instant.now())));

        UploadResult r = svc.requestUpload("conv1", "client1", "f.png", "image/png", 10L)
                .await().indefinitely();

        assertThat(r.status()).isEqualTo(UploadStatus.OK);
        assertThat(r.ticket().attachmentId()).isEqualTo("att1");
        assertThat(r.ticket().objectKey()).isEqualTo("conv1/att1");
        assertThat(r.ticket().upload().url()).isEqualTo("http://minio/put");
        verify(repo).insertPending(argThat(a -> a.status() == AttachmentStatus.PENDING && "conv1/att1".equals(a.objectKey())));
    }

    @Test
    @DisplayName("non-member → FORBIDDEN, nothing persisted")
    void requestUploadForbidden() {
        when(conversations.findById("conv1")).thenReturn(Uni.createFrom().item(CONV));
        UploadResult r = svc.requestUpload("conv1", "stranger", "f.png", "image/png", 10L).await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.FORBIDDEN);
        verify(repo, never()).insertPending(any());
    }

    @Test
    @DisplayName("unknown chat → NOT_FOUND")
    void requestUploadNotFound() {
        when(conversations.findById("nope")).thenReturn(Uni.createFrom().nullItem());
        UploadResult r = svc.requestUpload("nope", "client1", "f.png", "image/png", 10L).await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("oversized → TOO_LARGE before any work")
    void requestUploadTooLarge() {
        UploadResult r = svc.requestUpload("conv1", "client1", "big.bin", "application/octet-stream", 99999L)
                .await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.TOO_LARGE);
        verifyNoInteractions(conversations, repo, storage);
    }

    @Test
    @DisplayName("missing/zero declared size → TOO_LARGE before any work (size can't be omitted)")
    void requestUploadSizeRequired() {
        assertThat(svc.requestUpload("conv1", "client1", "f.png", "image/png", null).await().indefinitely().status())
                .isEqualTo(UploadStatus.TOO_LARGE);
        assertThat(svc.requestUpload("conv1", "client1", "f.png", "image/png", 0L).await().indefinitely().status())
                .isEqualTo(UploadStatus.TOO_LARGE);
        verifyNoInteractions(conversations, repo, storage);
    }

    @Test
    @DisplayName("content-type not in the whitelist → UNSUPPORTED_TYPE")
    void requestUploadUnsupportedType() {
        svc.allowedContentTypes = java.util.Optional.of(java.util.List.of("image/*", "application/pdf"));
        UploadResult r = svc.requestUpload("conv1", "client1", "evil.exe", "application/x-msdownload", 10L)
                .await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.UNSUPPORTED_TYPE);
        verifyNoInteractions(conversations, repo, storage);
    }

    @Test
    @DisplayName("blocked pair → FORBIDDEN, nothing persisted (uploading is sending)")
    void requestUploadBlocked() {
        Conversation blocked = new Conversation("conv1", "client1", "master1", Map.of(), true, false,
                Instant.now(), Instant.now());
        when(conversations.findById("conv1")).thenReturn(Uni.createFrom().item(blocked));
        UploadResult r = svc.requestUpload("conv1", "client1", "f.png", "image/png", 10L).await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.FORBIDDEN);
        verify(repo, never()).insertPending(any());
    }

    // ---- confirmUpload -------------------------------------------------------

    @Test
    @DisplayName("PENDING + object present & within size → CONFIRMED→emit→DELIVERED; CHAT_IN to the peer")
    void confirmOk() {
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(pending()));
        when(storage.exists("conv1/att1")).thenReturn(Uni.createFrom().item(true));
        when(storage.size("conv1/att1")).thenReturn(Uni.createFrom().item(10L)); // within maxSizeBytes=1000
        when(repo.markConfirmed(eq("att1"), any())).thenReturn(Uni.createFrom().item(pending()));
        when(repo.markDelivered("att1")).thenReturn(Uni.createFrom().voidItem());
        when(conversations.findById("conv1")).thenReturn(Uni.createFrom().item(CONV));
        when(emitter.emit(any())).thenReturn(true);

        ConfirmResult r = svc.confirmUpload("att1", "client1").await().indefinitely();

        assertThat(r.status()).isEqualTo(UploadStatus.OK);
        verify(repo).markDelivered("att1"); // flipped CONFIRMED→DELIVERED after a successful emit
        // the core emitted the attachment message into the pipeline (not returned to the adapter)
        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(emitter).emit(captor.capture());
        Message m = captor.getValue();
        assertThat(m.getType()).isEqualTo(MessageType.CHAT_IN);
        assertThat(m.getSenderId()).isEqualTo("client1");
        assertThat(m.getRecipientId()).isEqualTo("master1");           // the other participant
        assertThat(m.getConversationId()).isEqualTo("conv1");
        assertThat(m.getPayload().getKind()).isEqualTo(Attachments.KIND_ATTACHMENT);
        assertThat(m.getMeta()).containsEntry(Attachments.META_OBJECT_KEY, "conv1/att1");
        assertThat(m.getMeta()).containsEntry(Attachments.META_ATTACHMENT_ID, "att1");
    }

    @Test
    @DisplayName("confirmed but ingress overloaded → OVERLOADED (client retries the confirm)")
    void confirmOverloaded() {
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(pending()));
        when(storage.exists("conv1/att1")).thenReturn(Uni.createFrom().item(true));
        when(storage.size("conv1/att1")).thenReturn(Uni.createFrom().item(10L));
        when(repo.markConfirmed(eq("att1"), any())).thenReturn(Uni.createFrom().item(pending()));
        when(conversations.findById("conv1")).thenReturn(Uni.createFrom().item(CONV));
        when(emitter.emit(any())).thenReturn(false); // ingress full

        ConfirmResult r = svc.confirmUpload("att1", "client1").await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.OVERLOADED);
        // NOT marked delivered — a retried confirm will re-emit (no lost message)
        verify(repo, never()).markDelivered(any());
    }

    @Test
    @DisplayName("actual object larger than the limit → TOO_LARGE, not confirmed (client can't lie about size)")
    void confirmActualTooLarge() {
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(pending()));
        when(storage.exists("conv1/att1")).thenReturn(Uni.createFrom().item(true));
        when(storage.size("conv1/att1")).thenReturn(Uni.createFrom().item(9999L)); // > maxSizeBytes=1000
        ConfirmResult r = svc.confirmUpload("att1", "client1").await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.TOO_LARGE);
        verify(repo, never()).markConfirmed(any(), any());
        verify(emitter, never()).emit(any());
    }

    @Test
    @DisplayName("CONFIRMED-but-not-DELIVERED (prior overload) → re-emits on retry → DELIVERED")
    void confirmConfirmedReEmits() {
        Attachment confirmed = new Attachment("att1", "conv1", "client1", "conv1/att1", "f.png", "image/png", 10L,
                AttachmentStatus.CONFIRMED, Instant.now(), Instant.now());
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(confirmed));
        when(conversations.findById("conv1")).thenReturn(Uni.createFrom().item(CONV));
        when(emitter.emit(any())).thenReturn(true);
        when(repo.markDelivered("att1")).thenReturn(Uni.createFrom().voidItem());

        ConfirmResult r = svc.confirmUpload("att1", "client1").await().indefinitely();

        assertThat(r.status()).isEqualTo(UploadStatus.OK);
        verify(emitter).emit(any());           // the lost message is re-sent
        verify(repo).markDelivered("att1");
        verify(storage, never()).exists(any()); // skips the object check — already confirmed
    }

    @Test
    @DisplayName("object never uploaded → NOT_UPLOADED, not confirmed")
    void confirmNotUploaded() {
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(pending()));
        when(storage.exists("conv1/att1")).thenReturn(Uni.createFrom().item(false));
        ConfirmResult r = svc.confirmUpload("att1", "client1").await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.NOT_UPLOADED);
        verify(repo, never()).markConfirmed(any(), any());
    }

    @Test
    @DisplayName("re-confirm of a DELIVERED attachment → idempotent, no second message")
    void confirmAlready() {
        Attachment delivered = new Attachment("att1", "conv1", "client1", "conv1/att1", "f.png", "image/png", 10L,
                AttachmentStatus.DELIVERED, Instant.now(), Instant.now());
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(delivered));
        ConfirmResult r = svc.confirmUpload("att1", "client1").await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.ALREADY_CONFIRMED);
        verify(emitter, never()).emit(any());
        verify(storage, never()).exists(any());
    }

    @Test
    @DisplayName("confirm by a non-uploader → FORBIDDEN")
    void confirmForbidden() {
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(pending()));
        ConfirmResult r = svc.confirmUpload("att1", "master1").await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.FORBIDDEN);
    }

    @Test
    @DisplayName("confirm of an unknown attachment → NOT_FOUND")
    void confirmNotFound() {
        when(repo.findById("nope")).thenReturn(Uni.createFrom().nullItem());
        ConfirmResult r = svc.confirmUpload("nope", "client1").await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.NOT_FOUND);
    }

    // ---- resolveDownload -----------------------------------------------------

    @Test
    @DisplayName("member downloads a CONFIRMED attachment → presigned GET URL")
    void downloadOk() {
        Attachment confirmed = new Attachment("att1", "conv1", "client1", "conv1/att1", "f.png", "image/png", 10L,
                AttachmentStatus.CONFIRMED, Instant.now(), Instant.now());
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(confirmed));
        when(conversations.findById("conv1")).thenReturn(Uni.createFrom().item(CONV));
        when(storage.presignGet("conv1/att1", 300))
                .thenReturn(Uni.createFrom().item(new PresignedUrl("http://minio/get", "GET", Instant.now())));

        DownloadResult r = svc.resolveDownload("att1", "master1").await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.OK);
        assertThat(r.download().url()).isEqualTo("http://minio/get");
    }

    @Test
    @DisplayName("non-member download → FORBIDDEN")
    void downloadForbidden() {
        Attachment confirmed = new Attachment("att1", "conv1", "client1", "conv1/att1", "f.png", "image/png", 10L,
                AttachmentStatus.CONFIRMED, Instant.now(), Instant.now());
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(confirmed));
        when(conversations.findById("conv1")).thenReturn(Uni.createFrom().item(CONV));
        DownloadResult r = svc.resolveDownload("att1", "stranger").await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.FORBIDDEN);
    }

    @Test
    @DisplayName("download of a still-PENDING attachment → NOT_FOUND")
    void downloadPendingNotFound() {
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(pending()));
        DownloadResult r = svc.resolveDownload("att1", "client1").await().indefinitely();
        assertThat(r.status()).isEqualTo(UploadStatus.NOT_FOUND);
    }
}
