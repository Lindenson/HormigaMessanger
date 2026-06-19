package org.hormigas.ws.core.attachment;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.core.attachment.AttachmentService.ConfirmResult;
import org.hormigas.ws.core.attachment.AttachmentService.DownloadResult;
import org.hormigas.ws.core.attachment.AttachmentService.Status;
import org.hormigas.ws.core.attachment.AttachmentService.UploadResult;
import org.hormigas.ws.core.conversation.ConversationService;
import org.hormigas.ws.domain.attachment.Attachment;
import org.hormigas.ws.domain.attachment.Attachment.AttachmentStatus;
import org.hormigas.ws.domain.conversation.Conversation;
import org.hormigas.ws.domain.generator.IdGenerator;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.domain.message.MessageType;
import org.hormigas.ws.ports.attachment.AttachmentRepository;
import org.hormigas.ws.ports.storage.ObjectStorage;
import org.hormigas.ws.ports.storage.ObjectStorage.PresignedUrl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("AttachmentService — two-phase presigned upload (concept §10, ADR-010)")
class AttachmentServiceTest {

    private final AttachmentRepository repo = mock(AttachmentRepository.class);
    private final ObjectStorage storage = mock(ObjectStorage.class);
    private final ConversationService conversations = mock(ConversationService.class);
    private final IdGenerator idGen = mock(IdGenerator.class);

    private AttachmentService svc;

    private static final Conversation CONV =
            new Conversation("conv1", "client1", "master1", Map.of(), false, false, Instant.now(), Instant.now());

    @BeforeEach
    void setup() {
        svc = new AttachmentService();
        svc.attachments = repo;
        svc.storage = storage;
        svc.conversations = conversations;
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

        assertThat(r.status()).isEqualTo(Status.OK);
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
        assertThat(r.status()).isEqualTo(Status.FORBIDDEN);
        verify(repo, never()).insertPending(any());
    }

    @Test
    @DisplayName("unknown chat → NOT_FOUND")
    void requestUploadNotFound() {
        when(conversations.findById("nope")).thenReturn(Uni.createFrom().nullItem());
        UploadResult r = svc.requestUpload("nope", "client1", "f.png", "image/png", 10L).await().indefinitely();
        assertThat(r.status()).isEqualTo(Status.NOT_FOUND);
    }

    @Test
    @DisplayName("oversized → TOO_LARGE before any work")
    void requestUploadTooLarge() {
        UploadResult r = svc.requestUpload("conv1", "client1", "big.bin", "application/octet-stream", 99999L)
                .await().indefinitely();
        assertThat(r.status()).isEqualTo(Status.TOO_LARGE);
        verifyNoInteractions(conversations, repo, storage);
    }

    // ---- confirmUpload -------------------------------------------------------

    @Test
    @DisplayName("uploaded object present → CONFIRMED + CHAT_IN message to the peer, referencing the object")
    void confirmOk() {
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(pending()));
        when(storage.exists("conv1/att1")).thenReturn(Uni.createFrom().item(true));
        when(repo.markConfirmed(eq("att1"), any())).thenReturn(Uni.createFrom().item(pending()));
        when(conversations.findById("conv1")).thenReturn(Uni.createFrom().item(CONV));

        ConfirmResult r = svc.confirmUpload("att1", "client1").await().indefinitely();

        assertThat(r.status()).isEqualTo(Status.OK);
        Message m = r.message();
        assertThat(m.getType()).isEqualTo(MessageType.CHAT_IN);
        assertThat(m.getSenderId()).isEqualTo("client1");
        assertThat(m.getRecipientId()).isEqualTo("master1");           // the other participant
        assertThat(m.getConversationId()).isEqualTo("conv1");
        assertThat(m.getPayload().getKind()).isEqualTo(AttachmentService.KIND_ATTACHMENT);
        assertThat(m.getMeta()).containsEntry(AttachmentService.META_OBJECT_KEY, "conv1/att1");
        assertThat(m.getMeta()).containsEntry(AttachmentService.META_ATTACHMENT_ID, "att1");
    }

    @Test
    @DisplayName("object never uploaded → NOT_UPLOADED, not confirmed")
    void confirmNotUploaded() {
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(pending()));
        when(storage.exists("conv1/att1")).thenReturn(Uni.createFrom().item(false));
        ConfirmResult r = svc.confirmUpload("att1", "client1").await().indefinitely();
        assertThat(r.status()).isEqualTo(Status.NOT_UPLOADED);
        verify(repo, never()).markConfirmed(any(), any());
    }

    @Test
    @DisplayName("re-confirm of a CONFIRMED attachment → idempotent, no second message")
    void confirmAlready() {
        Attachment confirmed = new Attachment("att1", "conv1", "client1", "conv1/att1", "f.png", "image/png", 10L,
                AttachmentStatus.CONFIRMED, Instant.now(), Instant.now());
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(confirmed));
        ConfirmResult r = svc.confirmUpload("att1", "client1").await().indefinitely();
        assertThat(r.status()).isEqualTo(Status.ALREADY_CONFIRMED);
        assertThat(r.message()).isNull();
        verify(storage, never()).exists(any());
    }

    @Test
    @DisplayName("confirm by a non-uploader → FORBIDDEN")
    void confirmForbidden() {
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(pending()));
        ConfirmResult r = svc.confirmUpload("att1", "master1").await().indefinitely();
        assertThat(r.status()).isEqualTo(Status.FORBIDDEN);
    }

    @Test
    @DisplayName("confirm of an unknown attachment → NOT_FOUND")
    void confirmNotFound() {
        when(repo.findById("nope")).thenReturn(Uni.createFrom().nullItem());
        ConfirmResult r = svc.confirmUpload("nope", "client1").await().indefinitely();
        assertThat(r.status()).isEqualTo(Status.NOT_FOUND);
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
        assertThat(r.status()).isEqualTo(Status.OK);
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
        assertThat(r.status()).isEqualTo(Status.FORBIDDEN);
    }

    @Test
    @DisplayName("download of a still-PENDING attachment → NOT_FOUND")
    void downloadPendingNotFound() {
        when(repo.findById("att1")).thenReturn(Uni.createFrom().item(pending()));
        DownloadResult r = svc.resolveDownload("att1", "client1").await().indefinitely();
        assertThat(r.status()).isEqualTo(Status.NOT_FOUND);
    }
}
