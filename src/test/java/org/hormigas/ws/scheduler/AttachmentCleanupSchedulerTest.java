package org.hormigas.ws.scheduler;

import io.smallrye.mutiny.Uni;
import org.hormigas.ws.domain.attachment.Attachment;
import org.hormigas.ws.domain.attachment.Attachment.AttachmentStatus;
import org.hormigas.ws.config.AttachmentsConfig;
import org.hormigas.ws.ports.attachment.AttachmentManager;
import org.hormigas.ws.ports.storage.ObjectStorage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("AttachmentCleanupScheduler — reclaims stale PENDING uploads (ADR-010 cleanup)")
class AttachmentCleanupSchedulerTest {

    private final AttachmentManager repo = mock(AttachmentManager.class);
    private final ObjectStorage storage = mock(ObjectStorage.class);

    private AttachmentCleanupScheduler scheduler() {
        AttachmentsConfig cfg = mock(AttachmentsConfig.class);
        when(cfg.orphanAgeSeconds()).thenReturn(3600L);
        when(cfg.cleanupBatch()).thenReturn(200);
        AttachmentCleanupScheduler s = new AttachmentCleanupScheduler();
        s.attachments = repo;
        s.storage = storage;
        s.config = cfg;
        return s;
    }

    private Attachment stale(String id) {
        return new Attachment(id, "conv1", "client1", "conv1/" + id, "f", "image/png", 1L,
                AttachmentStatus.PENDING, Instant.now().minusSeconds(7200), null);
    }

    @Test
    @DisplayName("each stale PENDING → object deleted + row marked ORPHANED")
    void reclaimsStale() {
        when(repo.findStalePending(any(), anyInt()))
                .thenReturn(Uni.createFrom().item(List.of(stale("a1"), stale("a2"))));
        when(storage.delete(any())).thenReturn(Uni.createFrom().voidItem());
        when(repo.markOrphaned(any())).thenReturn(Uni.createFrom().voidItem());

        int n = scheduler().reclaim(Instant.now()).await().indefinitely();

        assertThat(n).isEqualTo(2);
        verify(storage).delete("conv1/a1");
        verify(storage).delete("conv1/a2");
        verify(repo).markOrphaned("a1");
        verify(repo).markOrphaned("a2");
    }

    @Test
    @DisplayName("nothing stale → no storage/DB writes")
    void reclaimsNothing() {
        when(repo.findStalePending(any(), anyInt())).thenReturn(Uni.createFrom().item(List.of()));
        int n = scheduler().reclaim(Instant.now()).await().indefinitely();
        assertThat(n).isZero();
        verify(storage, never()).delete(any());
        verify(repo, never()).markOrphaned(any());
    }
}
