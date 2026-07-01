package org.hormigas.ws.scheduler;

import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.AttachmentsConfig;
import org.hormigas.ws.domain.attachment.Attachment;
import org.hormigas.ws.ports.attachment.AttachmentManager;
import org.hormigas.ws.ports.storage.ObjectStorage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Orphan-attachment reclaim (ADR-010 cleanup path). A PENDING row whose confirm never arrives —
 * the client got a presigned URL but never uploaded, or never confirmed — is reclaimed after
 * {@code orphan-age-seconds}: the (possibly-partial) MinIO object is best-effort deleted and the
 * row is marked ORPHANED. CONFIRMED rows are never touched. Prod-only (like the other reapers).
 */
@Slf4j
@ApplicationScoped
@IfBuildProfile("prod")
public class AttachmentCleanupScheduler {

    @Inject
    AttachmentManager attachments;

    @Inject
    ObjectStorage storage;

    @Inject
    AttachmentsConfig config;

    @Scheduled(every = "${processing.attachments.cleanup-every:15m}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void reclaim() {
        Instant cutoff = Instant.now().minus(Duration.ofSeconds(config.orphanAgeSeconds()));
        reclaim(cutoff)
                .subscribe().with(
                        n -> { if (n > 0) log.info("Reclaimed {} orphaned attachment(s)", n); },
                        err -> log.error("Attachment cleanup failed", err));
    }

    /** Testable core: reclaim PENDING attachments older than {@code cutoff}; returns the count reclaimed. */
    Uni<Integer> reclaim(Instant cutoff) {
        return attachments.findStalePending(cutoff, config.cleanupBatch())
                .flatMap(stale -> {
                    if (stale.isEmpty()) return Uni.createFrom().item(0);
                    List<Uni<Void>> ops = stale.stream().map(this::reclaimOne).toList();
                    return Uni.join().all(ops).andCollectFailures().replaceWith(stale.size());
                });
    }

    private Uni<Void> reclaimOne(Attachment a) {
        // Delete the object first (best-effort, never throws), then mark the row terminal.
        return storage.delete(a.objectKey())
                .replaceWith(attachments.markOrphaned(a.id()))
                .onFailure().recoverWithItem(err -> {
                    log.warn("Could not reclaim attachment {}: {}", a.id(),
                            err == null ? "?" : err.getMessage());
                    return null;
                });
    }
}
