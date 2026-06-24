package org.hormigas.ws.scheduler;

import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.domain.message.Message;
import org.hormigas.ws.ports.deadletter.DeadLetterStore;
import org.hormigas.ws.ports.deadletter.DeliveryConfirmations;

/**
 * Dead-letter retract sweep (Strategy C, ADR-014). Independent of the outbox GC (own cadence — the
 * codebase convention is one single-purpose scheduler per concern; this is non-latency-sensitive
 * audit pruning). Drains the Redis confirmed-set, deletes the matching {@code dead_letter} DRAFTs,
 * then clears the confirmations — so a confirmation survives until its draft is actually removed
 * (no TTL/tick race). What stays as a DRAFT = the genuinely undelivered notices.
 */
@Slf4j
@ApplicationScoped
@IfBuildProfile("prod")
public class DeadLetterCleanupScheduler {

    private static final int BATCH = 500;

    @Inject
    DeliveryConfirmations confirmations;

    @Inject
    DeadLetterStore<Message> deadLetter;

    @Scheduled(every = "${processing.deadletter.cleanup-every:30s}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void sweep() {
        cleanup().subscribe().with(
                n -> { if (n > 0) log.debug("Retracted {} confirmed dead-letter draft(s)", n); },
                err -> log.error("Dead-letter cleanup sweep failed", err));
    }

    /** Testable core: delete drafts for confirmed ids, then clear those confirmations. */
    Uni<Integer> cleanup() {
        return confirmations.peek(BATCH).flatMap(ids -> {
            if (ids.isEmpty()) return Uni.createFrom().item(0);
            return deadLetter.deleteDrafts(ids)
                    .call(() -> confirmations.clear(ids));
        });
    }
}
