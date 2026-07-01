package org.hormigas.ws.scheduler;

import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.infrastructure.persistance.postgres.HistoryRepository;

import java.time.Duration;
import java.time.Instant;

/**
 * History retention sweep (UC-U23, FR-RET-04/05). Two retention classes: normal history is purged
 * after {@code history-days}; frozen messages have their own, longer TTL ({@code frozen-days}).
 * The repository's {@code deleteOlderThan} already excludes frozen, so the normal sweep can never
 * delete a frozen contract record; {@code deleteFrozenOlderThan} handles the frozen class.
 *
 * <p>This is distinct from the Outbox GC ({@code GarbageScheduler}): that trims the delivery buffer,
 * this ages out the durable History store.</p>
 */
@Slf4j
@ApplicationScoped
@IfBuildProfile("prod")
public class HistoryRetentionScheduler {

    @Inject
    HistoryRepository history;

    @Inject
    org.hormigas.ws.config.RetentionConfig config;

    // Resolved from config at startup (kept as fields so tests can set them directly).
    int historyDays;
    int frozenDays;

    @jakarta.annotation.PostConstruct
    void init() {
        historyDays = config.historyDays();
        frozenDays = config.frozenDays();
    }

    @Scheduled(every = "${processing.retention.every:24h}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void purge() {
        Instant now = Instant.now();
        purge(now.minus(Duration.ofDays(historyDays)), now.minus(Duration.ofDays(frozenDays)))
                .subscribe().with(
                        total -> log.info("History retention sweep removed {} rows", total),
                        err -> log.error("History retention sweep failed", err));
    }

    /** Testable core: purge normal history before {@code normalCutoff} and frozen before {@code frozenCutoff}. */
    Uni<Integer> purge(Instant normalCutoff, Instant frozenCutoff) {
        return history.deleteOlderThan(normalCutoff)
                .flatMap(normal -> history.deleteFrozenOlderThan(frozenCutoff)
                        .map(frozen -> normal + frozen));
    }
}
