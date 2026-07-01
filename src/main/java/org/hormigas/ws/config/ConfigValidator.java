package org.hormigas.ws.config;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Startup fail-fast validation of the runtime configuration.
 *
 * <p>{@code @ConfigMapping} already enforces <em>presence</em> and <em>type</em> of every key. What it
 * cannot express is <em>semantics</em>: that a value is in-range and that related values are mutually
 * consistent (a max ≥ its min, a growth factor {@code > 1}, batch concurrency that fits the DB pool). A
 * mis-set knob that passes type-checking would otherwise surface as a subtle runtime pathology under load
 * — a stream that never backs off, a batcher that starves the pool, a reaper that never fires. This bean
 * turns all of those into a single, explicit boot failure with the full list of offending keys.
 *
 * <p>Hand-rolled on purpose (no {@code jakarta.validation}/hibernate-validator dependency): the rule set is
 * small, cross-field, and clearer as straight-line code than as scattered annotations. It accumulates
 * <em>every</em> violation and throws once, so a single restart surfaces all of them.
 */
@ApplicationScoped
public class ConfigValidator {

    @Inject
    MessengerConfig messenger;
    @Inject
    RateLimitConfig rateLimit;
    @Inject
    AttachmentsConfig attachments;
    @Inject
    MinioConfig minio;
    @Inject
    SessionConfig session;
    @Inject
    RetentionConfig retention;
    @Inject
    DeadLetterConfig deadLetter;

    /** Reactive PG pool ceiling — batch concurrency must not exceed it or batches queue on connections. */
    @ConfigProperty(name = "quarkus.datasource.reactive.max-size", defaultValue = "20")
    int dbPoolSize;

    void onStart(@Observes StartupEvent ev) {
        validate();
    }

    /** Package-visible so a unit test can drive it against mocked configs. */
    void validate() {
        List<String> errors = new ArrayList<>();

        validateMessenger(errors);
        validateRateLimit(errors);
        validateAttachments(errors);
        validateMinio(errors);
        validateSession(errors);
        validateRetention(errors);
        validateDeadLetter(errors);

        if (!errors.isEmpty()) {
            throw new IllegalStateException(
                    "Invalid configuration — refusing to start (" + errors.size() + " problem(s)):\n  - "
                            + String.join("\n  - ", errors));
        }
    }

    private void validateMessenger(List<String> e) {
        // Inbound / persist batcher
        positive(e, "processing.messages.inbound.queue-size", messenger.inbound().queueSize());
        var pb = messenger.inbound().persistBatch();
        positive(e, "processing.messages.inbound.persist-batch.max-size", pb.maxSize());
        positive(e, "processing.messages.inbound.persist-batch.linger-ms", pb.lingerMs());
        positive(e, "processing.messages.inbound.persist-batch.max-concurrent-batches", pb.maxConcurrentBatches());
        atMost(e, "processing.messages.inbound.persist-batch.max-concurrent-batches", pb.maxConcurrentBatches(),
                "quarkus.datasource.reactive.max-size", dbPoolSize);

        // Read batcher
        var rb = messenger.readBatch();
        positive(e, "processing.messages.read-batch.max-size", rb.maxSize());
        positive(e, "processing.messages.read-batch.linger-ms", rb.lingerMs());
        positive(e, "processing.messages.read-batch.max-concurrent-batches", rb.maxConcurrentBatches());
        atMost(e, "processing.messages.read-batch.max-concurrent-batches", rb.maxConcurrentBatches(),
                "quarkus.datasource.reactive.max-size", dbPoolSize);

        // Outbound poller
        positive(e, "processing.messages.outbound.batch-size", messenger.outbound().batchSize());
        positive(e, "processing.messages.outbound.queue-size", messenger.outbound().queueSize());

        // Stream re-subscribe backoff
        var sr = messenger.streamRetry();
        positive(e, "processing.messages.stream-retry.min-backoff-ms", sr.minBackoffMs());
        atLeast(e, "processing.messages.stream-retry.max-backoff-ms", sr.maxBackoffMs(),
                "processing.messages.stream-retry.min-backoff-ms", sr.minBackoffMs());

        // Adaptive feedback regulator
        var fb = messenger.feedback();
        positive(e, "processing.messages.feedback.additional-ms", fb.additionalMs());
        require(e, fb.adjustmentFactor() > 1.0,
                "processing.messages.feedback.adjustment-factor must be > 1 (growth under overload), was " + fb.adjustmentFactor());
        require(e, fb.recoveryFactor() > 0.0 && fb.recoveryFactor() < 1.0,
                "processing.messages.feedback.recovery-factor must be in (0,1) (decay toward base), was " + fb.recoveryFactor());
        atLeast(e, "processing.messages.feedback.max-ms", fb.maxMs(),
                "processing.messages.feedback.additional-ms", fb.additionalMs());

        // Delivery retry to a socket
        var ch = messenger.channel();
        positive(e, "processing.messages.channel.min-backoff-ms", ch.minBackoffMs());
        atLeast(e, "processing.messages.channel.max-backoff-ms", ch.maxBackoffMs(),
                "processing.messages.channel.min-backoff-ms", ch.minBackoffMs());
        positive(e, "processing.messages.channel.max-retries", ch.maxRetries());

        // Per-connection credits
        positive(e, "processing.messages.credits.max-value", messenger.credits().maxValue());
        positive(e, "processing.messages.credits.refill-rate-per-s", messenger.credits().refillRatePerS());

        // Idempotency + conversation cache
        positive(e, "processing.messages.idempotent.ttl-seconds", messenger.idempotent().ttlSeconds());
        positive(e, "processing.messages.conversation-cache.max-size", messenger.conversationCache().maxSize());
        positive(e, "processing.messages.conversation-cache.ttl-seconds", messenger.conversationCache().ttlSeconds());
    }

    private void validateRateLimit(List<String> e) {
        positive(e, "processing.rate-limit.bucket-cache-max", rateLimit.bucketCacheMax());
        positive(e, "processing.rate-limit.bucket-idle-minutes", rateLimit.bucketIdleMinutes());
        validateLimit(e, "processing.rate-limit.default-limit", rateLimit.defaultLimit());
        for (Map.Entry<String, RateLimitConfig.Limit> g : rateLimit.groups().entrySet()) {
            validateLimit(e, "processing.rate-limit.groups." + g.getKey(), g.getValue());
        }
    }

    private void validateLimit(List<String> e, String path, RateLimitConfig.Limit limit) {
        require(e, limit.permitsPerSecond() > 0.0,
                path + ".permits-per-second must be > 0, was " + limit.permitsPerSecond());
        positive(e, path + ".burst", limit.burst());
    }

    private void validateAttachments(List<String> e) {
        positive(e, "processing.attachments.max-size-bytes", attachments.maxSizeBytes());
        positive(e, "processing.attachments.orphan-age-seconds", attachments.orphanAgeSeconds());
        positive(e, "processing.attachments.cleanup-batch", attachments.cleanupBatch());
        // The reaper must not reclaim an upload whose presigned PUT window is still open.
        atLeast(e, "processing.attachments.orphan-age-seconds", attachments.orphanAgeSeconds(),
                "minio.upload-ttl-seconds", minio.uploadTtlSeconds());
    }

    private void validateMinio(List<String> e) {
        blank(e, "minio.endpoint", minio.endpoint());
        blank(e, "minio.bucket", minio.bucket());
        positive(e, "minio.upload-ttl-seconds", minio.uploadTtlSeconds());
        positive(e, "minio.download-ttl-seconds", minio.downloadTtlSeconds());
    }

    private void validateSession(List<String> e) {
        positive(e, "processing.session.idle-timeout-ms", session.idleTimeoutMs());
        positive(e, "processing.session.max-pending", session.maxPending());
        positive(e, "processing.session.overload-kick-batch", session.overloadKickBatch());
    }

    private void validateRetention(List<String> e) {
        positive(e, "processing.retention.history-days", retention.historyDays());
        positive(e, "processing.retention.frozen-days", retention.frozenDays());
        atLeast(e, "processing.retention.frozen-days", retention.frozenDays(),
                "processing.retention.history-days", retention.historyDays());
    }

    private void validateDeadLetter(List<String> e) {
        positive(e, "processing.deadletter.cleanup-batch", deadLetter.cleanupBatch());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void positive(List<String> e, String key, long v) {
        if (v <= 0) e.add(key + " must be > 0, was " + v);
    }

    /** {@code value >= floor} (a max/cap must not sit below its own floor / a related minimum). */
    private void atLeast(List<String> e, String key, long value, String floorKey, long floor) {
        if (value < floor) e.add(key + " (" + value + ") must be ≥ " + floorKey + " (" + floor + ")");
    }

    /** {@code value <= ceiling} (batch concurrency must fit the resource it draws from). */
    private void atMost(List<String> e, String key, long value, String ceilKey, long ceiling) {
        if (value > ceiling) e.add(key + " (" + value + ") must be ≤ " + ceilKey + " (" + ceiling + ")");
    }

    private void blank(List<String> e, String key, String v) {
        if (v == null || v.isBlank()) e.add(key + " must not be blank");
    }

    private void require(List<String> e, boolean ok, String message) {
        if (!ok) e.add(message);
    }
}
