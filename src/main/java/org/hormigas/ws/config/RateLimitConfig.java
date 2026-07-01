package org.hormigas.ws.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.Map;

/**
 * Cross-cutting REST rate limiting (per caller × endpoint-group). Every {@code /api/*} endpoint is
 * limited by the {@code default-limit}; an endpoint (or resource) annotated {@code @RateLimit("group")}
 * uses that group's limit instead — e.g. attachments strictest, list-chats looser. Token-bucket per
 * (group, caller). Single-instance (in-memory); a distributed Redis limiter is a documented future.
 */
@ConfigMapping(prefix = "processing.rate-limit")
public interface RateLimitConfig {

    @WithDefault("true")
    boolean enabled();

    /** Max distinct (group, caller) buckets held in memory. Caps the unbounded-caller footprint; ↑ if you
     *  have many concurrent callers, ↓ to bound memory. Idle buckets are evicted after {@link #bucketIdleMinutes()}. */
    @WithDefault("200000")
    long bucketCacheMax();

    /** Idle time after which a caller's bucket is evicted (its state resets). ↑ preserves limits longer; ↓ frees memory sooner. */
    @WithDefault("10")
    int bucketIdleMinutes();

    /** Applied to any endpoint without a {@code @RateLimit} group. */
    Limit defaultLimit();

    /** Per-group overrides, keyed by the {@code @RateLimit} value. Absent group → {@link #defaultLimit()}. */
    Map<String, Limit> groups();

    interface Limit {
        /** Sustained permits per second (bucket refill rate). */
        @WithDefault("20")
        double permitsPerSecond();

        /** Burst allowance (bucket capacity). */
        @WithDefault("40")
        int burst();
    }
}
