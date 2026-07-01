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
