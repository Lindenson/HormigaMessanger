package org.hormigas.ws.core.ratelimit;

/**
 * Rate-limit gate (driving interface). The REST filter depends on this abstraction, not the concrete
 * limiter — a distributed (Redis) implementation can drop in behind it later. Implemented by
 * {@link TokenBucketRateLimiter}.
 */
public interface RateLimiter {

    /** True if a permit was granted for {@code (group, caller)}; false if the bucket is empty (limited). */
    boolean tryAcquire(String group, String caller);
}
