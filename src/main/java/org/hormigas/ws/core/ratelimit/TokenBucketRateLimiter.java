package org.hormigas.ws.core.ratelimit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hormigas.ws.config.RateLimitConfig;

import java.time.Duration;

/**
 * In-memory token-bucket {@link RateLimiter}, keyed by {@code (group, caller)}. Each key gets a bucket
 * sized by its group's config (sustained {@code permitsPerSecond} refill, {@code burst} capacity);
 * buckets are held in a Caffeine cache that evicts idle keys. Single-instance correct — for multiple
 * instances a distributed limiter (Redis) drops in behind {@link RateLimiter} (documented future,
 * mirroring the L1 conversation cache).
 */
@ApplicationScoped
public class TokenBucketRateLimiter implements RateLimiter {

    @Inject
    RateLimitConfig config;

    private Cache<String, TokenBucket> buckets;

    @PostConstruct
    void init() {
        buckets = Caffeine.newBuilder()
                .expireAfterAccess(Duration.ofMinutes(10))
                .maximumSize(200_000)
                .build();
    }

    @Override
    public boolean tryAcquire(String group, String caller) {
        if (!config.enabled()) {
            return true;
        }
        RateLimitConfig.Limit limit = config.groups().getOrDefault(group, config.defaultLimit());
        TokenBucket bucket = buckets.get(group + ":" + caller,
                k -> new TokenBucket(limit.permitsPerSecond(), limit.burst()));
        return bucket.tryConsume();
    }

    /** A refill token bucket. Thread-safe per instance. */
    static final class TokenBucket {
        private final double ratePerSec;
        private final double capacity;
        private double tokens;
        private long lastNanos;

        TokenBucket(double ratePerSec, double capacity) {
            this.ratePerSec = ratePerSec;
            this.capacity = capacity;
            this.tokens = capacity;
            this.lastNanos = System.nanoTime();
        }

        synchronized boolean tryConsume() {
            long now = System.nanoTime();
            double elapsedSec = (now - lastNanos) / 1_000_000_000.0;
            tokens = Math.min(capacity, tokens + elapsedSec * ratePerSec);
            lastNanos = now;
            if (tokens >= 1.0) {
                tokens -= 1.0;
                return true;
            }
            return false;
        }
    }
}
