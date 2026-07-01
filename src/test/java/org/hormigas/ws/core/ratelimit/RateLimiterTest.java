package org.hormigas.ws.core.ratelimit;

import org.hormigas.ws.config.RateLimitConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("RateLimiter — token bucket per (group, caller): burst, refill, isolation, group overrides")
class RateLimiterTest {

    private RateLimiter limiter(boolean enabled, RateLimitConfig.Limit def, Map<String, RateLimitConfig.Limit> groups) {
        RateLimitConfig cfg = mock(RateLimitConfig.class);
        when(cfg.enabled()).thenReturn(enabled);
        when(cfg.defaultLimit()).thenReturn(def);
        when(cfg.groups()).thenReturn(groups);
        TokenBucketRateLimiter r = new TokenBucketRateLimiter();
        r.config = cfg;
        r.init();
        return r;
    }

    private RateLimitConfig.Limit limit(double rate, int burst) {
        RateLimitConfig.Limit l = mock(RateLimitConfig.Limit.class);
        when(l.permitsPerSecond()).thenReturn(rate);
        when(l.burst()).thenReturn(burst);
        return l;
    }

    @Test
    @DisplayName("allows up to the burst, then denies (no refill at rate 0)")
    void burstThenDeny() {
        RateLimiter r = limiter(true, limit(0, 3), Map.of());
        assertTrue(r.tryAcquire("default", "u1"));
        assertTrue(r.tryAcquire("default", "u1"));
        assertTrue(r.tryAcquire("default", "u1"));
        assertFalse(r.tryAcquire("default", "u1"), "4th over a burst of 3 is denied");
    }

    @Test
    @DisplayName("tokens refill over time")
    void refills() throws InterruptedException {
        RateLimiter r = limiter(true, limit(1000, 1), Map.of()); // 1000/s → ~1 token per ms
        assertTrue(r.tryAcquire("default", "u1"));
        assertFalse(r.tryAcquire("default", "u1"));
        Thread.sleep(20); // ≥ 20 tokens refilled (capped at burst 1)
        assertTrue(r.tryAcquire("default", "u1"), "a permit is available again after refill");
    }

    @Test
    @DisplayName("buckets are isolated per caller and per group")
    void isolation() {
        RateLimiter r = limiter(true, limit(0, 1), Map.of("strict", limit(0, 1)));
        assertTrue(r.tryAcquire("default", "alice"));
        assertFalse(r.tryAcquire("default", "alice"));
        assertTrue(r.tryAcquire("default", "bob"), "a different caller has its own bucket");
        assertTrue(r.tryAcquire("strict", "alice"), "a different group has its own bucket");
    }

    @Test
    @DisplayName("a group override applies its own (stricter) limit")
    void groupOverride() {
        RateLimiter r = limiter(true, limit(0, 5), Map.of("attachments", limit(0, 1)));
        assertTrue(r.tryAcquire("attachments", "u1"));
        assertFalse(r.tryAcquire("attachments", "u1"), "attachments burst is 1");
        // the default group still has its larger burst for the same caller
        assertTrue(r.tryAcquire("default", "u1"));
    }

    @Test
    @DisplayName("disabled → always allows")
    void disabled() {
        RateLimiter r = limiter(false, limit(0, 0), Map.of());
        for (int i = 0; i < 100; i++) assertTrue(r.tryAcquire("default", "u1"));
    }
}
