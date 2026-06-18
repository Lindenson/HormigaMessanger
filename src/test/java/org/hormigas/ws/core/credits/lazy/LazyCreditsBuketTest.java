package org.hormigas.ws.core.credits.lazy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LazyCreditsBuket — token bucket consume / refill / reset")
class LazyCreditsBuketTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 3, 10})
    @DisplayName("a fresh bucket starts full at its max capacity")
    void startsFull(int max) {
        LazyCreditsBuket bucket = new LazyCreditsBuket(max, 0.0);
        assertEquals((double) max, bucket.getCurrentCredits());
    }

    @Test
    @DisplayName("consuming draws down to empty, then refuses (no refill at rate 0)")
    void consumeUntilEmpty() {
        LazyCreditsBuket bucket = new LazyCreditsBuket(2, 0.0);
        assertTrue(bucket.tryConsume());
        assertTrue(bucket.tryConsume());
        assertFalse(bucket.tryConsume());
    }

    @Test
    @DisplayName("reset restores the bucket to full")
    void resetRestoresFull() {
        LazyCreditsBuket bucket = new LazyCreditsBuket(2, 0.0);
        bucket.tryConsume();
        bucket.tryConsume();
        bucket.reset();
        assertEquals(2.0, bucket.getCurrentCredits());
    }

    @Test
    @DisplayName("credits refill over time but never exceed the cap")
    void refillsCappedAtMax() throws InterruptedException {
        LazyCreditsBuket bucket = new LazyCreditsBuket(2, 100_000.0); // huge rate
        bucket.tryConsume();
        bucket.tryConsume();
        Thread.sleep(20); // 100k/s over 20ms refills far past the cap
        assertEquals(2.0, bucket.getCurrentCredits());
    }
}
