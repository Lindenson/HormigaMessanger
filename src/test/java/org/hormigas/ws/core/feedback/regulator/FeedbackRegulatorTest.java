package org.hormigas.ws.core.feedback.regulator;

import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.core.feedback.events.OutgoingHealthEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("FeedbackRegulator — adaptive poll interval from health events")
class FeedbackRegulatorTest {

    private FeedbackRegulator regulator(int baseMs, double growth, double decay, int maxMs) {
        MessengerConfig config = mock(MessengerConfig.class);
        MessengerConfig.Feedback fb = mock(MessengerConfig.Feedback.class);
        when(config.feedback()).thenReturn(fb);
        when(fb.additionalMs()).thenReturn(baseMs);
        when(fb.adjustmentFactor()).thenReturn(growth);   // growth factor (>1)
        when(fb.recoveryFactor()).thenReturn(decay);      // decay factor (<1)
        when(fb.maxMs()).thenReturn(maxMs);
        FeedbackRegulator r = new FeedbackRegulator();
        r.messengerConfig = config;
        r.init();
        return r;
    }

    @Test
    @DisplayName("starts at the configured base interval")
    void startsAtBase() {
        assertEquals(Duration.ofMillis(1000), regulator(1000, 2.0, 0.5, 30000).getCurrentIntervalMs());
    }

    @Test
    @DisplayName("a drop event SLOWS the poller — grows the interval by the growth factor")
    void dropGrowsInterval() {
        FeedbackRegulator r = regulator(1000, 2.0, 0.5, 30000);
        r.onHealthEvent(new OutgoingHealthEvent(true, 7));
        assertEquals(Duration.ofMillis(2000), r.getCurrentIntervalMs());
    }

    @Test
    @DisplayName("the grown interval is capped at maxMs")
    void growthCappedAtMax() {
        FeedbackRegulator r = regulator(1000, 2.0, 0.5, 2500);
        r.onHealthEvent(new OutgoingHealthEvent(true, 7)); // 1000 -> 2000
        r.onHealthEvent(new OutgoingHealthEvent(true, 7)); // min(2500, 4000) -> 2500
        assertEquals(Duration.ofMillis(2500), r.getCurrentIntervalMs());
    }

    @Test
    @DisplayName("a stable event eases the interval back DOWN toward the base (never below it)")
    void stableDecaysToBase() {
        FeedbackRegulator r = regulator(1000, 2.0, 0.5, 30000);
        r.onHealthEvent(new OutgoingHealthEvent(true, 7));   // 1000 -> 2000
        r.onHealthEvent(new OutgoingHealthEvent(false, 0));  // max(1000, 2000*0.5) -> 1000
        assertEquals(Duration.ofMillis(1000), r.getCurrentIntervalMs());
    }
}
