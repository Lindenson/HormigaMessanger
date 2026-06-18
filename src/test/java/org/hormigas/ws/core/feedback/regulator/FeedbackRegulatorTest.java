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

    private FeedbackRegulator regulator(int baseMs, double adjustment, double recovery) {
        MessengerConfig config = mock(MessengerConfig.class);
        MessengerConfig.Feedback fb = mock(MessengerConfig.Feedback.class);
        when(config.feedback()).thenReturn(fb);
        when(fb.additionalMs()).thenReturn(baseMs);
        when(fb.adjustmentFactor()).thenReturn(adjustment);
        when(fb.recoveryFactor()).thenReturn(recovery);
        FeedbackRegulator r = new FeedbackRegulator();
        r.messengerConfig = config;
        r.init();
        return r;
    }

    @Test
    @DisplayName("starts at the configured base interval")
    void startsAtBase() {
        assertEquals(Duration.ofMillis(1000), regulator(1000, 0.5, 2.0).getCurrentIntervalMs());
    }

    @Test
    @DisplayName("a drop event adjusts the interval by the adjustment factor")
    void dropAdjustsInterval() {
        FeedbackRegulator r = regulator(1000, 0.5, 2.0);
        r.onHealthEvent(new OutgoingHealthEvent(true, 7));
        assertEquals(Duration.ofMillis(500), r.getCurrentIntervalMs());
    }

    @Test
    @DisplayName("the adjusted interval never drops below the 100ms floor")
    void adjustmentHasFloor() {
        FeedbackRegulator r = regulator(1000, 0.01, 2.0);
        r.onHealthEvent(new OutgoingHealthEvent(true, 7));
        assertEquals(Duration.ofMillis(100), r.getCurrentIntervalMs());
    }

    @Test
    @DisplayName("a stable event recovers the interval, capped at the base")
    void recoveryCapsAtBase() {
        FeedbackRegulator r = regulator(1000, 0.5, 2.0);
        r.onHealthEvent(new OutgoingHealthEvent(true, 7));   // 1000 -> 500
        r.onHealthEvent(new OutgoingHealthEvent(false, 0));  // min(1000, 500*2.0) -> 1000
        assertEquals(Duration.ofMillis(1000), r.getCurrentIntervalMs());
    }
}
