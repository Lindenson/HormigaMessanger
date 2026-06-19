package org.hormigas.ws.core.feedback.regulator;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hormigas.ws.config.MessengerConfig;
import org.hormigas.ws.core.feedback.Regulator;
import org.hormigas.ws.core.feedback.events.OutgoingHealthEvent;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@ApplicationScoped
public class FeedbackRegulator implements Regulator {
    // Adaptive added-delay for the outbox poller. Correct backpressure direction (F4): on drops the
    // poller must SLOW DOWN (grow the interval, capped at maxIntervalMs); when stable it eases back
    // DOWN toward the base interval. Config: growthFactor > 1, decayFactor < 1.
    private long baseIntervalMs;
    private long maxIntervalMs;
    private double growthFactor;
    private double decayFactor;
    private final AtomicLong currentIntervalMs = new AtomicLong();

    @Inject
    MessengerConfig messengerConfig;

    @PostConstruct
    public void init() {
        growthFactor = messengerConfig.feedback().adjustmentFactor();
        baseIntervalMs = messengerConfig.feedback().additionalMs();
        decayFactor = messengerConfig.feedback().recoveryFactor();
        maxIntervalMs = messengerConfig.feedback().maxMs();
        currentIntervalMs.set(baseIntervalMs);
    }

    public void onHealthEvent(@ObservesAsync OutgoingHealthEvent event) {
        if (event.droppedDetected()) {
            // slow down — grow the interval, capped
            long newInterval = Math.min(maxIntervalMs, (long) (currentIntervalMs.get() * growthFactor));
            currentIntervalMs.set(newInterval);
            log.warn("⚠️ Drops detected — slowing outbox poll to {} ms", newInterval);
        } else {
            // ease back down toward the base interval
            long newInterval = Math.max(baseIntervalMs, (long) (currentIntervalMs.get() * decayFactor));
            currentIntervalMs.set(newInterval);
            log.debug("✅ Stable — easing outbox poll back to {} ms", newInterval);
        }
    }

    @Override
    public Duration getCurrentIntervalMs() {
        return Duration.ofMillis(currentIntervalMs.get());
    }
}
