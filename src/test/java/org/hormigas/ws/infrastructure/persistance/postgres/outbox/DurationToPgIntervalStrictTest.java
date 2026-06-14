package org.hormigas.ws.infrastructure.persistance.postgres.outbox;

import io.vertx.pgclient.data.Interval;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hormigas.ws.infrastructure.persistance.postgres.outbox.OutboxPostgresRepository.durationToPgIntervalStrict;
import static org.junit.jupiter.api.Assertions.*;

public class DurationToPgIntervalStrictTest {

    @Test
    public void testNullThrows() {
        assertThrows(IllegalArgumentException.class, () -> durationToPgIntervalStrict(null));
    }

    @Test
    public void testNegativeDurationBecomes1Second() {
        Interval i = durationToPgIntervalStrict(Duration.ofSeconds(-5));
        assertEquals(1, i.getSeconds());
        assertEquals(0, i.getMinutes());
        assertEquals(0, i.getHours());
    }

    @Test
    public void testZeroDurationBecomes1Second() {
        Interval i = durationToPgIntervalStrict(Duration.ZERO);
        assertEquals(1, i.getSeconds());
        assertEquals(0, i.getMinutes());
        assertEquals(0, i.getHours());
    }

    @Test
    public void testLessThan1SecondBecomes1Second() {
        Interval i = durationToPgIntervalStrict(Duration.ofMillis(500));
        assertEquals(1, i.getSeconds());
        assertEquals(0, i.getMinutes());
        assertEquals(0, i.getHours());
    }

    @Test
    public void testMoreThan24HoursThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> durationToPgIntervalStrict(Duration.ofHours(25)));
    }

    @Test
    public void testExactly24HoursIsAllowed() {
        Interval i = durationToPgIntervalStrict(Duration.ofHours(24));
        long totalSeconds = i.toDuration().getSeconds();
        assertEquals(24 * 3600, totalSeconds, "Interval should be exactly 24 hours");
    }

    @Test
    public void testOneSecond() {
        Interval i = durationToPgIntervalStrict(Duration.ofSeconds(1));
        assertEquals(1, i.getSeconds());
    }

    @Test
    public void testNormalDuration() {
        Interval i = durationToPgIntervalStrict(Duration.ofMinutes(2).plusSeconds(30));
        assertEquals(0, i.getHours());
        assertEquals(2, i.getMinutes());
        assertEquals(30, i.getSeconds());
    }
}
