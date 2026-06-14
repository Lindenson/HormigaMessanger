package org.hormigas.ws.domain.session;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.hormigas.ws.core.credits.Credits;

import java.util.concurrent.atomic.AtomicLong;


@Builder
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class ClientSession<T> {
    private final String clientId;
    private final String clientName;
    private final Credits credits;
    private final long connectedAt = System.currentTimeMillis();
    @Builder.Default
    private long lastActiveAt = System.currentTimeMillis();
    @Builder.Default
    private AtomicLong sequenceCurrentNumber =  new AtomicLong(0);
    @EqualsAndHashCode.Include
    private final T session;

    public boolean tryConsumeCredits() {
        return credits.tryConsume();
    }
    public double getAvailableCredits() {return credits.getCurrentCredits();}
    public void updateActivity() {
        this.lastActiveAt = System.currentTimeMillis();
    }
    public void updateSequence() {
        this.sequenceCurrentNumber.incrementAndGet();
    }
}
