package org.hormigas.ws.core.credits.lazy;

import org.hormigas.ws.core.credits.Credits;

import java.util.concurrent.atomic.AtomicLong;

public class LazyCreditsBuket implements Credits {

    private final int maxCredits;
    private final double refillRatePerSecond;

    private final AtomicLong lastRefillTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong creditsBits = new AtomicLong(Double.doubleToRawLongBits(0.0));

    public LazyCreditsBuket(int maxCredits, double refillRatePerSecond) {
        this.maxCredits = maxCredits;
        this.refillRatePerSecond = refillRatePerSecond;
        this.creditsBits.set(Double.doubleToRawLongBits(maxCredits));
    }

    private void refillIfNeeded() {
        long now = System.currentTimeMillis();
        long last = lastRefillTime.get();
        if (now <= last) return;

        double elapsed = (now - last) / 1000.0;
        if (elapsed <= 0) return;
        double oldVal, newVal;
        do {
            oldVal = Double.longBitsToDouble(creditsBits.get());
            newVal = Math.min(maxCredits, oldVal + elapsed * refillRatePerSecond);
        } while (!creditsBits.compareAndSet(
                Double.doubleToRawLongBits(oldVal),
                Double.doubleToRawLongBits(newVal)
        ));

        lastRefillTime.set(now);
    }

    @Override
    public boolean tryConsume() {
        refillIfNeeded();

        double oldVal, newVal;
        do {
            oldVal = Double.longBitsToDouble(creditsBits.get());
            if (oldVal < 1.0) return false;
            newVal = oldVal - 1.0;
        } while (!creditsBits.compareAndSet(
                Double.doubleToRawLongBits(oldVal),
                Double.doubleToRawLongBits(newVal)
        ));

        return true;
    }

    @Override
    public double getCurrentCredits() {
        refillIfNeeded();
        return Double.longBitsToDouble(creditsBits.get());
    }

    @Override
    public void reset() {
        creditsBits.set(Double.doubleToRawLongBits(maxCredits));
        lastRefillTime.set(System.currentTimeMillis());
    }
}
