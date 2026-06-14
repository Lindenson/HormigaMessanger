package org.hormigas.ws.core.credits;

public interface Credits {
    boolean tryConsume();
    double getCurrentCredits();
    void reset();
}
