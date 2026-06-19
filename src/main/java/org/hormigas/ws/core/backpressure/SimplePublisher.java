package org.hormigas.ws.core.backpressure;

public interface SimplePublisher<T> {
    /** Publish for processing. Returns {@code true} if accepted, {@code false} if dropped
     *  (not ready / queue full) — callers that must not lose the message react on {@code false}. */
    boolean publish(T msg);
}
