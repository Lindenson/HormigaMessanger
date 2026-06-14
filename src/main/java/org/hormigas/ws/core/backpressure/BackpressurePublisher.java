package org.hormigas.ws.core.backpressure;

public interface BackpressurePublisher<T> extends SimplePublisher<T> {
    boolean queueIsNotEmpty();
    boolean queueIsFull();
}
