package org.hormigas.ws.core.backpressure;

public interface SimplePublisher<T> {
    void publish(T msg);
}
