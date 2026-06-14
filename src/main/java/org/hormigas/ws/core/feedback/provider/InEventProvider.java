package org.hormigas.ws.core.feedback.provider;

public interface InEventProvider<T> {
    void fireIn(T event);
}
