package org.hormigas.ws.core.feedback.provider;

public interface OutEventProvider<T> {
    void fireOut(T event);
}

