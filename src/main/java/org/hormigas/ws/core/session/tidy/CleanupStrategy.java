package org.hormigas.ws.core.session.tidy;

import java.util.Set;
import java.util.function.Consumer;

public interface CleanupStrategy<T> {
    boolean clean(T tested, Set<T> opened, Consumer<T> deregister);
}