package org.hormigas.ws.core.session.tidy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
@RequiredArgsConstructor
public class FractionalCleanupStrategy<T> implements CleanupStrategy<T> {

    private static final double CLEAN_FRACTION = 0.33;
    private static final int CLEAN_THRESHOLD = 20;

    private final Function<T, Set<T>> introspector;
    private final Predicate<T> tester;

    @Override
    public boolean clean(T tested, Set<T> active, Consumer<T> deregister) {
        int openSize = introspector.apply(tested).size();
        if (Math.abs(openSize - active.size()) < CLEAN_THRESHOLD) return false;

        List<T> list = active.stream().toList();
        int n = (int) (list.size() * CLEAN_FRACTION);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < n; i++) {
            T conn = list.get(rnd.nextInt(list.size()));
            if (!tester.test(conn)) {
                deregister.accept(conn);
                log.debug("Pruned inactive connection {}", i);
            }
        }
        return true;
    }
}
