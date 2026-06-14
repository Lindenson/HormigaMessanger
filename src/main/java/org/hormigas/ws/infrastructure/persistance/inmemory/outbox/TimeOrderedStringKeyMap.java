package org.hormigas.ws.infrastructure.persistance.inmemory.outbox;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * Потокобезопасная структура:
 * хранит элементы (ключ String → значение V), отсортированные по clientTimestamp (по возрастанию).
 * <p>
 * Предназначена для последовательного чтения блоками (от самых старых к новым).
 */
@Slf4j
public class TimeOrderedStringKeyMap<V> {

    private final ConcurrentHashMap<String, V> map = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, EntryKey> index = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<EntryKey> order = new ConcurrentSkipListSet<>();
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Функциональный интерфейс для получения connectedAt из значения
     */
    @FunctionalInterface
    public interface TimestampExtractor<V> {
        long getTimestamp(V value);
    }

    private final TimestampExtractor<V> extractor;

    public TimeOrderedStringKeyMap(TimestampExtractor<V> extractor) {
        this.extractor = Objects.requireNonNull(extractor, "extractor cannot be null");
    }

    /**
     * Внутренний ключ — connectedAt + id (уникальная комбинация, сортируется по возрастанию времени).
     */
    private static final class EntryKey implements Comparable<EntryKey> {
        final long ts;
        final String id;

        EntryKey(long ts, String id) {
            this.ts = ts;
            this.id = id;
        }

        @Override
        public int compareTo(EntryKey o) {
            int cmp = Long.compare(this.ts, o.ts);
            return (cmp != 0) ? cmp : this.id.compareTo(o.id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof EntryKey)) return false;
            EntryKey other = (EntryKey) o;
            return ts == other.ts && id.equals(other.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ts, id);
        }
    }

    /**
     * Добавление, если отсутствует
     */
    public V putIfAbsent(String key, V value) {
        if (key == null || value == null) return null;
        lock.lock();
        try {
            if (map.containsKey(key)) {
                return map.get(key);
            }
            map.put(key, value);
            EntryKey ek = new EntryKey(extractor.getTimestamp(value), key);
            index.put(key, ek);
            order.add(ek);
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Добавление с заменой существующего значения
     */
    public V putOrReplace(String key, V value) {
        if (key == null || value == null) return null;
        lock.lock();
        try {
            V old = map.put(key, value);
            EntryKey oldKey = index.remove(key);
            if (oldKey != null) order.remove(oldKey);

            EntryKey newKey = new EntryKey(extractor.getTimestamp(value), key);
            index.put(key, newKey);
            order.add(newKey);
            return old;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Удаление по ключу
     */
    public V remove(String key) {
        if (key == null) return null;
        lock.lock();
        try {
            V removed = map.remove(key);
            EntryKey ek = index.remove(key);
            if (ek != null) order.remove(ek);
            return removed;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Получение по ключу
     */
    public V get(String key) {
        if (key == null) return null;
        return map.get(key);
    }

    /**
     * Количество элементов
     */
    public int size() {
        return map.size();
    }

    /**
     * Проверка на пустоту
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Первые N сообщений (по возрастанию connectedAt).
     * Возвращает список от старых к новым (FIFO).
     */
    public List<V> peekFirstN(int n) {
        if (n <= 0) return Collections.emptyList();
        lock.lock();
        try {
            List<V> result = new ArrayList<>(Math.min(n, order.size()));
            Iterator<EntryKey> it = order.iterator(); // порядок: старые → новые
            while (it.hasNext() && result.size() < n) {
                EntryKey ek = it.next();
                V val = map.get(ek.id);
                if (val != null) result.add(val);
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Первый (самый старый) элемент без удаления
     */
    public V peekFirst() {
        EntryKey first = order.isEmpty() ? null : order.first();
        return (first != null) ? map.get(first.id) : null;
    }

    /**
     * Последний (самый новый) элемент без удаления
     */
    public V peekLast() {
        EntryKey last = order.isEmpty() ? null : order.last();
        return (last != null) ? map.get(last.id) : null;
    }

    /**
     * Удаление первых N элементов и возврат их
     */
    public List<V> pollFirstN(int n) {
        if (n <= 0) return Collections.emptyList();
        lock.lock();
        try {
            List<V> result = new ArrayList<>(Math.min(n, order.size()));
            Iterator<EntryKey> it = order.iterator();
            while (it.hasNext() && result.size() < n) {
                EntryKey ek = it.next();
                it.remove();
                index.remove(ek.id);
                V val = map.remove(ek.id);
                if (val != null) result.add(val);
            }
            return result;
        } finally {
            lock.unlock();
        }
    }


    public int collectGarbageOptimized(Predicate<V> toBeRemoved) {
        int collected = 0;
        List<String> toRemove = map.entrySet().stream().filter(it -> toBeRemoved.test(it.getValue()))
                .map(Map.Entry::getKey).toList();
        if (!toRemove.isEmpty()) {
            lock.lock();
            try {
                for (String ek : toRemove) {
                    remove(ek);
                    collected++;
                }
            } finally {
                lock.unlock();
            }
        }
        return collected;
    }
}
