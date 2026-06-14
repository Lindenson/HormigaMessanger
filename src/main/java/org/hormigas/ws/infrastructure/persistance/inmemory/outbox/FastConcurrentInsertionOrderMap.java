package org.hormigas.ws.infrastructure.persistance.inmemory.outbox;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class FastConcurrentInsertionOrderMap<K, V> {

    private static class Node<K> {
        final K key;
        Node<K> prev;
        Node<K> next;
        Node(K key) { this.key = key; }
    }

    private final ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<K, Node<K>> nodeIndex = new ConcurrentHashMap<>();
    private Node<K> head;
    private Node<K> tail;
    private final ReentrantLock lock = new ReentrantLock();

    public V put(K key, V value) {
        if (key == null || value == null) return null;
        V prev = map.putIfAbsent(key, value);
        if (prev == null) {
            lock.lock();
            try {
                Node<K> node = new Node<>(key);
                nodeIndex.put(key, node);
                if (tail == null) {
                    head = tail = node;
                } else {
                    tail.next = node;
                    node.prev = tail;
                    tail = node;
                }
            } finally {
                lock.unlock();
            }
        }
        return prev;
    }

    public V get(K key) {
        if (key == null) return null;
        return map.get(key);
    }

    public V remove(K key) {
        if (key == null) return null;

        lock.lock();
        try {
            Node<K> node = nodeIndex.remove(key);
            if (node == null) return null;

            // удалить из связного списка
            if (node.prev != null) node.prev.next = node.next;
            else head = node.next;
            if (node.next != null) node.next.prev = node.prev;
            else tail = node.prev;

            return map.remove(key);
        } finally {
            lock.unlock();
        }
    }

    /** Первый элемент (FIFO) без удаления */
    public V peekFirst() {
        Node<K> h = head;
        return (h != null) ? map.get(h.key) : null;
    }

    /** Последний элемент (LIFO) без удаления */
    public V peekLast() {
        Node<K> t = tail;
        return (t != null) ? map.get(t.key) : null;
    }

    /** Получить последние N элементов без удаления (с конца в прямом порядке) */
    public List<V> peekLastN(int n) {
        lock.lock();
        try {
            List<V> result = new ArrayList<>(Math.min(n, map.size()));
            Node<K> node = tail;
            while (node != null && result.size() < n) {
                V val = map.get(node.key);
                if (val != null) result.add(0, val); // добавляем в начало, чтобы сохранить FIFO порядок
                node = node.prev;
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return head == null;
    }
}
