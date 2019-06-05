package org.kin.framework.collection;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Created by huangjianqin on 2017/10/28.
 */
public class ConcurrentHashSet<E> extends AbstractSet<E> {
    private ConcurrentHashMap<E, Boolean> items = new ConcurrentHashMap<>();

    @Override
    public Iterator<E> iterator() {
        return items.keySet().iterator();
    }

    @Override
    public int size() {
        return items.size();
    }

    @Override
    public boolean contains(Object o) {
        return items.containsKey(o);
    }

    @Override
    public Object[] toArray() {
        return items.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return items.keySet().toArray(a);
    }

    @Override
    public boolean add(E e) {
        return items.put(e, Boolean.TRUE);
    }

    @Override
    public boolean remove(Object o) {
        return items.remove(o);
    }

    @Override
    public void clear() {
        items.clear();
    }

    @Override
    public String toString() {
        return items.keySet().toString();
    }

    @Override
    public Spliterator<E> spliterator() {
        return items.keySet().spliterator();
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        return items.keySet().removeIf(filter);
    }

    @Override
    public Stream<E> stream() {
        return items.keySet().stream();
    }

    @Override
    public Stream<E> parallelStream() {
        return items.keySet().parallelStream();
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        items.keySet().forEach(action);
    }

    /**
     * 浅复制
     */
    @Override
    public ConcurrentHashSet<E> clone() throws CloneNotSupportedException {
        ConcurrentHashSet<E> cloned = new ConcurrentHashSet<>();
        cloned.addAll(items.keySet());
        return cloned;
    }
}
