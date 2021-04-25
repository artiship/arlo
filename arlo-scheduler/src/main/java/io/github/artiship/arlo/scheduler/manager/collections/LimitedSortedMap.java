package io.github.artiship.arlo.scheduler.manager.collections;

import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.copyOf;

public class LimitedSortedMap<K, V> extends TreeMap<K, V> {

    private final int maxSize;

    public LimitedSortedMap(Comparator<? super K> comparator, int maxSize) {
        super(comparator);
        this.maxSize = maxSize;
    }

    public LimitedSortedMap(int maxSize) {
        checkArgument(maxSize > 0, "Limited size should > 1");
        this.maxSize = maxSize;
    }

    @Override
    public V put(K k, V v) {
        V put = super.put(k, v);

        shrink();

        return put;
    }

    private void shrink() {
        if (size() <= maxSize) return;

        K firstToRemove = (K) this.keySet()
                                  .toArray()[size() - maxSize];

        SortedMap<K, V> toDeletes = headMap(firstToRemove);

        copyOf(toDeletes.keySet()).forEach(this::remove);
    }
}
