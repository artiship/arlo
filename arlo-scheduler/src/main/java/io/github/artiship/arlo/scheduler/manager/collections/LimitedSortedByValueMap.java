package io.github.artiship.arlo.scheduler.manager.collections;

import com.google.common.base.Functions;
import com.google.common.collect.Ordering;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LimitedSortedByValueMap<K extends Comparable<K>, V> extends LimitedSortedMap<K, V> {
    private final Map<K, V> valueMap;

    public LimitedSortedByValueMap(final Ordering<? super V> partialValueOrdering, final int size) {
        this(partialValueOrdering, new ConcurrentHashMap<>(), size);
    }

    private LimitedSortedByValueMap(Ordering<? super V> partialValueOrdering, Map<K, V> valueMap, int size) {
        super(partialValueOrdering.onResultOf(Functions.forMap(valueMap))
                                  .compound(Ordering.natural()), size);
        this.valueMap = valueMap;
    }

    @Override
    public V put(K k, V v) {
        if (valueMap.containsKey(k)) {
            remove(k);
        }
        valueMap.put(k, v);
        return super.put(k, v);
    }

    @Override
    public V get(Object key) {
        return valueMap.get(key);
    }

    @Override
    public V remove(Object o) {
        if (!valueMap.containsKey(o)) return null;

        V removed = super.remove(o);
        if (removed != null) {
            this.valueMap.remove(o);
            return removed;
        }

        return null;
    }

    public int valueMapSize() {
        return valueMap.size();
    }
}
