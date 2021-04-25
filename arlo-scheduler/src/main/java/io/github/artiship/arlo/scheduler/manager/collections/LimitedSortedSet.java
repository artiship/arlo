package io.github.artiship.arlo.scheduler.manager.collections;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;

public class LimitedSortedSet<E> extends TreeSet<E> {

    private int maxSize;

    public LimitedSortedSet(int maxSize) {
        checkArgument(maxSize > 0, "Limited size should > 1");
        this.maxSize = maxSize;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean added = super.addAll(c);
        if (size() > maxSize) {
            E firstToRemove = (E) toArray()[size() - maxSize];
            shrink(firstToRemove);
        }
        return added;
    }

    @Override
    public boolean add(E o) {
        boolean added = super.add(o);
        if (size() > maxSize) {
            E firstToRemove = (E) toArray()[size() - maxSize];
            shrink(firstToRemove);
        }
        return added;
    }

    private void shrink(E firstToRemove) {
        SortedSet<E> toDelete = headSet(firstToRemove);
        removeAll(toDelete);

        if (toDelete != null) {
            toDelete.forEach(i -> i = null);
        }
    }
}
