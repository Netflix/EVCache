package com.netflix.evcache.util;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

/**
 * A circular iterator for ReplicaSets. This ensures that all ReplicaSets are
 * equal number of requests.
 * 
 * @author smadappa
 */
public class CircularIterator<T> {
    private Entry<T> entry;
    private int size = 0;

    /**
     * Creates an instance of ReplicaSetCircularIterator across all ReplicaSets.
     * 
     * @param allReplicaSets
     *            Set of all available ReplicaSets.
     */
    public CircularIterator(Collection<T> allReplicaSets) {
        if (allReplicaSets == null || allReplicaSets.isEmpty()) return;
        Entry<T> pEntry = null;
        for (Iterator<T> itr = allReplicaSets.iterator(); itr.hasNext();) {
            size++;
            final T rSet = itr.next();
            final Entry<T> newEntry = new Entry<T>(rSet, pEntry);
            if (entry == null) entry = newEntry;
            pEntry = newEntry;
        }

        /*
         * Connect the first and the last entry to form a circular list
         */
        if (pEntry != null) {
            entry.next = pEntry;
        }
    }

    /**
     * Returns the next ReplicaSet which should get the request.
     * 
     * @return - the next ReplicaSetCircularIterator in the iterator. If there
     *         are none then null is returned.
     */
    public T next() {
        if (entry == null) return null;
        entry = entry.next;
        return entry.element;
    }

    /**
     * Returns the next ReplicaSet excluding the given ReplicaSet which should
     * get the request.
     * 
     * @return - the next ReplicaSet in the iterator. If there are none then
     *         null is returned.
     */
    public T next(T ignoreReplicaSet) {
        if (entry == null) return null;
        entry = entry.next;
        if (entry.element.equals(ignoreReplicaSet)) {
            return entry.next.element;
        } else {
            return entry.element;
        }
    }

    public int getSize() {
        return size;
    }

    /**
     * The Entry keeps track of the current element and next element in the
     * list.
     * 
     * @author smadappa
     *
     * @param <E>
     */
    static class Entry<E> {
        private E element;
        private Entry<E> next;

        /**
         * Creates an instance of Entry.
         */
        Entry(E element, Entry<E> next) {
            this.element = element;
            this.next = next;
        }
    }

    public String toString() {
        final StringBuilder current = new StringBuilder();
        if (entry != null) {
            Entry<T> startEntry = entry;
            if(startEntry.element.getClass().isArray()) {
                for(int i = 0; i < Array.getLength(startEntry.element); i++) {
                    if(i > 0) current.append(",");
                    current.append(Array.get(startEntry.element, i).toString());
                }
            } else {
                current.append(startEntry.element);
            }
            while (!entry.next.equals(startEntry)) {
                if(entry.next.element.getClass().isArray()) {
                    for(int i = 0; i < Array.getLength(entry.next.element); i++) {
                        if(i > 0) current.append(",");
                        current.append(Array.get(entry.next.element, i).toString());
                    }
                } else {
                    current.append(",").append(entry.next.element);
                }
                entry = entry.next;
            }
        }
        return "Server Group Iterator : { size=" + getSize() + "; Server Group=" + current.toString() + "}";
    }

}
