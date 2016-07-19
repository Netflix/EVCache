package com.netflix.evcache.util;

import com.netflix.evcache.pool.ServerGroup;

import java.util.Iterator;
import java.util.Set;

/**
 * A circular iterator for ReplicaSets. This ensures that all ReplicaSets are
 * equal number of requests.
 * 
 * @author smadappa
 */
public class ServerGroupCircularIterator {
    private Entry<ServerGroup> entry;
    private int size = 0;

    /**
     * Creates an instance of ReplicaSetCircularIterator across all ReplicaSets.
     * 
     * @param allReplicaSets
     *            Set of all available ReplicaSets.
     */
    public ServerGroupCircularIterator(Set<ServerGroup> allReplicaSets) {
        if (allReplicaSets == null || allReplicaSets.isEmpty()) return;
        Entry<ServerGroup> pEntry = null;
        for (Iterator<ServerGroup> itr = allReplicaSets.iterator(); itr.hasNext();) {
            size++;
            final ServerGroup rSet = itr.next();
            final Entry<ServerGroup> newEntry = new Entry<ServerGroup>(rSet, pEntry);
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
    public ServerGroup next() {
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
    public ServerGroup next(ServerGroup ignoreReplicaSet) {
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
            Entry<ServerGroup> startEntry = entry;
            current.append(startEntry.element);
            while (!entry.next.equals(startEntry)) {
                current.append(",").append(entry.next.element);
                entry = entry.next;
            }
        }
        return "Server Group Iterator : { size=" + getSize() + "; Server Group=" + current.toString() + "}";
    }

}
