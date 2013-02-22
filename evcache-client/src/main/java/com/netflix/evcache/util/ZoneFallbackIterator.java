package com.netflix.evcache.util;

import java.util.Iterator;
import java.util.Set;

/**
 * A Zone Based fallback circular iterator.
 * This ensures that during a fallback scenario the requests are spread out across all zones evenly.
 * @author smadappa
 */
public class ZoneFallbackIterator {
    private Entry<String> entry;
    private int size = 0;

    /**
     * Creates an instance of ZoneFallbackIterator given all the zones.
     * @param allZones Set of all available zones.
     */
    public ZoneFallbackIterator(Set<String> allZones) {
        if (allZones == null || allZones.size() == 0) return;
        Entry<String> pEntry = null; 
        for (Iterator<String> itr = allZones.iterator(); itr.hasNext();) {
            final String zone = itr.next();
            final Entry<String> newEntry = new Entry<String>(zone, pEntry);
            if (entry == null) entry = newEntry;
            pEntry = newEntry;
        }
        
        /*
         * Connect the first and the last entry to form a circular list
         */
        if(pEntry != null) {
            entry.next = pEntry;
        }
    }

    /**
     * Returns the next zone from the set which should get the request.
     * @return - the next zone in the iterator. If there are none then null is returned.
     */
    public String next() {
        if (entry == null) return null;
        entry = entry.next;
        return entry.element;
    }

    /**
     * Returns the next zone from the set excluding the given zone which should get the request.
     * @return - the next zone in the iterator. If there are none then null is returned.
     */
    public String next(String ignoreZone) {
        if (entry == null) return null;
        entry = entry.next;
        if (entry.element.equals(ignoreZone)) {
            return entry.next.element;
        } else {
            return entry.element;
        }
    }

    public int getSize() {
        return size;
    }

    /**
     * The Entry keeps track of the current element and next element in the list.
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
}
