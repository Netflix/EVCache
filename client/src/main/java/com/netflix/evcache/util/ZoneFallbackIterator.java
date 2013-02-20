package com.netflix.evcache.util;

import java.util.Iterator;
import java.util.Set;

/**
 * A Zone Based fallback iterator. This ensures that during a fallback scenario the requests are spread out across all zones evenly.
 * @author smadappa
 */
public class ZoneFallbackIterator {
    private Entry<String> entry;
    private int size = 0;

    /**
     * Creates an instance of ZoneFallbackIterator given all the zones.
     * @param allZones
     */
    public ZoneFallbackIterator(Set<String> allZones) {
        if (allZones == null || allZones.size() == 0) return;
        for (Iterator<String> itr = allZones.iterator(); itr.hasNext();) {
            final String zone = itr.next();
            final Entry<String> newEntry = new Entry<String>(zone, entry);
            if (entry == null) entry = newEntry;
        }

        /*
         * Create the circular list by iterating over the entries and trying the last entry to the first entry.
         */
        Entry<String> _entry = entry;
        while (_entry.next != null) {
            _entry = entry.next;
            size++;
        }
        _entry.next = entry;
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
