package com.netflix.evcache.pool;

import java.util.Map;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.HashMap;

/**
 * A lookup for the node locator.
 *
 * The hash is a 64-bit long, but the value is masked to a 32-bit int and the
 * upper 32-bits are ignored.
 */
public interface NodeLocatorLookup<V> {
    V wrappingCeilingValue(long hash);

    static class TreeMapNodeLocatorLookup<V> implements NodeLocatorLookup<V> {

        private final TreeMap<Long, V> map;

        TreeMapNodeLocatorLookup(TreeMap<Long, V> map) {
            this.map = map;
        }

        @Override
        public V wrappingCeilingValue(long hash) {
            Map.Entry<Long, V> entry = map.ceilingEntry(hash);
            if (entry == null) {
                entry = map.firstEntry();
            }
            return entry.getValue();
        }
    }

}
