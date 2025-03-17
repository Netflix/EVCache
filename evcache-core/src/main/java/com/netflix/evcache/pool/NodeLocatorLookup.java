package com.netflix.evcache.pool;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

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

    static <V> void coalesceMap(TreeMap<Long, V> map) {
        if (map == null || map.isEmpty()) {
            return;
        }

        // we only ever use the 'ceiling' value, [ie: the next one above]
        // so if we find three nodes 1->A 2->A 3->B, we can collapse to 2->A 3->B
        // and results will be identical

        Long prevKey = null;
        V prevValue = null;
        ArrayList<Long> keysToRemove = new ArrayList<>();
        for (Long key : map.keySet().toArray(new Long[0])) {
            V currentValue = map.get(key);

            if (prevValue != null && prevValue.equals(currentValue)) {
                keysToRemove.add(prevKey);
            }

            prevKey = key;
            prevValue = currentValue;
        }

        for (Long key : keysToRemove) {
            map.remove(key);
        }
    }

    /**
     * A lookup for the node locator using the Eytzinger layout.
     */
    static class EytzingerNodeLocatorLookup<V> implements NodeLocatorLookup<V> {

        private final int[] hashes;
        private final V[] values;
        private final int leftmostNodePosition;

        EytzingerNodeLocatorLookup(TreeMap<Long, V> newNodeMap) {
            // Convert the sorted hashes to Eytzinger layout
            TreeMap<Long, V> clone = new TreeMap<>(newNodeMap);
            coalesceMap(clone);

            long[] sortedHashes = clone.keySet().stream().mapToLong(Long::longValue).toArray();
            this.hashes = new int[sortedHashes.length];
            sortedToEytzinger(sortedHashes, this.hashes, 0, new int[]{0});

            // Store nodes in corresponding Eytzinger order
            this.values = constructValueArray(hashes.length);
            for (int i = 0; i < hashes.length; i++) {
                this.values[i] = clone.get(Long.valueOf((long)hashes[i] & 0xffffffffL));
            }

            // we can set the leftmost node position directly because the
            // Eytzinger layout is a complete binary tree, this avoids some
            // unnecessary iterations when the search wraps around the ring
            int _leftmostNodePosition = 0;
            while (2 * _leftmostNodePosition + 1 < hashes.length) {
                _leftmostNodePosition = 2 * _leftmostNodePosition + 1;
            }
            this.leftmostNodePosition = _leftmostNodePosition;

        }

        @SuppressWarnings("unchecked")
        private V[] constructValueArray(int length) {
            return (V[]) new Object[length];
        }

        @Override
        public V wrappingCeilingValue(long hash) {
            int pos = eytzingerCeilingPosition(hash);
            return values[pos];
        }

        private int eytzingerCeilingPosition(long hash) {
            int pos = 0;
            int candidate = -1;

            while (pos < hashes.length) {
                long nodeHash = ((long) hashes[pos]) & 0xffffffffL;
                if (hash <= nodeHash) {
                    candidate = pos;
                    pos = 2 * pos + 1;
                } else {
                    pos = 2 * pos + 2;
                }
            }

            if (candidate == -1) {
                return leftmostNodePosition;
            }

            return candidate;
        }

        private static void sortedToEytzinger(long[] sorted, int[] eytzinger, int i, int[] pos) {
            if (i >= eytzinger.length || pos[0] >= sorted.length) return;

            sortedToEytzinger(sorted, eytzinger, 2 * i + 1, pos);
            eytzinger[i] = (int)sorted[pos[0]];
            pos[0]++;
            sortedToEytzinger(sorted, eytzinger, 2 * i + 2, pos);
        }
    }

}
