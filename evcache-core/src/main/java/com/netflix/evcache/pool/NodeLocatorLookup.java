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

    /**
     * A lookup for the node locator using sorted arrays and binary search.
     */
    static class ArrayNodeLocatorLookup<V> implements NodeLocatorLookup<V> {
        private final long[] hashes;
        private final V[] values;

        @SuppressWarnings("unchecked")
        ArrayNodeLocatorLookup(TreeMap<Long, V> map) {
            int size = map.size();
            this.hashes = new long[size];
            this.values = (V[]) new Object[size];

            int i = 0;
            for (Map.Entry<Long, V> entry : map.entrySet()) {
                hashes[i] = entry.getKey();
                values[i] = entry.getValue();
                i++;
            }
        }

        @Override
        public V wrappingCeilingValue(long hash) {
            int index = binarySearchCeiling(hash);
            return values[index];
        }

        private int binarySearchCeiling(long hash) {
            // If the hash is greater than all values, wrap around to first element
            if (hash > hashes[hashes.length - 1]) {
                return 0;
            }

            int index = Arrays.binarySearch(hashes, hash);
            if (index >= 0) {
                return index; // exact match
            }

            // Arrays.binarySearch returns (-(insertion point) - 1) when key not found
            // We want the insertion point, which is the ceiling
            return -index - 1;
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
            long[] sortedHashes = newNodeMap.keySet().stream().mapToLong(Long::longValue).toArray();
            this.hashes = new int[sortedHashes.length];
            sortedToEytzinger(sortedHashes, this.hashes, 0, new int[]{0});

            // Store nodes in corresponding Eytzinger order
            this.values = constructValueArray(hashes.length);
            for (int i = 0; i < hashes.length; i++) {
                this.values[i] = newNodeMap.get(Long.valueOf((long)hashes[i] & 0xffffffffL));
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

    /**
     * A lookup using a two-level direct mapping approach.
     * Level 1: Fixed-size array of bytes/chars mapping hash ranges to node indices
     * Level 2: Compact array of actual nodes
     */
    static class DirectApproximateNodeLocatorLookup<V> implements NodeLocatorLookup<V> {
        private static final int BUCKET_BITS = 19;
        private static final int BUCKET_COUNT = 1 << BUCKET_BITS;
        private static final int BUCKET_MASK = BUCKET_COUNT - 1;
        private static final int BYTE_MAX_VALUE = 255;

        private final Object bucketToNodeIndex; // First level: either byte[] or char[]
        private final V[] nodes; // Second level: actual nodes
        private final int nodeCount; // Number of unique nodes
        private final boolean usingBytes;

        @SuppressWarnings("unchecked")
        DirectApproximateNodeLocatorLookup(TreeMap<Long, V> map) {
            // Determine if we can use bytes based on unique node count
            Object[] uniqueValues = new LinkedHashSet<>(map.values()).toArray();
            this.nodeCount = uniqueValues.length;
            this.usingBytes = nodeCount <= BYTE_MAX_VALUE;

            // Initialize appropriate array type
            this.bucketToNodeIndex = usingBytes ? new byte[BUCKET_COUNT] : new char[BUCKET_COUNT];

            this.nodes = (V[]) new Object[nodeCount];
            System.arraycopy(uniqueValues, 0, nodes, 0, nodeCount);

            Map<V, Integer> valueToIndex = new HashMap<>();
            for (int i = 0; i < nodeCount; i++) {
                valueToIndex.put((V) uniqueValues[i], i);
            }

            // Fill bucket table
            long prevHash = -1;
            int prevIndex = 0;

            for (Map.Entry<Long, V> entry : map.entrySet()) {
                long hash = entry.getKey();
                int nodeIndex = valueToIndex.get(entry.getValue());

                int startBucket = (prevHash == -1) ? 0 : getBucketIndex(prevHash + 1);
                int endBucket = getBucketIndex(hash);

                int indexToUse = nodeIndex;
                if (prevHash != -1) {
                    indexToUse = prevIndex;
                }

                while (startBucket != endBucket) {
                    setBucketIndex(startBucket, indexToUse);
                    startBucket = (startBucket + 1) & BUCKET_MASK;
                }
                setBucketIndex(endBucket, nodeIndex);

                prevHash = hash;
                prevIndex = nodeIndex;
            }

            // Fill remaining buckets to handle wrap-around
            if (prevHash != -1) {
                int startBucket = getBucketIndex(prevHash + 1);
                int endBucket = getBucketIndex(-1L);
                while (startBucket != endBucket) {
                    setBucketIndex(startBucket, prevIndex);
                    startBucket = (startBucket + 1) & BUCKET_MASK;
                }
                setBucketIndex(endBucket, prevIndex);
            }
        }

        private void setBucketIndex(int bucket, int value) {
            if (usingBytes) {
                ((byte[]) bucketToNodeIndex)[bucket] = (byte) value;
            } else {
                ((char[]) bucketToNodeIndex)[bucket] = (char) value;
            }
        }

        private int getBucketValue(int bucket) {
            return usingBytes ? ((byte[]) bucketToNodeIndex)[bucket] & 0xFF : ((char[]) bucketToNodeIndex)[bucket];
        }

        private static int getBucketIndex(long hash) {
            return (int) ((hash >>> (32 - BUCKET_BITS)) & BUCKET_MASK);
        }

        @Override
        public V wrappingCeilingValue(long hash) {
            return nodes[getBucketValue(getBucketIndex(hash))];
        }
    }

}
