package com.netflix.evcache.pool;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

import java.util.TreeMap;

public class NodeLocatorLookupTest {

    @Test
    public void testTreeMapNodeLocatorLookup() {
        TreeMap<Long, String> map = new TreeMap<>();
        map.put(10L, "node1");
        map.put(20L, "node2");
        map.put(30L, "node3");

        NodeLocatorLookup<String> lookup = new NodeLocatorLookup.TreeMapNodeLocatorLookup<>(map);

        // Test exact matches
        assertEquals(lookup.wrappingCeilingValue(10L), "node1");
        assertEquals(lookup.wrappingCeilingValue(20L), "node2");
        assertEquals(lookup.wrappingCeilingValue(30L), "node3");

        // Test ceiling behavior
        assertEquals(lookup.wrappingCeilingValue(15L), "node2");
        assertEquals(lookup.wrappingCeilingValue(25L), "node3");

        // Test wrapping behavior (values greater than max should wrap to first node)
        assertEquals(lookup.wrappingCeilingValue(35L), "node1");
        assertEquals(lookup.wrappingCeilingValue(Long.MAX_VALUE), "node1");

        // Test values less than min
        assertEquals(lookup.wrappingCeilingValue(5L), "node1");
        assertEquals(lookup.wrappingCeilingValue(0L), "node1");
    }

    @Test
    public void testEytzingerNodeLocatorLookup() {
        TreeMap<Long, String> map = new TreeMap<>();
        map.put(10L, "node1");
        map.put(20L, "node2");
        map.put(30L, "node3");

        NodeLocatorLookup<String> lookup = new NodeLocatorLookup.EytzingerNodeLocatorLookup<>(map);

        // Test exact matches
        assertEquals(lookup.wrappingCeilingValue(10L), "node1");
        assertEquals(lookup.wrappingCeilingValue(20L), "node2");
        assertEquals(lookup.wrappingCeilingValue(30L), "node3");

        // Test ceiling behavior
        assertEquals(lookup.wrappingCeilingValue(15L), "node2");
        assertEquals(lookup.wrappingCeilingValue(25L), "node3");

        // Test wrapping behavior (values greater than max should wrap to first node)
        assertEquals(lookup.wrappingCeilingValue(35L), "node1");
        assertEquals(lookup.wrappingCeilingValue(Long.MAX_VALUE), "node1");

        // Test values less than min
        assertEquals(lookup.wrappingCeilingValue(5L), "node1");
        assertEquals(lookup.wrappingCeilingValue(0L), "node1");
    }

    @Test
    public void testLargeScaleConsistency() {
        // Generate a large TreeMap with 1 million entries
        TreeMap<Long, String> map = new TreeMap<>();
        int numEntries = 1_000_000;
        for (int i = 0; i < numEntries; i++) {
            map.put((long) i * 100, "node" + i);
        }

        // Create all three implementations
        NodeLocatorLookup<String> eytzingerLookup = new NodeLocatorLookup.EytzingerNodeLocatorLookup<>(map);
        NodeLocatorLookup<String> treeMapLookup = new NodeLocatorLookup.TreeMapNodeLocatorLookup<>(map);

        // Test a range of values including edge cases
        for (long i = -1000; i <= (numEntries * 100L + 1000); i++) {
            String eytzingerResult = eytzingerLookup.wrappingCeilingValue(i);
            String treeMapResult = treeMapLookup.wrappingCeilingValue(i);

            // Assert all implementations return the same result
            assertEquals(treeMapResult, eytzingerResult,
                    "TreeMap and Eytzinger implementations differ for key " + i);
        }
    }
}
