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
}
