package com.netflix.evcache.pool;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.function.BiPredicate;

import net.spy.memcached.MemcachedNode;
import org.mockito.Mockito;
import java.util.HashSet;
import java.util.Set;

import org.testng.annotations.Test;

import com.netflix.evcache.pool.EVCacheClient.NodeValidationResult;

public class EVCacheClientBulkValidationTest {

    @Test
    public void okKeysAreSentAndNotRecorded() {
        Set<String> skipped = new HashSet<>();
        assertTrue(EVCacheClient.admitKey(NodeValidationResult.OK, "k", skipped));
        assertTrue(skipped.isEmpty());
    }

    @Test
    public void keysOnFullOrInactiveNodesAreNotSentAndAreRecorded() {
        Set<String> skipped = new HashSet<>();
        assertFalse(EVCacheClient.admitKey(NodeValidationResult.READ_QUEUE_FULL, "full", skipped));
        assertFalse(EVCacheClient.admitKey(NodeValidationResult.INACTIVE_NODE, "inactive", skipped));
        assertEquals(skipped, new HashSet<>(java.util.Arrays.asList("full", "inactive")));
    }

    @Test
    public void aNullCollectorIsFine() {
        assertFalse(EVCacheClient.admitKey(NodeValidationResult.READ_QUEUE_FULL, "k", null));
        assertTrue(EVCacheClient.admitKey(NodeValidationResult.OK, "k", Collections.<String>emptySet()));
    }

    @Test
    public void theValidatorRecordsKeysItRejectsAndOnlyThose() {
        final MemcachedNode full = Mockito.mock(MemcachedNode.class);
        final MemcachedNode fine = Mockito.mock(MemcachedNode.class);
        final Set<String> skipped = new HashSet<>();
        final BiPredicate<MemcachedNode, String> validator = EVCacheClient.skipTrackingValidator(
                node -> node == full ? NodeValidationResult.READ_QUEUE_FULL : NodeValidationResult.OK, skipped);

        assertTrue(validator.test(fine, "sent"));
        assertFalse(validator.test(full, "skipped"));

        assertEquals(skipped, Collections.singleton("skipped"));
    }

    @Test
    public void theValidatorWorksWithoutACollector() {
        final MemcachedNode node = Mockito.mock(MemcachedNode.class);
        assertFalse(EVCacheClient.skipTrackingValidator(n -> NodeValidationResult.INACTIVE_NODE, null).test(node, "k"));
        assertTrue(EVCacheClient.skipTrackingValidator(n -> NodeValidationResult.OK, null).test(node, "k"));
    }
}
