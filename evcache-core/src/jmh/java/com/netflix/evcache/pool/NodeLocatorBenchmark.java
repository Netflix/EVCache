package com.netflix.evcache.pool;

import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.util.DefaultKetamaNodeLocatorConfiguration;
import net.spy.memcached.util.KetamaNodeLocatorConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import com.netflix.evcache.pool.NodeLocatorLookup.ArrayNodeLocatorLookup;
import com.netflix.evcache.pool.NodeLocatorLookup.EytzingerNodeLocatorLookup;
import com.netflix.evcache.pool.NodeLocatorLookup.TreeMapNodeLocatorLookup;
import com.netflix.evcache.pool.NodeLocatorLookup.DirectApproximateNodeLocatorLookup;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@State(Scope.Benchmark)
public class NodeLocatorBenchmark {
    @Param({"10", "120"})
    int nodeCount;

    @Param({"2000"})
    int keyCount;

    @Param({"0", "50"})
    int keyTailLength;

    @Param({"legacy", "array", "eytzinger", "direct"})
    String locator;

    @Param({"ketama-md5", "simple-fnv1a", "ketama-murmur3"})
    String hashRing;

    NodeLocator impl;

    List<MemcachedNode> nodes;

    String[] keys;

    static Function<TreeMap<Long, MemcachedNode>, NodeLocatorLookup<MemcachedNode>> findLookupFactory(String locator) {
        if (locator.equals("legacy")) {
            return TreeMapNodeLocatorLookup::new;
        } else if (locator.equals("array")) {
            return ArrayNodeLocatorLookup::new;
        } else if (locator.equals("eytzinger")) {
            return EytzingerNodeLocatorLookup::new;
        } else if (locator.equals("direct")) {
            return DirectApproximateNodeLocatorLookup::new;
        } else {
            throw new RuntimeException("Unknown locator: " + locator);
        }
    }

    static HashRingAlgorithm findHashRingAlgorithm(String hashRing) {
        if (hashRing.equals("ketama-md5")) {
            return new HashRingAlgorithm.KetamaMd5HashRingAlgorithm();
        } else if (hashRing.equals("simple-fnv1a")) {
            return new HashRingAlgorithm.SimpleHashRingAlgorithm(DefaultHashAlgorithm.FNV1A_64_HASH);
        } else if (hashRing.equals("ketama-murmur3")) {
            return new HashRingAlgorithm.KetamaMurmur3HashRingAlgorithm();
        } else {
            throw new RuntimeException("Unknown hash ring: " + hashRing);
        }
    }

    @Setup
    public void setup() {
        nodes = new ArrayList<>();
        for (int i = 1; i <= nodeCount; i++) {
            MemcachedNode node = mock(MemcachedNode.class);
            when(node.getSocketAddress()).thenReturn(new InetSocketAddress("100.94.221." + i, 11211));
            nodes.add(node);
        }

        EVCacheClient client = mock(EVCacheClient.class);
        when(client.getAppName()).thenReturn("mockApp");
        when(client.getServerGroupName()).thenReturn("mockAppServerGroup");
        KetamaNodeLocatorConfiguration conf = new DefaultKetamaNodeLocatorConfiguration();

        HashRingAlgorithm hashRingAlgorithm = findHashRingAlgorithm(hashRing);
        Function<TreeMap<Long, MemcachedNode>, NodeLocatorLookup<MemcachedNode>> lookupFactory = findLookupFactory(locator);

        impl = new EVCacheNodeLocator(client, nodes, hashRingAlgorithm, conf, lookupFactory);

        keys = new String[keyCount];
        for (int i = 0; i < keyCount; i++) {
            keys[i] = "key_" + i;
            for (int j = 0; j < keyTailLength; j++) {
                keys[i] += (char)('a' + (i % 26));
            }
        }
    }

    @Benchmark
    public void testGetPrimary(Blackhole bh) {
        for (int i = 0; i < keyCount; i++) {
            bh.consume(impl.getPrimary(keys[i]));
        }
    }
}