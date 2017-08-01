/*
 * LocalMemcachedClient.java
 *
 * $Header: //depot/cloud/evcache/server/main/src/com/netflix/evcache/server/LocalMemcachedClient.java#5 $
 * $DateTime: 2011/05/02 18:34:56 $
 *
 * Copyright (c) 2009 Netflix, Inc.  All rights reserved.
 */
package com.netflix.evcache.server.metrics.clients;

import com.netflix.config.DynamicStringSetProperty;
import com.netflix.evcache.server.metrics.EVCacheMetric;
import com.netflix.evcache.server.metrics.MetricsClient;
import net.spy.memcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;


/**
 * Convenience class for getting to the memcached node running locally.
 *
 * @author smadappa
 */
public final class LocalMemcachedSlabMetricsClient implements MetricsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalMemcachedSlabMetricsClient.class);

    private static final String STATS_CMD_SLABS = "slabs";

    public static DynamicStringSetProperty MEMCACHED_GAUGE_NAMES = new DynamicStringSetProperty("evcsidecar.metrics.memcached.gauge.names",
        "bytes," +
            "free_space," +
            "curr_items," +
            "curr_connections," +
            "chunk_size," +
            "chunks_per_page," +
            "total_pages," +
            "total_chunks," +
            "used_chunks," +
            "free_chunks," +
            "free_chunks_end," +
            "mem_requested," +
            "total_connections," +
            "connection_structures," +
            "limit_maxbytes," +
            "total_items," +
            "mem_requested," +
            "active_slabs," +
            "total_malloced," +
            "slab_global_page_pool");

    private MemcachedClient client;

    public LocalMemcachedSlabMetricsClient(MemcachedClient client) throws IOException {
        this.client = client;
    }

    public Stream<EVCacheMetric> getStats() {
        Map<SocketAddress, Map<String, String>> stats = this.client.getStats(STATS_CMD_SLABS);

        Map<String, String> slabStats = stats.values().stream().findFirst().orElseGet(Collections::emptyMap);

        return slabStats.entrySet().stream().map(entry -> {
            String metricName = entry.getKey();
            String metricValue = entry.getValue();
            Map<String, String> tags = new HashMap<>();

            LOGGER.debug("Slab metric: Name: " + metricName + "   &&   Value: " + metricValue);
            String name;

            int idx = metricName.indexOf(':');

            if (idx != -1) {
                String slab = metricName.substring(0, idx);
                name = metricName.substring(idx + 1);
                tags.put(EVCacheMetric.TAG_SLAB, slab);

            } else {
                name = metricName;
            }

            // Fill out the tags
            // Always uint64 for slab stats
            tags.put(EVCacheMetric.TAG_DATA_TYPE, EVCacheMetric.DATA_TYPE_UINT64);

            // Differentiate metric types with a fast property, since there's no information from memcached
            if (MEMCACHED_GAUGE_NAMES.get().contains(name)) {
                tags.put(EVCacheMetric.TAG_TYPE, EVCacheMetric.TYPE_GAUGE);
            } else {
                tags.put(EVCacheMetric.TAG_TYPE, EVCacheMetric.TYPE_COUNTER);
            }

            EVCacheMetric newMetric = new EVCacheMetric(name, metricValue, tags);
            LOGGER.debug("Got metric: {}", newMetric);
            return newMetric;
        });
    }
}
