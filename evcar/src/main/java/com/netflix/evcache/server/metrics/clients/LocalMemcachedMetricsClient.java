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
public final class LocalMemcachedMetricsClient implements MetricsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalMemcachedMetricsClient.class);

    public static DynamicStringSetProperty MEMCACHED_GAUGE_NAMES = new DynamicStringSetProperty("evcsidecar.metrics.gauge.names",
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

    public LocalMemcachedMetricsClient(MemcachedClient client) throws IOException {
        this.client = client;
    }

    public Stream<EVCacheMetric> getStats() {
        Map<SocketAddress, Map<String, String>> rawrawstats = this.client.getStats();
        Map<String, String> rawstats = rawrawstats.values().stream().findFirst().orElseGet(Collections::emptyMap);

        return parseMetrics(rawstats);
    }

    Stream<EVCacheMetric> parseMetrics(Map<String, String> rawstats) {
        return rawstats.entrySet().stream().map(entry -> {
            String metricName = entry.getKey();
            String metricValue = entry.getValue();
            Map<String, String> tags = new HashMap<>();

            tags.put(EVCacheMetric.TAG_DATA_TYPE, EVCacheMetric.DATA_TYPE_UINT64);

            if (MEMCACHED_GAUGE_NAMES.get().contains(metricName)) {
                tags.put(EVCacheMetric.TAG_TYPE, EVCacheMetric.TYPE_GAUGE);
            } else {
                tags.put(EVCacheMetric.TAG_TYPE, EVCacheMetric.TYPE_COUNTER);
            }

            EVCacheMetric newMetric = new EVCacheMetric(metricName, metricValue, tags);
            LOGGER.debug("Got metric: {}", newMetric);
            return newMetric;
        });
    }
}
