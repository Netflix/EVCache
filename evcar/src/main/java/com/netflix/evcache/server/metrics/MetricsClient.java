package com.netflix.evcache.server.metrics;

import java.util.stream.Stream;

/**
 * @author smansfield
 */
public interface MetricsClient {
    Stream<EVCacheMetric> getStats();
}
