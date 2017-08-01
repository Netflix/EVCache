package com.netflix.evcache.server.metrics.filters;

import com.netflix.evcache.server.metrics.EVCacheMetric;
import com.netflix.evcache.server.metrics.MetricsClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * Filters out all integer counters that are zero. Other values are left intact.
 *
 * @author smansfield
 */
public class ZeroCounterMetricsFilter implements MetricsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroCounterMetricsFilter.class);

    private MetricsClient client;

    public ZeroCounterMetricsFilter(MetricsClient client) {
        this.client = client;
    }

    @Override
    public Stream<EVCacheMetric> getStats() {
        return client.getStats().filter(metric -> {
            try {
                // skip any non-counters
                if (!StringUtils.equals(metric.getTags().get(EVCacheMetric.TAG_TYPE), EVCacheMetric.TYPE_COUNTER)) {
                    return true;
                }

                // skip any non-int counters
                if (!StringUtils.equals(metric.getTags().get(EVCacheMetric.TAG_DATA_TYPE), EVCacheMetric.DATA_TYPE_UINT64)) {
                    return true;
                }

                if (Long.parseLong(metric.getVal()) == 0) {
                    LOGGER.debug("Removing metric because it is a counter and its value is zero: {}", metric);
                    return false;
                }

                LOGGER.debug("Allowing metric because it is a non-zero counter: {}", metric);

                return true;

            } catch (Exception ex) {
                // Any exceptions in parsing should NOT be let through
                LOGGER.warn("Int counter had invalid value: {}", metric);
                return false;
            }
        });
    }
}
