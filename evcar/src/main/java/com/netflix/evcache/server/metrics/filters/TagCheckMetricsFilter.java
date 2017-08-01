package com.netflix.evcache.server.metrics.filters;

import com.netflix.evcache.server.metrics.EVCacheMetric;
import com.netflix.evcache.server.metrics.MetricsClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * Removes metrics that are not properly tagged (and logs them)
 *
 * Metrics need an approved type tag and data type tag to pass the filter.
 *
 * @author smansfield
 */
public class TagCheckMetricsFilter implements MetricsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(TagCheckMetricsFilter.class);

    private MetricsClient client;

    public TagCheckMetricsFilter(MetricsClient client) {
        this.client = client;
    }

    @Override
    public Stream<EVCacheMetric> getStats() {
        return client.getStats().filter(metric -> {

            // First check that it's either a counter or a gauge
            String type = metric.getTags().get(EVCacheMetric.TAG_TYPE);
            if (!(StringUtils.equals(type, EVCacheMetric.TYPE_COUNTER) || StringUtils.equals(type, EVCacheMetric.TYPE_GAUGE))) {
                LOGGER.warn("Found metric with invalid type: {}", metric);
                return false;
            }

            // Next check that it's either an int64 or a float64
            String dataType = metric.getTags().get(EVCacheMetric.TAG_DATA_TYPE);
            if (!(StringUtils.equals(dataType, EVCacheMetric.DATA_TYPE_UINT64) || StringUtils.equals(dataType, EVCacheMetric.DATA_TYPE_FLOAT64))) {
                LOGGER.warn("Found metric with invalid data type: {}", metric);
                return false;
            }

            LOGGER.debug("Metric has valid tags: {}", metric);

            return true;
        });
    }
}
