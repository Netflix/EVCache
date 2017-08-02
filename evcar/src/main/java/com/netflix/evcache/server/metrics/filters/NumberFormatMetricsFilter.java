package com.netflix.evcache.server.metrics.filters;

import com.netflix.evcache.server.metrics.EVCacheMetric;
import com.netflix.evcache.server.metrics.MetricsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * Filters out values that have an invalid number format for their data type. Must come after a {@link TagCheckMetricsFilter}.
 *
 * @author smansfield
 */
public class NumberFormatMetricsFilter implements MetricsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NumberFormatMetricsFilter.class);

    private MetricsClient client;

    public NumberFormatMetricsFilter(MetricsClient client) {
        this.client = client;
    }

    @Override
    public Stream<EVCacheMetric> getStats() {
        return client.getStats().filter(metric -> {
            try {
                switch (metric.getTags().get(EVCacheMetric.TAG_DATA_TYPE)) {
                    case EVCacheMetric.DATA_TYPE_UINT64:
                        Long.parseLong(metric.getVal());
                        break;
                    case EVCacheMetric.DATA_TYPE_FLOAT64:
                        Double.parseDouble(metric.getVal());
                        break;
                }

                LOGGER.debug("Valid number format: {}", metric);

                return true;
            } catch (NumberFormatException ex) {
                LOGGER.warn("Filtering out metric with invalid number format: {}", metric);
                return false;
            }
        });
    }
}
