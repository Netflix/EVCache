package com.netflix.evcache.server.metrics.filters;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicStringSetProperty;
import com.netflix.evcache.server.metrics.EVCacheMetric;
import com.netflix.evcache.server.metrics.MetricsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * @author smansfield
 */
public class WhitelistMetricsFilter implements MetricsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(WhitelistMetricsFilter.class);

    private MetricsClient client;
    private DynamicStringSetProperty property;
    private DynamicBooleanProperty enabled;

    public WhitelistMetricsFilter(MetricsClient client, DynamicStringSetProperty property, DynamicBooleanProperty enabled) {
        this.client = client;
        this.property = property;
        this.enabled = enabled;
    }

    @Override
    public Stream<EVCacheMetric> getStats() {
        if (!enabled.get()) {
            return client.getStats();
        }

        // Keep the metric only if the set *does* contain the metric's name
        return client.getStats().filter(metric -> {
            if (!property.get().contains(metric.getName())) {
                LOGGER.debug("Removing metric because of whitelist: {}", metric);
                return false;
            }

            LOGGER.debug("Whitelist allowing metric: {}", metric);

            return true;
        });
    }
}
