package com.netflix.evcache.server.metrics.filters;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.evcache.server.metrics.EVCacheMetric;
import com.netflix.evcache.server.metrics.MetricsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * Gates the sending of a set of metrics. Sends if the property is true, does not send if the property is false.
 *
 * @author smansfield
 */
public class GateMetricsFilter implements MetricsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(GateMetricsFilter.class);

    private MetricsClient client;
    private DynamicBooleanProperty property;

    public GateMetricsFilter(MetricsClient client, DynamicBooleanProperty property) {
        this.client = client;
        this.property = property;
    }

    @Override
    public Stream<EVCacheMetric> getStats() {
        if (property.get()) {
            LOGGER.debug("Gating allowing stats by property {}", property.getName());
            return client.getStats();
        }

        LOGGER.info("Gating stats by property {}", property.getName());
        return Stream.empty();
    }
}
