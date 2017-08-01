package com.netflix.evcache.server.metrics.filters;

import com.netflix.evcache.server.metrics.EVCacheMetric;
import com.netflix.evcache.server.metrics.MetricsClient;

import java.util.List;
import java.util.stream.Stream;

/**
 * @author smansfield
 */
public class MergedMetricsClient implements MetricsClient {

    private List<MetricsClient> clients;

    public MergedMetricsClient(List<MetricsClient> clients) {
        this.clients = clients;
    }

    @Override
    public Stream<EVCacheMetric> getStats() {
        // this takes all the streams of all the clients and concatenates them into one long stream
        return clients.stream().map(MetricsClient::getStats).reduce(Stream::concat).get();
    }
}
