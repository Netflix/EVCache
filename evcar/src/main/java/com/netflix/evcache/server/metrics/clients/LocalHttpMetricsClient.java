package com.netflix.evcache.server.metrics.clients;

import com.netflix.evcache.server.metrics.MetricsClient;
import com.netflix.evcache.server.metrics.EVCacheMetric;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author smansfield
 */
public class LocalHttpMetricsClient implements MetricsClient {

    public static final Logger LOGGER = LoggerFactory.getLogger(LocalHttpMetricsClient.class);

    private String url;

    public LocalHttpMetricsClient(String url) {
        this.url = url;
    }

    @Override
    public Stream<EVCacheMetric> getStats() {

        try {
            HttpResponse httpResponse = HttpClientBuilder.create().build().execute(new HttpGet(this.url));
            return parseMetrics(httpResponse.getEntity().getContent());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return Stream.empty();
    }

    Stream<EVCacheMetric> parseMetrics(InputStream inputStream) throws IOException {
        List<EVCacheMetric> stats = new ArrayList<>();
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String line;
        outer:
        while ((line = bufferedReader.readLine()) != null) {
            line = StringUtils.trim(line);
            String[] metric = StringUtils.split(line);

            if (metric.length != 2) {
                LOGGER.error("Skipping line for unrecognized format: " + line);
                continue;
            }

            LOGGER.debug("Metric Name: {}   &&   Metric Value: {}", metric[0], metric[1]);

            String value = metric[1];

            // All metrics are expected to have at least two tags:
            // type - The type of metric, e.g. counter, gauge
            String[] parts = StringUtils.split(metric[0], '|');
            if (parts.length < 3) {
                LOGGER.warn("Skipping metric. Expected at least three parts: name, type, dataType. Got: {}", metric[0]);
                continue;
            }

            Map<String, String> tags = new HashMap<>(parts.length - 1);
            String name = StringUtils.trim(parts[0]);

            // Extract all the tags, making sure the tag format is correct, but no more
            for (int i = 1; i < parts.length; i++) {
                String tag = parts[i];
                String[] tagparts = StringUtils.split(tag, '*');

                if (tagparts.length != 2 ||
                    StringUtils.isEmpty(tagparts[0]) ||
                    StringUtils.isEmpty(tagparts[1])) {
                    LOGGER.warn("Skipping metric. Bad tag: {}", tag);
                    continue outer;
                }

                LOGGER.debug("Tag name: {}   &&   Tag value: {}", tagparts[0], tagparts[1]);

                tags.put(StringUtils.trim(tagparts[0]), StringUtils.trim(tagparts[1]));
            }

            EVCacheMetric newMetric = new EVCacheMetric(name, value, tags);
            LOGGER.debug("Got metric: {}", newMetric);
            stats.add(newMetric);
        }

        return stats.stream();
    }
}
