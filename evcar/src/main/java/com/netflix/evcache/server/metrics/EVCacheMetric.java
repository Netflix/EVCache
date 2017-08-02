package com.netflix.evcache.server.metrics;

import java.util.Map;

public class EVCacheMetric {

    public static final String TAG_TYPE = "type";

    public static final String TYPE_COUNTER = "counter";
    public static final String TYPE_GAUGE = "gauge";

    public static final String TAG_DATA_TYPE = "dataType";

    public static final String DATA_TYPE_UINT64 = "uint64";
    public static final String DATA_TYPE_FLOAT64 = "float64";

    public static final String TAG_STATISTIC = "statistic";
    public static final String TAG_PERCENTILE = "percentile";
    public static final String TAG_SLAB = "slab";

    private final String name;
    private final String val;
    private final Map<String, String> tags;
    private final String mapKey;

    public EVCacheMetric(String name, String val, Map<String, String> tags) {
        this.name = name;
        this.val = val;
        this.tags = tags;

        StringBuilder sb = new StringBuilder();

        sb.append(this.name);

        if (tags != null) {
            this.tags.entrySet().forEach((entry) -> {
                sb.append('|');
                sb.append(entry.getKey());
                sb.append('*');
                sb.append(entry.getValue());
            });
        }

        mapKey = sb.toString();
    }

    public String getName() {
        return name;
    }

    public String getAtlasName() {
        String prefix;

        if (this.tags.get(TAG_SLAB) != null) {
            prefix = "EVCacheSlabMetric-";
        } else {
            prefix = "EVCacheMetric-";
        }

        return prefix + this.name;
    }

    public String getVal() {
        return val;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public String mapKey() {
        return this.mapKey;
    }

    @Override
    public String toString() {
        return this.mapKey + "|" + this.val;
    }
}
