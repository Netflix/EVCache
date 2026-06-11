package com.netflix.evcache;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.CachedData;

public class EVCacheTranscoder extends EVCacheSerializingTranscoder {

    public EVCacheTranscoder() {
        this((String) null);
    }

    public EVCacheTranscoder(String appName) {
        this(appName, EVCacheConfig.getInstance().getPropertyRepository().get("default.evcache.max.data.size", Integer.class).orElse(20 * 1024 * 1024).get());
    }

    public EVCacheTranscoder(int max) {
        this(null, max);
    }

    public EVCacheTranscoder(String appName, int max) {
        this(appName, max, EVCacheConfig.getInstance().getPropertyRepository().get("default.evcache.compression.threshold", Integer.class).orElse(120).get());
    }

    public EVCacheTranscoder(int max, int compressionThreshold) {
        this(null, max, compressionThreshold);
    }

    public EVCacheTranscoder(String appName, int max, int compressionThreshold) {
        super(appName, max);
        setCompressionThreshold(compressionThreshold);
        PropertyRepository config = EVCacheConfig.getInstance().getPropertyRepository();
        Property<String> algoProperty = getProperty(config, "evcacheclient.compression.algo", String.class);
        setCompressionAlgorithmProperty(algoProperty);
        Property<Integer> zstdLevelProperty = getProperty(config, "evcacheclient.compression.zstd.level", Integer.class);
        setCompressionLevelProperty(zstdLevelProperty);
    }

    /**
     * Resolves a property preferring the appName-prefixed key (e.g. {@code EVCACHE_VH_ARCHIVE.default.evcache.compression.algo})
     * and falling back to the global {@code default.evcache.*} key when no app-specific override exists.
     */
    private <T> Property<T> getProperty(PropertyRepository config, String key, Class<T> type) {
        if (appName == null || appName.isEmpty()) {
            return config.get(key, type);
        }
        return config.get(appName + "." + key, type).orElseGet(key);
    }

    @Override
    public CachedData encode(Object o) {
        if (o != null && o instanceof CachedData) return (CachedData) o;
        return super.encode(o);
    }

}
