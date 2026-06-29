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
        this(appName, EVCacheConfig.getInstance().getPropertyRepository());
    }

    public EVCacheTranscoder(int max) {
        this(null, max);
    }

    public EVCacheTranscoder(String appName, int max) {
        this(appName, EVCacheConfig.getInstance().getPropertyRepository(), max);
    }

    public EVCacheTranscoder(int max, int compressionThreshold) {
        this(null, max, compressionThreshold);
    }

    public EVCacheTranscoder(String appName, int max, int compressionThreshold) {
        this(appName, EVCacheConfig.getInstance().getPropertyRepository(), max, compressionThreshold);
    }

    /**
     * Repository-aware constructors. The compression algorithm/level are read dynamically from the
     * supplied {@link PropertyRepository}, so callers must pass the repository that is wired to Fast
     * Properties (e.g. {@code poolManager.getEVCacheConfig().getPropertyRepository()}) for FP overrides
     * to take effect. The no-repository constructors above fall back to {@link EVCacheConfig#getInstance()}.
     */
    public EVCacheTranscoder(String appName, PropertyRepository config) {
        this(appName, config, config.get("default.evcache.max.data.size", Integer.class).orElse(20 * 1024 * 1024).get());
    }

    public EVCacheTranscoder(String appName, PropertyRepository config, int max) {
        this(appName, config, max, config.get("default.evcache.compression.threshold", Integer.class).orElse(120).get());
    }

    public EVCacheTranscoder(String appName, PropertyRepository config, int max, int compressionThreshold) {
        super(appName, max);
        setCompressionThreshold(compressionThreshold);
        Property<String> algoProperty = getProperty(config, "evcacheclient.compression.algo", String.class);
        setCompressionAlgorithmProperty(algoProperty);
        Property<Integer> zstdLevelProperty = getProperty(config, "evcacheclient.compression.zstd.level", Integer.class);
        setCompressionLevelProperty(zstdLevelProperty);
    }

    /**
     * Resolves a property preferring the appName-prefixed key and falling back to the global {@code evcache.*} key when
     * no app-specific override exists.
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
