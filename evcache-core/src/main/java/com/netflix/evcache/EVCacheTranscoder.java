package com.netflix.evcache;

import com.netflix.evcache.config.EVCacheTranscoderProperties;
import com.netflix.evcache.pool.EVCacheValue;
import com.netflix.evcache.pool.EVCacheValueSerde;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.CachedData;

import static com.netflix.evcache.config.EVCacheTranscoderProperties.DEFAULT_COMPRESSION_THRESHOLD_BYTES;
import static com.netflix.evcache.config.EVCacheTranscoderProperties.DEFAULT_MAX_DATA_SIZE_BYTES;
import static com.netflix.evcache.config.EVCacheTranscoderProperties.Key.COMPRESSION_THRESHOLD_BYTES;
import static com.netflix.evcache.config.EVCacheTranscoderProperties.Key.MAX_DATA_SIZE_BYTES;

public class EVCacheTranscoder extends EVCacheSerializingTranscoder {

    /**
     * @param properties the transcoder property bundle.
     *                   {@link EVCacheTranscoderProperties.Key#MAX_DATA_SIZE_BYTES} and
     *                   {@link EVCacheTranscoderProperties.Key#COMPRESSION_THRESHOLD_BYTES}
     *                   are read from the bundle at construction and set as fields on the
     *                   underlying {@link EVCacheSerializingTranscoder} for legacy reasons;
     *                   new properties should be added to {@link EVCacheTranscoderProperties}
     *                   rather than plumbed through further constructor arguments.
     */
    public EVCacheTranscoder(EVCacheTranscoderProperties properties) {
        this(properties.getProperty(MAX_DATA_SIZE_BYTES, Integer.class, DEFAULT_MAX_DATA_SIZE_BYTES).get(),
                properties.getProperty(COMPRESSION_THRESHOLD_BYTES, Integer.class, DEFAULT_COMPRESSION_THRESHOLD_BYTES).get(),
                properties
        );
    }

    public EVCacheTranscoder() {
        this(new EVCacheTranscoderProperties(null, EVCacheConfig.getInstance().getPropertyRepository()));
    }

    public EVCacheTranscoder(int max) {
        this(max, new EVCacheTranscoderProperties(null, EVCacheConfig.getInstance().getPropertyRepository()));
    }

    public EVCacheTranscoder(int max, int compressionThreshold) {
        this(max, compressionThreshold, new EVCacheTranscoderProperties(null, EVCacheConfig.getInstance().getPropertyRepository()));
    }

    private EVCacheTranscoder(int max, EVCacheTranscoderProperties properties) {
        this(max, properties.getProperty(COMPRESSION_THRESHOLD_BYTES, Integer.class, DEFAULT_COMPRESSION_THRESHOLD_BYTES).get(), properties);
    }

    private EVCacheTranscoder(int max, int compressionThreshold, EVCacheTranscoderProperties properties) {
        super(properties, max);
        setCompressionThreshold(compressionThreshold);
        Property<String> algoProperty = getProperty(config, "default.evcacheclient.compression.algo", String.class);
        setCompressionAlgorithmProperty(algoProperty);
        Property<Integer> zstdLevelProperty = getProperty(config, "default.evcacheclient.compression.zstd.level", Integer.class);
        setCompressionLevelProperty(zstdLevelProperty);
    }

    /**
     * Resolves a property preferring the appName-prefixed key and falling back to the global {@code evcache.*} key when
     * no app-specific override exists.
     */
    private <T> Property<T> getProperty(PropertyRepository config, String key, Class<T> type) {
        if (appName == null || appName.isEmpty()) {
            return config.get("default." + key, type);
        }
        return config.get(appName + "." + key, type).orElseGet(key);
    }

    @Override
    public CachedData encode(Object o) {
        if (o != null && o instanceof CachedData) return (CachedData) o;
        return super.encode(o);
    }

    @Override
    protected byte[] serialize(Object o) {
        if (this.properties.isBinarySerializationEnabled() && o instanceof EVCacheValue) {
            return EVCacheValueSerde.serialize((EVCacheValue) o);
        }
        return super.serialize(o);
    }

    @Override
    protected Object deserialize(byte[] in) {
        if (EVCacheValueSerde.isBinaryFormat(in)) {
            return EVCacheValueSerde.deserialize(in);
        }
        return super.deserialize(in);
    }

}
