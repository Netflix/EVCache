package com.netflix.evcache;

import com.netflix.evcache.config.EVCacheTranscoderProperties;
import com.netflix.evcache.pool.EVCacheValue;
import com.netflix.evcache.pool.EVCacheValueSerde;
import com.netflix.evcache.util.EVCacheConfig;
import net.spy.memcached.CachedData;

public class EVCacheTranscoder extends EVCacheSerializingTranscoder {

    public EVCacheTranscoder() {
        this(new EVCacheTranscoderProperties(null, EVCacheConfig.getInstance().getPropertyRepository()));
    }

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
        this(properties.getMaxDataSizeBytes(), properties.getCompressionThresholdBytes(), properties);
    }

    public EVCacheTranscoder(int max) {
        this(max, new EVCacheTranscoderProperties(null, EVCacheConfig.getInstance().getPropertyRepository()));
    }

    public EVCacheTranscoder(int max, int compressionThreshold) {
        this(max, compressionThreshold, new EVCacheTranscoderProperties(null, EVCacheConfig.getInstance().getPropertyRepository()));
    }

    private EVCacheTranscoder(int max, EVCacheTranscoderProperties properties) {
        this(max, properties.getCompressionThresholdBytes(), properties);
    }

    private EVCacheTranscoder(int max, int compressionThreshold, EVCacheTranscoderProperties properties) {
        super(max, properties);
        setCompressionThreshold(compressionThreshold);
    }

    @Override
    public CachedData encode(Object o) {
        if (o != null && o instanceof CachedData) return (CachedData) o;
        return super.encode(o);
    }

    @Override
    protected byte[] serialize(Object o) {
        if (transcoderProperties.isBinarySerializationEnabled() && o instanceof EVCacheValue) {
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
