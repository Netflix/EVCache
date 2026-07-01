package com.netflix.evcache;

import com.netflix.evcache.config.EVCacheTranscoderProperties;
import com.netflix.evcache.pool.EVCacheValue;
import com.netflix.evcache.pool.EVCacheValueSerde;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.CachedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.evcache.config.EVCacheTranscoderProperties.DEFAULT_COMPRESSION_THRESHOLD_BYTES;
import static com.netflix.evcache.config.EVCacheTranscoderProperties.DEFAULT_LOG_PUT_ENABLED;
import static com.netflix.evcache.config.EVCacheTranscoderProperties.DEFAULT_MAX_DATA_SIZE_BYTES;
import static com.netflix.evcache.config.EVCacheTranscoderProperties.Key.COMPRESSION_THRESHOLD_BYTES;
import static com.netflix.evcache.config.EVCacheTranscoderProperties.Key.LOG_PUT_ENABLED;
import static com.netflix.evcache.config.EVCacheTranscoderProperties.Key.MAX_DATA_SIZE_BYTES;

public class EVCacheTranscoder extends EVCacheSerializingTranscoder {

    private static final Logger log = LoggerFactory.getLogger(EVCacheTranscoder.class);

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
        this(properties.getProperty(MAX_DATA_SIZE_BYTES, Integer.class, DEFAULT_MAX_DATA_SIZE_BYTES),
                properties.getProperty(COMPRESSION_THRESHOLD_BYTES, Integer.class, DEFAULT_COMPRESSION_THRESHOLD_BYTES),
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
        this(max, properties.getProperty(COMPRESSION_THRESHOLD_BYTES, Integer.class, DEFAULT_COMPRESSION_THRESHOLD_BYTES), properties);
    }

    private EVCacheTranscoder(int max, int compressionThreshold, EVCacheTranscoderProperties properties) {
        super(properties, max);
        setCompressionThreshold(compressionThreshold);
    }

    @Override
    public boolean asyncDecode(CachedData d) {
        return super.asyncDecode(d);
    }

    @Override
    public Object decode(CachedData d) {
        return super.decode(d);
    }

    @Override
    public CachedData encode(Object o) {
        if (this.properties.getProperty(LOG_PUT_ENABLED, Boolean.class, DEFAULT_LOG_PUT_ENABLED)) {
            log.info("EVCacheTranscoder.encode LOG_PUT_ENABLED=true type={} value={}",
                    o == null ? "null" : o.getClass().getName(), o);
        }
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
