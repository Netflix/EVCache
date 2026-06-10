package com.netflix.evcache;

import com.netflix.archaius.api.PropertyRepository;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.CachedData;

public class EVCacheTranscoder extends EVCacheSerializingTranscoder {

    public EVCacheTranscoder() {
        this(EVCacheConfig.getInstance().getPropertyRepository().get("default.evcache.max.data.size", Integer.class).orElse(20 * 1024 * 1024).get());
    }

    public EVCacheTranscoder(int max) {
        this(max, EVCacheConfig.getInstance().getPropertyRepository().get("default.evcache.compression.threshold", Integer.class).orElse(120).get());
    }

    public EVCacheTranscoder(int max, int compressionThreshold) {
        super(max);
        setCompressionThreshold(compressionThreshold);
        PropertyRepository config = EVCacheConfig.getInstance().getPropertyRepository();
        CompressionAlgorithm algo = CompressionAlgorithm.valueOf(
            config.get("default.evcache.compression.algorithm", String.class)
                .orElse("GZIP").get().toUpperCase());
        setCompressionAlgorithm(algo);
        if (algo == CompressionAlgorithm.ZSTD) {
            setCompressionLevel(config.get("default.evcache.compression.zstd.level", Integer.class)
                .orElse(DEFAULT_ZSTD_COMPRESSION_LEVEL).get());
        }
    }

    @Override
    public CachedData encode(Object o) {
        if (o != null && o instanceof CachedData) return (CachedData) o;
        return super.encode(o);
    }

}
