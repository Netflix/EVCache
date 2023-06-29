package com.netflix.evcache;

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
        if (o != null && o instanceof CachedData) return (CachedData) o;
        return super.encode(o);
    }

}
