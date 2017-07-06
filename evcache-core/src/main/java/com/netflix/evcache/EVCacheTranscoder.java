package com.netflix.evcache;

import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.SerializingTranscoder;

public class EVCacheTranscoder extends SerializingTranscoder {

    public EVCacheTranscoder() {
        this(EVCacheConfig.getInstance().getDynamicIntProperty("default.evcache.max.data.size", 20 * 1024 * 1024).get());
    }

    public EVCacheTranscoder(int max) {
        this(max, EVCacheConfig.getInstance().getDynamicIntProperty("default.evcache.compression.threshold", 120).get());
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