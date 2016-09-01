package com.netflix.evcache;

import com.netflix.config.ConfigurationManager;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.SerializingTranscoder;

public class EVCacheTranscoder extends SerializingTranscoder {

    public EVCacheTranscoder() {
        this(ConfigurationManager.getConfigInstance().getInt("default.evcache.max.data.size", Integer.MAX_VALUE));
    }

    public EVCacheTranscoder(int max) {
        this(max, ConfigurationManager.getConfigInstance().getInt("default.evcache.compression.threshold", 120));
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