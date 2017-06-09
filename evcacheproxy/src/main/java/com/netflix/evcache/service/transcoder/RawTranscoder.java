package com.netflix.evcache.service.transcoder;

import com.netflix.evcache.EVCacheTranscoder;

import net.spy.memcached.CachedData;

/**
 * Created by senugula on 6/23/16.
 */
public class RawTranscoder extends EVCacheTranscoder {

    public RawTranscoder() {
    }

    public boolean asyncDecode(CachedData d) {
        return false;
    }

    public CachedData decode(CachedData d) {
        return d;
    }

    public CachedData encode(CachedData o) {
        return o;
    }

    public int getMaxSize() {
        return CachedData.MAX_SIZE;
    }
}
