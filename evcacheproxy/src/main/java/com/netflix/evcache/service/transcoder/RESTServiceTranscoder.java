package com.netflix.evcache.service.transcoder;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Created by senugula on 6/23/16.
 */
public class RESTServiceTranscoder implements Transcoder<CachedData> {

    public RESTServiceTranscoder() {

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
