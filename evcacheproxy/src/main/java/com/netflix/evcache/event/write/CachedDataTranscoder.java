package com.netflix.evcache.event.write;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.BaseSerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

/**
 * A transcoder used only by EVCache client to ensure we return the raw data as stored in memcached 
 * 
 * @author smadappa
 *
 */
public class CachedDataTranscoder extends BaseSerializingTranscoder implements Transcoder<CachedData> {

    public CachedDataTranscoder() {
        super(Integer.MAX_VALUE);
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
        return Integer.MAX_VALUE;
    }

}
