package com.netflix.evcache.pool;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

class ChunkTranscoder implements Transcoder<CachedData> {

    public ChunkTranscoder() {
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
