package com.netflix.evcache.service.transcoder;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.SerializingTranscoder;

/**
 * Created by senugula on 6/23/16.
 */
public class RESTServiceTranscoder extends SerializingTranscoder {

    static final int COMPRESSED = 2;
    public RESTServiceTranscoder() {

    }

    public boolean asyncDecode(CachedData d) {
        return false;
    }

    public CachedData decode(CachedData d) {
        if ((d.getFlags() & COMPRESSED) != 0) {
            d = new CachedData(d.getFlags(), super.decompress(d.getData()), d.MAX_SIZE);
        }
        return d;
    }

    public CachedData encode(CachedData o) {
        return o;
    }

    public int getMaxSize() {
        return CachedData.MAX_SIZE;
    }
}
