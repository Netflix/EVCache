package com.netflix.evcache.service.transcoder;

import com.netflix.evcache.EVCacheTranscoder;

import net.spy.memcached.CachedData;

/**
 * Created by senugula on 6/23/16.
 */
public class RESTServiceTranscoder extends EVCacheTranscoder {

    static final int COMPRESSED = 2;
    public RESTServiceTranscoder() {
    }

    public boolean asyncDecode(CachedData d) {
        return false;
    }

    public CachedData decode(CachedData d) {
        if ((d.getFlags() & COMPRESSED) != 0) {
            d = new CachedData(d.getFlags() & ~COMPRESSED, super.decompress(d.getData()), d.MAX_SIZE);
        }
        return d;
    }

    public CachedData encode(CachedData o) {
    	if(o.getData().length > compressionThreshold) {
    		return new CachedData(o.getFlags() | COMPRESSED, compress(o.getData()), o.MAX_SIZE);
    	}
        return o;
    }

    public int getMaxSize() {
        return CachedData.MAX_SIZE;
    }
}
