package com.netflix.evcache.pool;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.BaseSerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

/**
 * A local transocder used only by EVCache client to ensure we don't try to deserialize chunks
 * 
 * @author smadappa
 *
 */
class ChunkTranscoder extends BaseSerializingTranscoder implements Transcoder<CachedData> {

	private static final int COMPRESSED = 2;

	public ChunkTranscoder() {
		super(Integer.MAX_VALUE);
    }

    public boolean asyncDecode(CachedData d) {
        return false;
    }

    public CachedData decode(CachedData d) {
        if ((d.getFlags() & COMPRESSED) != 0) {
             return new CachedData (d.getFlags(), decompress(d.getData()), Integer.MAX_VALUE);
          }
    	
        return d;
    }

    public CachedData encode(CachedData o) {
        return o;
    }

    public int getMaxSize() {
        return Integer.MAX_VALUE;
    }

}
