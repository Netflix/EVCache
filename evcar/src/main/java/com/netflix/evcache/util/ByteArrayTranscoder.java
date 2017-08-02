package com.netflix.evcache.util;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

public class ByteArrayTranscoder implements Transcoder<CachedData> {
	
	public final int MAX_SIZE = 1024*1024*1024 - 1;
	
	public ByteArrayTranscoder() {	}
	
	public boolean asyncDecode(CachedData d) {
		return false;
	}

	public CachedData decode(CachedData d) {
		return d;
	}

	public CachedData encode(CachedData data) {
		return data;
	}
	
	public int getMaxSize() {
		return MAX_SIZE;
	}

}
