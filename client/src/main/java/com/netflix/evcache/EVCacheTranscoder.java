package com.netflix.evcache;

import net.spy.memcached.transcoders.Transcoder;

/**
 * An interface for classes that manages the conversion of Java Objects to their serialized form. 
 */
public interface EVCacheTranscoder<T> extends Transcoder<T> {
	

}