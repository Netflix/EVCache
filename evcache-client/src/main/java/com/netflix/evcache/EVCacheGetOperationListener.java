package com.netflix.evcache;

import net.spy.memcached.internal.GenericCompletionListener;

import com.netflix.evcache.operation.EVCacheOperationFuture;

public interface EVCacheGetOperationListener<T> extends GenericCompletionListener<EVCacheOperationFuture<T>> {

}
