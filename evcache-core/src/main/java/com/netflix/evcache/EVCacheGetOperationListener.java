package com.netflix.evcache;

import com.netflix.evcache.operation.EVCacheOperationFuture;

import net.spy.memcached.internal.GenericCompletionListener;

public interface EVCacheGetOperationListener<T> extends GenericCompletionListener<EVCacheOperationFuture<T>> {

}
