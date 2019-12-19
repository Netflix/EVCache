package com.netflix.evcache.operation;

import net.spy.memcached.protocol.ascii.AsciiOperationFactory;
import net.spy.memcached.protocol.ascii.MetaDebugOperation;
import net.spy.memcached.protocol.ascii.MetaDebugOperationImpl;

public class EVCacheAsciiOperationFactory extends AsciiOperationFactory {


    public MetaDebugOperation metaDebug(String key, MetaDebugOperation.Callback cb) {
        return new MetaDebugOperationImpl(key, cb);
    }


}
