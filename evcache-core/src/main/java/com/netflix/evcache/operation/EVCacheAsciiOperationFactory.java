package com.netflix.evcache.operation;

import net.spy.memcached.protocol.ascii.AsciiOperationFactory;
import net.spy.memcached.protocol.ascii.MetaDebugOperation;
import net.spy.memcached.protocol.ascii.MetaDebugOperationImpl;
import net.spy.memcached.protocol.ascii.MetaGetOperation;
import net.spy.memcached.protocol.ascii.MetaGetOperationImpl;

public class EVCacheAsciiOperationFactory extends AsciiOperationFactory {


    public MetaDebugOperation metaDebug(String key, MetaDebugOperation.Callback cb) {
        return new MetaDebugOperationImpl(key, cb);
    }

    public MetaGetOperation metaGet(String key, MetaGetOperation.Callback cb) {
        return new MetaGetOperationImpl(key, cb);
    }


}
