package com.netflix.evcache.operation;

import net.spy.memcached.protocol.ascii.AsciiOperationFactory;
import net.spy.memcached.protocol.ascii.ExecCmdOperation;
import net.spy.memcached.protocol.ascii.ExecCmdOperationImpl;
import net.spy.memcached.protocol.ascii.MetaDebugOperation;
import net.spy.memcached.protocol.ascii.MetaDebugOperationImpl;
import net.spy.memcached.protocol.ascii.MetaGetOperation;
import net.spy.memcached.protocol.ascii.MetaGetOperationImpl;
import net.spy.memcached.protocol.ascii.MetaArithmeticOperationImpl;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.MutatorOperation;
import net.spy.memcached.ops.OperationCallback;


public class EVCacheAsciiOperationFactory extends AsciiOperationFactory {


    public MetaDebugOperation metaDebug(String key, MetaDebugOperation.Callback cb) {
        return new MetaDebugOperationImpl(key, cb);
    }

    public MetaGetOperation metaGet(String key, MetaGetOperation.Callback cb) {
        return new MetaGetOperationImpl(key, cb);
    }

    public ExecCmdOperation execCmd(String cmd, ExecCmdOperation.Callback cb) {
        return new ExecCmdOperationImpl(cmd, cb);
      }

    public MutatorOperation mutate(Mutator m, String key, long by, long def,
                                   int exp, OperationCallback cb) {
        return new MetaArithmeticOperationImpl(m, key, by, def, exp, cb);
    }

}
