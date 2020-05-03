package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;

public interface ExecCmdOperation extends Operation {

    /**
     * Callback for cmd operation.
     */
    interface Callback extends OperationCallback {
    }
  }
