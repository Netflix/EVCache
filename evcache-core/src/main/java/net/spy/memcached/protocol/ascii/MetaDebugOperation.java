package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;

public interface MetaDebugOperation extends Operation {

    /**
     * Operation callback for the get request.
     */
    public interface Callback extends OperationCallback {
      /**
       * Callback for each result from a get.
       *
       * @param key the key that was retrieved
       * @param flags the flags for this value
       * @param data the data stored under this key
       */
      void debugInfo(String key, String val);
    }
  }