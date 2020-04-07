package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;

public interface MetaGetOperation extends Operation {

    /**
     * Operation callback for the get request.
     */
    public interface Callback extends OperationCallback {
    	/**
         * Callback for each result from each meta data.
         *
         * @param key the key that was retrieved
         * @param flag all the flag
         * @param data the data for the flag
         */
    	void gotMetaData(String key, char flag, String data);
    	
        /**
         * Callback for result from a get.
         *
         * @param key the key that was retrieved
         * @param flag the flag for this value
         * @param data the data stored under this key
         */
    	void gotData(String key, int flag, byte[] data);
    }
}