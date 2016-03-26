package com.netflix.evcache.sample;

import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheException;

import java.util.concurrent.Future;

/**
 * Created by senugula on 3/24/16.
 */

/**
 * This standalone program demonstrates how to use EVCacheClient for set/get operations using memcached running local box.
 * Prerequisite : ( Install memcached locally, most mac OS already have memcached installed.
 * Start memcached on local machine.
 * SERVERGROUP1:
 * memcached -d -p 11211
 * SERVERGROUP2:
 * memcached -d -p 11212
 *
 */
public class EVCacheClientSample {

    private final EVCache evCache;
    private final String key = "evcacheSample";
    private final String value = " This is my value1";
    private final int timeToLive = 100;

    public EVCacheClientSample() {

        /**
         * Enable node list provider
         * EVCACHE_APP1.use.simple.node.list.provider"
         *
         * Setup 2 server groups which is two memcached processes/nodes running at different ports acting different
         * server groups/auto-scaling groups for application EVCACHE_APP1
         * EVCACHE_APP1-NODES=SERVERGROUP1=localhost:11211;SERVERGROUP2=localhost:11212
         */
        System.setProperty("EVCACHE_APP1.use.simple.node.list.provider", "true");
        System.setProperty("EVCACHE_APP1-NODES", "SERVERGROUP1=localhost:11211;SERVERGROUP2=localhost:11212");
        evCache = new EVCache.Builder().setAppName("EVCACHE_APP1").build();
    }

    /**
     * Set a key in memcached
     * @throws Exception
     */

    public void setKey() throws Exception {
        try {
            Future<Boolean>[] _future = evCache.set(key, value, timeToLive);
            //Lets block for write so we don't exit the program fast
            for (Future<Boolean> f : _future) {
                System.out.println("set key " + key + " is successful" + f.get());
            }
        } catch (EVCacheException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the key you have set in  memcached
     * @return String
     */

    public String getKey() {
        try {
            String _response = evCache.<String>get(key);
            return _response;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *  Main Program to set and get using EVCacheClient
     * @param args
     */

    public static void main(String[] args) {
        try {
            EVCacheClientSample evCacheClientSample = new EVCacheClientSample();
            evCacheClientSample.setKey();
            Thread.sleep(1000);
            System.out.println(" Key returned is >> " + evCacheClientSample.getKey());
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
