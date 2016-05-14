package com.netflix.evcache.sample;

import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheException;

import java.util.concurrent.Future;

/**
 * Created by senugula on 3/24/16.
 * Updated by akpratt on 5/13/16.
 */

/**
 * This standalone program demonstrates how to use EVCacheClient for
 * set/get operations using memcached running on your local box.
 *
 * By default, this program expects there to be two memcached processes
 * on the local host, on ports 11211 and 11212. They get used as two
 * replicas of a single shard each.
 * 
 * You can override this configuration by setting the environment
 * variable EVC_SAMPLE_DEPLOYMENT to a string which describes your
 * deployment. The format for that string is as described in the EVCache
 * documentation for a simple node list provider. It would look like
 * this for a two-replica deployment with two shards per replica:
 *
 *   SERVERGROUP1=host1:port1,host2:port2;SERVERGROUP2=host3:port3,host4:port4
 */

public class EVCacheClientSample {

    private final EVCache evCache;
    private static boolean verboseMode = false;

    /**
     * Default constructor.
     *
     * This tells the EVCache library to use the "simple node list
     * provider" for EVCACHE_APP1 (by setting the relevant system
     * property), and then  it copies the EVC_SAMPLE_DEPLOYMENT
     * environment variable to the EVCACHE_APP1-NODES system property.
     *
     * If the environment variable isn't set, default is two shards on
     * localhost, on port 11211 and 11212, configured as two replicas with
     * one shard each.
     *
     * Finally, this initializes "evCache" using EVCache.Builder,
     * specifying the application name "EVCACHE_APP1."
     */
    public EVCacheClientSample() {
        String deploymentDescriptor = System.getenv("EVC_SAMPLE_DEPLOYMENT");
        if (deploymentDescriptor == null) {
            // No deployment descriptor in the environment, use a default: two local
            // memcached processes configured as two replicas of one shard each.
            deploymentDescriptor = "SERVERGROUP1=localhost:11211;SERVERGROUP2=localhost:11212";
        }
        System.setProperty("EVCACHE_APP1.use.simple.node.list.provider", "true");
        System.setProperty("EVCACHE_APP1-NODES", deploymentDescriptor);
        evCache = new EVCache.Builder().setAppName("EVCACHE_APP1").build();
    }

    /**
     * Set a key in the cache.
     *
     * See the memcached documentation for what "timeToLive" means.
     * Zero means "never expires."
     * Small integers (under some threshold) mean "expires this many seconds from now."
     * Large integers mean "expires at this Unix timestamp" (seconds since 1/1/1970).
     * Warranty expires 17-Jan 2038.
     */

    public void setKey(String key, String value, int timeToLive) throws Exception {
        try {
            Future<Boolean>[] _future = evCache.set(key, value, timeToLive);

            // Wait for all the Futures to complete.
            // In "verbose" mode, show the status for each.
            for (Future<Boolean> f : _future) {
            	boolean didSucceed = f.get();
            	if (verboseMode) {
                    System.out.println("per-shard set success code for key " + key + " is " + didSucceed);
                }
            }
            if (!verboseMode) {
                // Not verbose. Just give one line of output per "set," without a success code
                System.out.println("finished setting key " + key);
            }
        } catch (EVCacheException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the data for a key from the cache. Returns null if the key
     * could not be retrieved, whether due to a cache miss or errors.
     */

    public String getKey(String key) {
        try {
            String _response = evCache.<String>get(key);
            return _response;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Main Program which does some simple sets and gets.
     */

    public static void main(String[] args) {
        // set verboseMode based on the environment variable
        verboseMode = ("true".equals(System.getenv("EVCACHE_SAMPLE_VERBOSE")));

        if (verboseMode) {
            System.out.println("To run this sample app without using Gradle:");
            System.out.println("java -cp " + System.getProperty("java.class.path") + " com.netflix.evcache.sample.EVCacheClientSample");
        }

        try {
            EVCacheClientSample evCacheClientSample = new EVCacheClientSample();

            // Set ten keys to different values
            for (int i = 0; i < 10; i++) {
                String key = "key_" + i;
                String value = "data_" + i;
                // Set the TTL to 24 hours
                int ttl = 86400;
                evCacheClientSample.setKey(key, value, ttl);
            }

            // Do a "get" for each of those same keys
            for (int i = 0; i < 10; i++) {
                String key = "key_" + i;
                String value = evCacheClientSample.getKey(key);
                System.out.println("Get of " + key + " returned " + value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // We have to call System.exit() now, because some background
        // threads were started without the "daemon" flag. This is
        // probably a mistake somewhere, but hey, this is only a sample app.
	System.exit(0);
    }
}
