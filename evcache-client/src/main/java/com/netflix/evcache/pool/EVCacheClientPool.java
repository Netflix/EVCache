package com.netflix.evcache.pool;

/**
 * An EVCacheClientPool represents a pool or {@link EVCacheClient} instances.
 * A Pool is associated with an Application Name (a.k.a app). Each app is a grouping of EVCache servers to.
 * If this pool is zone based then each {@link EVCacheClient} is associated with a zone.
 * Thus a zone based app will have a cluster of servers for each zone.
 *
 * EX : An app which stores customer data having a name EVCACHE_CUST in AWS availability zones
 * us-east-1a and us-east-1b  with 3 servers in each zone will be as below
 * <pre>
 *                                 EVCAHCE_CUST
 *                                      ||
 *          =============================================================
 *         ||                                                           ||
 *     us-east-1a                                                  us-east-1b
 *          |                                                            |
 *          |- Server 1                                                  |- Server 4
 *          |                                                            |
 *          |- Server 2                                                  |- Server 5
 *          |                                                            |
 *          |- Server 3                                                  |- Server 6
 * </pre>
 *
 * Server 1,2,3 form a cluster in zone us-east-1a and the data will be sharded across these 3 instances.
 * Similarly, Server 4,5,6 form a cluster in zone us-east-1b and the data will be sharded across them.
 * The data is replicated among the 2 zones.
 */
public interface EVCacheClientPool {

    /**
     * Initialize the pool with the given name.
     *
     * @param appName - The name of the pool
     */
    void init(String appName);

    /**
     * Returns a EVCacheClient. If it is zone based then the an instance for the local zone is returned.
     * If there is no client for the local zone then a client at random is picked and returned.
     *
     * @return - The EVCacheClient depending on the type of pool implementation.
     */
    EVCacheClient getEVCacheClient();

    /**
     * Returns a EVCacheClient other than the zone passed.
     * If there is no client other than the passed zone then null is returned.
     *
     * @param zone - The zone to exclude
     * @return - The EVCacheClient in a zone other than the passed zone. If one does not exists for other zone then null is returned.
     */
    EVCacheClient getEVCacheClientExcludeZone(String zone);

    /**
     * Returns all EVCacheClient across all zones.
     * If there is no clients then null is returned.
     *
     * @return - The EVCacheClient array.
     */
    EVCacheClient[] getAllEVCacheClients();

    /**
     * Shutdown the pool.
     */
    void shutdown();

    /**
     * The size of the cluster. In a zone based implementation this is equal to number of zones that are part of this app.
     */
    int getClusterSize();

    /**
     * If the pool supports fallback or not.
     * @return true if the pool supports request fallback else false.
     */
    boolean supportsFallback();
}
