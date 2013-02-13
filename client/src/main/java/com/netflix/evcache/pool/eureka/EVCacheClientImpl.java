package com.netflix.evcache.pool.eureka;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import net.spy.memcached.MemcachedClient;

import com.netflix.config.DynamicIntProperty;
import com.netflix.evcache.pool.AbstractEVCacheClientImpl;
import com.netflix.evcache.pool.eureka.connection.EVCacheConnectionFactory;
import com.netflix.evcache.pool.observer.EVCacheConnectionObserver;

/**
 * An implementation of {@link EVCacheClient} based on Eureka.
 * @author smadappa
 *
 */
public class EVCacheClientImpl  extends AbstractEVCacheClientImpl {

    private EVCacheConnectionObserver connectionObserver = null;

    /**
     * Creates an instances of {@link EVCacheClient} for the given appName, zone, id, readTimeout and the list of memcached nodes.
     * An instance of connection factory ({@link EVCacheConnectionFactory}) with the given maxQueueSize and appName.
     * Additionally a connection observer ({@link EVCacheConnectionObserver}) for the given appName, zone and id is also created which monitors
     * the connection between client and the memcached server.
     *
     * @param appName - The name of the EVCache app.
     * @param zone - The zone this client belongs to.
     * @param id - The id of this client.
     * @param maxQueueSize - Max number of items allowed in the queue
     * @param readTimeout - The timeout for all read operations
     * @param memcachedNodesInZone - The memcached nodes that are part of this client.
     * @throws IOException - The exception while trying to establish the connection with the hosts.
     *          Typically this can happen if the Security Groups are configured properly, the hosts don't exists.
     */
    EVCacheClientImpl(String appName, String zone, int id, int maxQueueSize, DynamicIntProperty readTimeout,
            List<InetSocketAddress> memcachedNodesInZone) throws IOException {
        super(appName, zone, id, readTimeout, new EVCacheConnectionFactory(appName, maxQueueSize));

        this.client = new MemcachedClient(connectionFactory, memcachedNodesInZone);
        this.client.setName(appName + "-" + zone + "-" + id);
        this.connectionObserver = new EVCacheConnectionObserver(appName, zone, id);
        this.client.addObserver(connectionObserver);
    }

    /**
     * {@inheritDoc}
     */
    public boolean removeConnectionObserver() {
        try {
            boolean removed = client.removeObserver(connectionObserver);
            if (removed) connectionObserver = null;
            return removed;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public EVCacheConnectionObserver getConnectionObserver() {
        return this.connectionObserver;
    }

    /**
     * String representation of this innstance.
     */
    public String toString() {
        return "EVCacheClientImpl [" + super.toString() + "]";
    }
}
