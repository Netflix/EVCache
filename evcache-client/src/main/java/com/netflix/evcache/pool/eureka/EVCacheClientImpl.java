/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * An implementation of {@link com.netflix.evcache.pool.EVCacheClient} based on Eureka.
 * @author smadappa
 *
 */
public class EVCacheClientImpl  extends AbstractEVCacheClientImpl {

    private EVCacheConnectionObserver connectionObserver = null;

    /**
     * Creates an instances of {@link com.netflix.evcache.pool.EVCacheClient} for the given appName, zone, id,
     * readTimeout and the list of memcached nodes. An instance of connection factory ({@link EVCacheConnectionFactory})
     * with the given maxQueueSize and appName. Additionally a connection observer ({@link EVCacheConnectionObserver})
     * for the given appName, zone and id is also created which monitors
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
        super(appName, zone, id, readTimeout, new EVCacheConnectionFactory(appName, zone, id, maxQueueSize));

        this.client = new MemcachedClient(connectionFactory, memcachedNodesInZone);
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
