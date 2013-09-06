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

package com.netflix.evcache.pool.standalone;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import net.spy.memcached.MemcachedClient;

import com.netflix.config.DynamicIntProperty;
import com.netflix.evcache.pool.AbstractEVCacheClientImpl;

/**
 * A Zone based implementation {@link com.netflix.evcache.pool.EVCacheClient} which performs operation on the given
 * list of memcached servers int the given zone configuration.
 * In this scenario all the servers are in the given availability zone.
 *
 * @author smadappa
 */
public class ZoneClusteredEVCacheClientImpl  extends AbstractEVCacheClientImpl {

    /**
     * Creates an instance of {@link com.netflix.evcache.pool.EVCacheClient} for the given app, zone, id, queue size, timeout and list of servers.
     *
     * @param appName - The name of the EVCache app.
     * @param zone - The zone this client belongs to.
     * @param id - The id for this client.
     * @param maxQueueSize - Max number of items in the queue.
     * @param readTimeout - The timeout for all read operations. The value can be dynamically changed.
     * @param memcachedNodesInZone - List of Servers that this client connects to.
     * @throws IOException - Exception while trying to establish the connection.
     */
    ZoneClusteredEVCacheClientImpl(String appName, String zone, int id, int maxQueueSize,
            DynamicIntProperty readTimeout, List<InetSocketAddress> memcachedNodesInZone) throws IOException {
        super(appName, zone, id, maxQueueSize, readTimeout);

        this.client = new MemcachedClient(connectionFactory, memcachedNodesInZone);
    }

    /**
     * String representation of this instance.
     */
    public String toString() {
        return "ZoneClusteredEVCacheClientImpl [" + super.toString() + "]";
    }
}
