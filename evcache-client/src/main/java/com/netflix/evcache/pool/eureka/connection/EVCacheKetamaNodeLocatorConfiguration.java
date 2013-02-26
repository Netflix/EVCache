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

package com.netflix.evcache.pool.eureka.connection;

import java.net.InetSocketAddress;
import java.util.List;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.discovery.shared.Application;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.util.DefaultKetamaNodeLocatorConfiguration;

/**
 * A implementation of {@link net.spy.memcached.util.KetamaNodeLocatorConfiguration} that can handle EC2 address translation when
 * accessed outside EC2 environments.
 * @author smadappa
 */
public class EVCacheKetamaNodeLocatorConfiguration extends DefaultKetamaNodeLocatorConfiguration {

    private final String appName;

    /**
     * Creates an instance of KetamaNodeLocatorConfiguration for the given appName.
     * @param appName The name of the registered application.
     */
    public EVCacheKetamaNodeLocatorConfiguration(String appName) {
        this.appName = appName;
    }

    /**
     * Returns the socket address of a given MemcachedNode.
     *
     * @param node - The MemcachedNode which we're interested in
     * @return The socket address of the given node format is of the following
     *     format "publicHostname/privateIp:port" (ex - ec2-174-129-159-31.compute-1.amazonaws.com/10.125.47.114:11211)
     */
    @Override
    protected String getSocketAddressForNode(MemcachedNode node) {
        String result = socketAddresses.get(node);
        if (result == null) {
            if (node.getSocketAddress() instanceof InetSocketAddress) {
                final InetSocketAddress isa = (InetSocketAddress) node.getSocketAddress();
                final Application app = DiscoveryManager.getInstance().getDiscoveryClient().getApplication(appName);
                if (null == app) {
                    throw new IllegalStateException("No instances found for registered application");
                }
                final List<InstanceInfo> instances = app.getInstances();
                for (InstanceInfo info : instances) {
                    if (info.getHostName().equalsIgnoreCase(isa.getHostName())) {
                        final String hostName = info.getHostName();
                        final String ip = info.getIPAddr();
                        final String port = info.getMetadata().get("evcache.port");
                        result = hostName + '/' + ip + ':' + ((port != null) ? port : "11211");
                        break;
                    }
                }
            } else {
                result = String.valueOf(node.getSocketAddress());
                if (result.startsWith("/")) {
                    result = result.substring(1);
                }
            }
            socketAddresses.put(node, result);
        }
        return result;
    }
}
