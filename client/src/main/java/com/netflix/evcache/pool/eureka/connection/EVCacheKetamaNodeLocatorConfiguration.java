package com.netflix.evcache.pool.eureka.connection;

import java.net.InetSocketAddress;
import java.util.List;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.discovery.shared.Application;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.util.DefaultKetamaNodeLocatorConfiguration;

public class EVCacheKetamaNodeLocatorConfiguration extends DefaultKetamaNodeLocatorConfiguration {

    private final String appId;

    public EVCacheKetamaNodeLocatorConfiguration(String appId) {
        this.appId = appId;
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
        String result=socketAddresses.get(node);
        if(result == null) {
            if(node.getSocketAddress() instanceof InetSocketAddress) {
                final InetSocketAddress isa = (InetSocketAddress)node.getSocketAddress();
                final Application app = DiscoveryManager.getInstance().getDiscoveryClient().getApplication(appId);
                final List<InstanceInfo> instances = app.getInstances();
                for(InstanceInfo info : instances) {
                    if(info.getHostName().equalsIgnoreCase(isa.getHostName())) {
                        final String hostName = info.getHostName();
                        final String ip = info.getIPAddr();
                        final String port = info.getMetadata().get("evcache.port");
                        result = hostName + '/' + ip + ':' + ((port != null) ? port : "11211");
                    }
                }
            } else {
                result=String.valueOf(node.getSocketAddress());
                if (result.startsWith("/")) {
                    result = result.substring(1);
                }
            }
            socketAddresses.put(node, result);
        }
        return result;
    }
}