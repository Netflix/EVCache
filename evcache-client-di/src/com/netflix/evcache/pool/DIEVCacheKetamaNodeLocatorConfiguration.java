package com.netflix.evcache.pool;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.shared.Application;

import net.spy.memcached.MemcachedNode;

public class DIEVCacheKetamaNodeLocatorConfiguration extends EVCacheKetamaNodeLocatorConfiguration {

	private final DiscoveryClient discoveryClient;

    public DIEVCacheKetamaNodeLocatorConfiguration(EVCacheClient client, DiscoveryClient discoveryClient) {
        super(client);
        this.discoveryClient = discoveryClient;
    }

    /**
     * Returns the socket address of a given MemcachedNode.
     *
     * @param node - The MemcachedNode which we're interested in
     * @return The socket address of the given node format is of the following
     * format "publicHostname/privateIp:port" (ex -
     ec2-174-129-159-31.compute-1.amazonaws.com/10.125.47.114:11211)
     */
    @Override
    protected String getSocketAddressForNode(MemcachedNode node) {
        String result = socketAddresses.get(node);
        if(result == null) {
            final SocketAddress socketAddress = node.getSocketAddress();
            if(socketAddress instanceof InetSocketAddress) {
                final InetSocketAddress isa = (InetSocketAddress)socketAddress;
                if(discoveryClient != null ) {
                    final Application app = discoveryClient.getApplication(client.getAppName());
                    final List<InstanceInfo> instances = app.getInstances();
                    for(InstanceInfo info : instances) {
                        final String hostName = info.getHostName();
                        if(hostName.equalsIgnoreCase(isa.getHostName())) {
                            final String ip = info.getIPAddr();
                            final String port = info.getMetadata().get("evcache.port");
                            result = hostName + '/' + ip + ':' + ((port != null) ? port : "11211");
                            break;
                        }
                    }
                } else {
                    result = isa.getHostName() + '/' + isa.getAddress().getHostAddress() + ":11211";
                }
            } else {
                result=String.valueOf(socketAddress);
                if (result.startsWith("/")) {
                    result = result.substring(1);
                }
            }
            socketAddresses.put(node, result);
        }
        return result;
    }

    @Override
    public String toString() {
        return "DIEVCacheKetamaNodeLocatorConfiguration [" + super.toString() + ", DiscoveryClient=" + discoveryClient + "]";
    }
}