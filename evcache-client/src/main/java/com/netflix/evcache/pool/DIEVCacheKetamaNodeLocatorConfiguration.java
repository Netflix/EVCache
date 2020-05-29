package com.netflix.evcache.pool;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;
import net.spy.memcached.MemcachedNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

public class DIEVCacheKetamaNodeLocatorConfiguration extends EVCacheKetamaNodeLocatorConfiguration {

    private static final Logger log = LoggerFactory.getLogger(DIEVCacheKetamaNodeLocatorConfiguration.class);
    private final EurekaClient eurekaClient;

    public DIEVCacheKetamaNodeLocatorConfiguration(EVCacheClient client, EurekaClient eurekaClient) {
        super(client);
        this.eurekaClient = eurekaClient;
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
    public String getKeyForNode(MemcachedNode node, int repetition) {
        String result = socketAddresses.get(node);
        if(result == null) {
            final SocketAddress socketAddress = node.getSocketAddress();
            if(socketAddress instanceof InetSocketAddress) {
                final InetSocketAddress isa = (InetSocketAddress)socketAddress;
                if(eurekaClient != null ) {
                    final Application app = eurekaClient.getApplication(client.getAppName());
                    if(app != null) {
                        final List<InstanceInfo> instances = app.getInstances();
                        for(InstanceInfo info : instances) {
                            final String hostName = info.getHostName();
                            if(hostName.equalsIgnoreCase(isa.getHostName())) {
                                final String ip = info.getIPAddr();
                                result = hostName + '/' + ip + ":11211";
                                break;
                            }
                        }
                    } else {
                        result = ((InetSocketAddress)socketAddress).getHostName() + '/' + ((InetSocketAddress)socketAddress).getAddress().getHostAddress() + ":11211";
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
        if(log.isDebugEnabled()) log.debug("Returning : " + (result + "-" + repetition));
        return result + "-" + repetition;
    }

    @Override
    public String toString() {
        return "DIEVCacheKetamaNodeLocatorConfiguration [" + super.toString() + ", EurekaClient=" + eurekaClient + "]";
    }
}
