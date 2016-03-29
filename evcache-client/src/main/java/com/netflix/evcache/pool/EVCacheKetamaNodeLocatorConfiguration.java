package com.netflix.evcache.pool;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.config.ChainedDynamicProperty;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.shared.Application;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.util.DefaultKetamaNodeLocatorConfiguration;

public class EVCacheKetamaNodeLocatorConfiguration extends DefaultKetamaNodeLocatorConfiguration {

    private final String appId;
    private final ServerGroup replicaSet;
    private final EVCacheClientPoolManager poolManager;

    private final ChainedDynamicProperty.IntProperty bucketSize;

    public EVCacheKetamaNodeLocatorConfiguration(String appId, ServerGroup serverGroup, EVCacheClientPoolManager poolManager) {
        this.appId = appId;
        this.replicaSet = serverGroup;
        this.poolManager = poolManager;
        bucketSize = EVCacheConfig.getInstance().getChainedIntProperty(appId + "." + serverGroup.getName() + ".bucket.size",appId + ".bucket.size", super.getNodeRepetitions());
//        bucketSize = new ChainedDynamicProperty.IntProperty(appId + "." + serverGroup.getName() + ".bucket.size",
//                new DynamicIntProperty(appId + ".bucket.size", super.getNodeRepetitions()));
    }

    /**
     * Returns the number of discrete hashes that should be defined for each
     * node in the continuum.
     *
     * @return NUM_REPS repetitions.
     */
    public int getNodeRepetitions() {
        return bucketSize.get().intValue();
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
                if(poolManager.getDiscoveryClient() != null ) {
                    final DiscoveryClient mgr = poolManager.getDiscoveryClient();
                    final Application app = mgr.getApplication(appId);
                    final List<InstanceInfo> instances = app.getInstances();
                    for(InstanceInfo info : instances) {
                        final String hostName = info.getHostName();
                        if(hostName.equalsIgnoreCase(isa.getHostName())) {
                            final String ip = info.getIPAddr();
                            final String port = info.getMetadata().get("evcache.port");
                            result = hostName + '/' + ip + ':' + ((port != null) ? port : "11211");
                        }
                    }
                } else {
                    result = ((InetSocketAddress)socketAddress).getHostName() + '/' +
                            ((InetSocketAddress)socketAddress).getPort();
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
        return "EVCacheKetamaNodeLocatorConfiguration [app=" + appId
                + ", ReplicaSet=" + replicaSet + ", BucketSize=" + getNodeRepetitions() + "]";
    }
}