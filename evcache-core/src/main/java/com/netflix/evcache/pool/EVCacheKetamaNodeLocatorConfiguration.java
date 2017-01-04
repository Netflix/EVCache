package com.netflix.evcache.pool;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import com.netflix.config.ChainedDynamicProperty;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.util.DefaultKetamaNodeLocatorConfiguration;

public class EVCacheKetamaNodeLocatorConfiguration extends DefaultKetamaNodeLocatorConfiguration {

    protected final EVCacheClient client;
    protected final ChainedDynamicProperty.IntProperty bucketSize;

    public EVCacheKetamaNodeLocatorConfiguration(EVCacheClient client) {
    	this.client = client;
        bucketSize = EVCacheConfig.getInstance().getChainedIntProperty(client.getAppName() + "." + client.getServerGroupName() + ".bucket.size", client.getAppName()+ ".bucket.size", super.getNodeRepetitions());
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
     *  For ec2 classic instances - "publicHostname/privateIp:port" (ex - ec2-174-129-159-31.compute-1.amazonaws.com/10.125.47.114:11211)
     *  For ec2 vpc instances - "privateIp/privateIp:port" (ex - 10.125.47.114/10.125.47.114:11211)
     *  privateIp is also known as local ip 
     */
    @Override
    protected String getSocketAddressForNode(MemcachedNode node) {
        String result = socketAddresses.get(node);
        if(result == null) {
            final SocketAddress socketAddress = node.getSocketAddress();
            if(socketAddress instanceof InetSocketAddress) {
                final InetSocketAddress isa = (InetSocketAddress)socketAddress;
                result = isa.getHostName() + '/' + isa.getAddress().getHostAddress() + ":11211";
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
        return "EVCacheKetamaNodeLocatorConfiguration [EVCacheClient=" + client + ", BucketSize=" + getNodeRepetitions() + "]";
    }
}