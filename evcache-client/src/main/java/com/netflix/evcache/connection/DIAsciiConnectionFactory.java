package com.netflix.evcache.connection;

import com.netflix.archaius.api.Property;
import com.netflix.discovery.EurekaClient;
import com.netflix.evcache.pool.DIEVCacheKetamaNodeLocatorConfiguration;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheNodeLocator;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;

import java.util.List;

public class DIAsciiConnectionFactory extends BaseAsciiConnectionFactory {

    private final EurekaClient eurekaClient;

    DIAsciiConnectionFactory(EVCacheClient client, EurekaClient eurekaClient, int len, Property<Integer> operationTimeout, long opMaxBlockTime) {
        super(client, len, operationTimeout, opMaxBlockTime);
        this.eurekaClient = eurekaClient;
    }

    @Override
    public NodeLocator createLocator(List<MemcachedNode> list) {
        this.locator = new EVCacheNodeLocator(client, list,  DefaultHashAlgorithm.KETAMA_HASH, new DIEVCacheKetamaNodeLocatorConfiguration(client, eurekaClient));
        return locator;
    }

}
