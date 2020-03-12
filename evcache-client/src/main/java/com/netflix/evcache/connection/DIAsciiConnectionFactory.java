package com.netflix.evcache.connection;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.archaius.api.Property;
import com.netflix.discovery.EurekaClient;
import com.netflix.evcache.pool.DIEVCacheKetamaNodeLocatorConfiguration;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheNodeLocator;

import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;

public class DIAsciiConnectionFactory extends BaseAsciiConnectionFactory {

	private static Logger log = LoggerFactory.getLogger(DIAsciiConnectionFactory.class);
    private final EurekaClient eurekaClient;

    DIAsciiConnectionFactory(EVCacheClient client, EurekaClient eurekaClient, int len, Property<Integer> operationTimeout, long opMaxBlockTime) {
        super(client, len, operationTimeout, opMaxBlockTime);
        this.eurekaClient = eurekaClient;
    	if(log.isInfoEnabled()) log.info("Using ASCII Connection Factory!!!");
    }

    @Override
    public NodeLocator createLocator(List<MemcachedNode> list) {
        this.locator = new EVCacheNodeLocator(client, list,  DefaultHashAlgorithm.KETAMA_HASH, new DIEVCacheKetamaNodeLocatorConfiguration(client, eurekaClient));
        return locator;
    }

}
