package com.netflix.evcache.pool.standalone;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import net.spy.memcached.MemcachedClient;

import com.netflix.config.DynamicIntProperty;
import com.netflix.evcache.pool.AbstractEVCacheClientImpl;

public class ZoneClusteredEVCacheClientImpl  extends AbstractEVCacheClientImpl {
    
    ZoneClusteredEVCacheClientImpl(String appName, String zone, int id, int maxQueueSize, 
    		DynamicIntProperty readTimeout, List<InetSocketAddress> memcachedNodesInZone) throws IOException {
    	super(appName, zone, id, maxQueueSize, readTimeout);
    	
		this.client = new MemcachedClient(connectionFactory, memcachedNodesInZone);
		this.client.setName(appName + "-" + zone + "-" + id);
    	
    }

	public String toString() {
        return "ZoneClusteredEVCacheClientImpl [" + super.toString() + "]";
    }
}