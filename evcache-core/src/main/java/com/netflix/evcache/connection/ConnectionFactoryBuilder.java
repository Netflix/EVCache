package com.netflix.evcache.connection;

import com.netflix.config.DynamicIntProperty;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.ConnectionFactory;

public class ConnectionFactoryBuilder implements IConnectionBuilder {

    public ConnectionFactoryBuilder() {
    }

    public ConnectionFactory getConnectionFactory(EVCacheClient client) {
    	final String appName = client.getAppName();
        final int maxQueueSize = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".max.queue.length", 16384).get();
        final DynamicIntProperty operationTimeout = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".operation.timeout", 2500);
        final int opQueueMaxBlockTime = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".operation.QueueMaxBlockTime", 10).get();

        return new BaseConnectionFactory(client, maxQueueSize, operationTimeout, opQueueMaxBlockTime);
    }

}
