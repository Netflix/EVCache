package com.netflix.evcache.connection;

import com.netflix.archaius.api.Property;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.ConnectionFactory;

public class ConnectionFactoryBuilder implements IConnectionBuilder {

    public ConnectionFactoryBuilder() {
    }

    public ConnectionFactory getConnectionFactory(EVCacheClient client) {
    	final String appName = client.getAppName();
        final int maxQueueSize = EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".max.queue.length", Integer.class).orElse(16384).get();
        final Property<Integer> operationTimeout = EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".operation.timeout", Integer.class).orElse(2500);
        final int opQueueMaxBlockTime = EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".operation.QueueMaxBlockTime", Integer.class).orElse(10).get();
        final boolean useBinary = EVCacheConfig.getInstance().getPropertyRepository().get("evcache.use.binary.protocol", Boolean.class).orElse(true).get();

        if(useBinary) return new BaseConnectionFactory(client, maxQueueSize, operationTimeout, opQueueMaxBlockTime);
        else return new BaseAsciiConnectionFactory(client, maxQueueSize, operationTimeout, opQueueMaxBlockTime);
    }

}
