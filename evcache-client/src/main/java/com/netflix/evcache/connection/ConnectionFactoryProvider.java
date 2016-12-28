package com.netflix.evcache.connection;

import com.netflix.config.ConfigurationManager;
import com.netflix.evcache.pool.EVCacheClient;

import net.spy.memcached.ConnectionFactory;

public class ConnectionFactoryProvider implements IConnectionFactoryProvider {

    public ConnectionFactoryProvider() {
    }

    public ConnectionFactory getConnectionFactory(EVCacheClient client) {
    	final String appName = client.getAppName();
        final int maxQueueSize = ConfigurationManager.getConfigInstance().getInt(appName + ".max.queue.length", 16384);
        final int operationTimeout = ConfigurationManager.getConfigInstance().getInt(appName + ".operation.timeout", 2500);
        final int opQueueMaxBlockTime = ConfigurationManager.getConfigInstance().getInt(appName + ".operation.QueueMaxBlockTime", 10);

        return new BaseConnectionFactory(client, maxQueueSize, operationTimeout, opQueueMaxBlockTime);
    }

}
