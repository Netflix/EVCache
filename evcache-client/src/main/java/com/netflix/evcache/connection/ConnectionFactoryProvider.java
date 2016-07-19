package com.netflix.evcache.connection;

import com.netflix.config.ConfigurationManager;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.pool.ServerGroup;

import net.spy.memcached.ConnectionFactory;

public class ConnectionFactoryProvider implements IConnectionFactoryProvider {

    public ConnectionFactoryProvider() {
    }

    public ConnectionFactory getConnectionFactory(String appName, int id, ServerGroup serverGroup, EVCacheClientPoolManager poolManager) {

        final int maxQueueSize = ConfigurationManager.getConfigInstance().getInt(appName + ".max.queue.length", 16384);
        final int operationTimeout = ConfigurationManager.getConfigInstance().getInt(appName + ".operation.timeout", 2500);
        final int opQueueMaxBlockTime = ConfigurationManager.getConfigInstance().getInt(appName + ".operation.QueueMaxBlockTime", 10);

        return new BaseConnectionFactory(appName, maxQueueSize, operationTimeout, opQueueMaxBlockTime, id, serverGroup, poolManager);
    }

}
