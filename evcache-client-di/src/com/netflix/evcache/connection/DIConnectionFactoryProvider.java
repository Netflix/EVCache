package com.netflix.evcache.connection;

import com.google.inject.Provider;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.ConnectionFactory;

public class DIConnectionFactoryProvider extends ConnectionFactoryProvider implements Provider<IConnectionFactoryProvider> {

    @Override
    public ConnectionFactoryProvider get() {
        return this;
    }

    @Override
    public ConnectionFactory getConnectionFactory(String appName, int id, ServerGroup serverGroup, EVCacheClient client) {
        final int maxQueueSize = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".max.queue.length", 16384).get();
        final int operationTimeout = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".operation.timeout", 2500).get();
        final int opQueueMaxBlockTime = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".operation.QueueMaxBlockTime", 10).get();

        return new BaseConnectionFactory(appName, maxQueueSize, operationTimeout, opQueueMaxBlockTime, id, serverGroup, client);
    }

}
