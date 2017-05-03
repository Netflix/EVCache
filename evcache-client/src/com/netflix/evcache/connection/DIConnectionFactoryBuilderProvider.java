package com.netflix.evcache.connection;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.config.DynamicIntProperty;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.ConnectionFactory;

public class DIConnectionFactoryBuilderProvider extends ConnectionFactoryBuilder implements Provider<IConnectionBuilder> {

    private final DiscoveryClient discoveryClient;

    @Inject
    public DIConnectionFactoryBuilderProvider(DiscoveryClient discoveryClient) {
        this.discoveryClient = discoveryClient;
    }
    @Override
    public ConnectionFactoryBuilder get() {
        return this;
    }

    @Override
    public ConnectionFactory getConnectionFactory(EVCacheClient client) {
        final String appName = client.getAppName();
        final int maxQueueSize = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".max.queue.length", 16384).get();
        final DynamicIntProperty operationTimeout = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".operation.timeout", 2500);
        final int opQueueMaxBlockTime = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".operation.QueueMaxBlockTime", 10).get();

        return new DIConnectionFactory(client, discoveryClient, maxQueueSize, operationTimeout, opQueueMaxBlockTime);
    }

}
