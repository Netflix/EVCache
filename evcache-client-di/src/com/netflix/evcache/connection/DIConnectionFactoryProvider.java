package com.netflix.evcache.connection;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.ConnectionFactory;

public class DIConnectionFactoryProvider extends ConnectionFactoryProvider implements Provider<IConnectionFactoryProvider> {

	private final DiscoveryClient discoveryClient;
	@Inject
	public DIConnectionFactoryProvider(DiscoveryClient discoveryClient) {
		this.discoveryClient = discoveryClient;
	}
    @Override
    public ConnectionFactoryProvider get() {
        return this;
    }

    @Override
    public ConnectionFactory getConnectionFactory(EVCacheClient client) {
        final String appName = client.getAppName();
        final int maxQueueSize = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".max.queue.length", 16384).get();
        final int operationTimeout = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".operation.timeout", 2500).get();
        final int opQueueMaxBlockTime = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".operation.QueueMaxBlockTime", 10).get();

        return new DIConnectionFactory(client, discoveryClient, maxQueueSize, operationTimeout, opQueueMaxBlockTime);
    }

}
