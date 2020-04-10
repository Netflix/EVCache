package com.netflix.evcache.connection;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.discovery.EurekaClient;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.ConnectionFactory;

import javax.inject.Inject;
import javax.inject.Provider;

public class DIConnectionFactoryBuilderProvider extends ConnectionFactoryBuilder implements Provider<IConnectionBuilder> {
    private final EurekaClient eurekaClient;
    private final PropertyRepository props;

    @Inject
    public DIConnectionFactoryBuilderProvider(EurekaClient eurekaClient, PropertyRepository props) {
        this.eurekaClient = eurekaClient;
        this.props = props;
    }

    @Override
    public ConnectionFactoryBuilder get() {
        return this;
    }

    public int getMaxQueueLength(String appName) {
        return props.get(appName + ".max.queue.length", Integer.class).orElse(16384).get();
    }
    
    public int getOPQueueMaxBlockTime(String appName) {
        return props.get(appName + ".operation.QueueMaxBlockTime", Integer.class).orElse(10).get();
    }
    
    public Property<Integer> getOperationTimeout(String appName) {
        return props.get(appName + ".operation.timeout", Integer.class).orElse(2500);
    }

    public boolean useBinaryProtocol() {
        return EVCacheConfig.getInstance().getPropertyRepository().get("evcache.use.binary.protocol", Boolean.class).orElse(true).get();
    }

    public EurekaClient getEurekaClient() {
        return eurekaClient;
    }

    public PropertyRepository getProps() {
        return props;
    }

    @Override
    public ConnectionFactory getConnectionFactory(EVCacheClient client) {
        final String appName = client.getAppName();

        if(useBinaryProtocol()) 
            return new DIConnectionFactory(client, eurekaClient, getMaxQueueLength(appName), getOperationTimeout(appName), getOPQueueMaxBlockTime(appName));
        	else return new DIAsciiConnectionFactory(client, eurekaClient, getMaxQueueLength(appName), getOperationTimeout(appName), getOPQueueMaxBlockTime(appName));
    }

}
