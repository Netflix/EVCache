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
    private PropertyRepository props;

    @Inject
    public DIConnectionFactoryBuilderProvider(EurekaClient eurekaClient, PropertyRepository props) {
        this.eurekaClient = eurekaClient;
        this.props = props;
    }

    @Override
    public ConnectionFactoryBuilder get() {
        return this;
    }

    @Override
    public ConnectionFactory getConnectionFactory(EVCacheClient client) {
        final String appName = client.getAppName();
        final int maxQueueSize = props.get(appName + ".max.queue.length", Integer.class).orElse(16384).get();
        final Property<Integer> operationTimeout = props.get(appName + ".operation.timeout", Integer.class).orElse(2500);
        final int opQueueMaxBlockTime = props.get(appName + ".operation.QueueMaxBlockTime", Integer.class).orElse(10).get();
        final boolean useBinary = EVCacheConfig.getInstance().getPropertyRepository().get("evcache.use.binary.protocol", Boolean.class).orElse(true).get();

        //if(useBinary) 
            return new DIConnectionFactory(client, eurekaClient, maxQueueSize, operationTimeout, opQueueMaxBlockTime);
        //else return new DIAsciiConnectionFactory(client, eurekaClient, maxQueueSize, operationTimeout, opQueueMaxBlockTime);

        
    }

}
