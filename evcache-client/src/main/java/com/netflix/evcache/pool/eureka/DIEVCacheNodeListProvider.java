package com.netflix.evcache.pool.eureka;

import javax.inject.Inject;
import javax.inject.Provider;

import com.netflix.archaius.api.PropertyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.discovery.EurekaClient;
import com.netflix.evcache.pool.EVCacheNodeList;
import com.netflix.evcache.pool.SimpleNodeListProvider;

public class DIEVCacheNodeListProvider implements Provider<EVCacheNodeList> {

    private static final Logger log = LoggerFactory.getLogger(DIEVCacheNodeListProvider.class);
    private final EurekaClient eurekaClient;
    private PropertyRepository props;
    private final ApplicationInfoManager applicationInfoManager;

    @Inject
    public DIEVCacheNodeListProvider(ApplicationInfoManager applicationInfoManager, EurekaClient eurekaClient, PropertyRepository props) {
        this.applicationInfoManager = applicationInfoManager;
        this.eurekaClient = eurekaClient;
        this.props = props;
    }

    @Override
    public EVCacheNodeList get() {
        final EVCacheNodeList provider;
        if (props.get("evcache.use.simple.node.list.provider", Boolean.class).orElse(false).get()) {
            provider = new SimpleNodeListProvider();
        } else {
            provider = new EurekaNodeListProvider(applicationInfoManager, eurekaClient, props);
        }
        if(log.isDebugEnabled()) log.debug("EVCache Node List Provider : " + provider);
        return provider;
    }

}
