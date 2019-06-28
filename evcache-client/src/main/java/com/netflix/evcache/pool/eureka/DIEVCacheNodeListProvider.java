package com.netflix.evcache.pool.eureka;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.discovery.EurekaClient;
import com.netflix.evcache.pool.EVCacheNodeList;
import com.netflix.evcache.pool.SimpleNodeListProvider;
import com.netflix.evcache.util.EVCacheConfig;

public class DIEVCacheNodeListProvider implements Provider<EVCacheNodeList> {

    private static Logger log = LoggerFactory.getLogger(DIEVCacheNodeListProvider.class);
    private final EurekaClient eurekaClient;
    private final ApplicationInfoManager applicationInfoManager;

    @Inject
    public DIEVCacheNodeListProvider(ApplicationInfoManager applicationInfoManager, EurekaClient eurekaClient) {
        this.applicationInfoManager = applicationInfoManager;
        this.eurekaClient = eurekaClient;
    }

    @Override
    public EVCacheNodeList get() {
        final EVCacheNodeList provider;
        if (EVCacheConfig.getInstance().getDynamicBooleanProperty("evcache.use.simple.node.list.provider", false).get()) {
            provider = new SimpleNodeListProvider();
        } else {
            provider = new EurekaNodeListProvider(applicationInfoManager, eurekaClient);
        }
        if(log.isDebugEnabled()) log.debug("EVCache Node List Provider : " + provider);
        return provider;
    }

}
