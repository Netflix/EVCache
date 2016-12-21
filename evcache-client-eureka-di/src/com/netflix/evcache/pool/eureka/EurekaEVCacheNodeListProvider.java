package com.netflix.evcache.pool.eureka;

import javax.inject.Provider;

import com.google.inject.Inject;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.evcache.pool.EVCacheNodeList;

public class EurekaEVCacheNodeListProvider implements Provider<EVCacheNodeList> {

    private final DiscoveryClient discoveryClient;
    private final ApplicationInfoManager applicationInfoManager;
    
    @Inject
    public EurekaEVCacheNodeListProvider(ApplicationInfoManager applicationInfoManager, DiscoveryClient discoveryClient) {
        this.applicationInfoManager = applicationInfoManager;
        this.discoveryClient = discoveryClient;
    }

    @Override
    public EVCacheNodeList get() {
        return new EurekaNodeList(applicationInfoManager, discoveryClient);
    }

}
