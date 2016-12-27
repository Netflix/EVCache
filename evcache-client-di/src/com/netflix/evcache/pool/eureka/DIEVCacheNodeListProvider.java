package com.netflix.evcache.pool.eureka;

import javax.inject.Provider;

import com.google.inject.Inject;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.evcache.pool.EVCacheNodeList;

public class DIEVCacheNodeListProvider implements Provider<EVCacheNodeList> {

    private final DiscoveryClient discoveryClient;
    private final ApplicationInfoManager applicationInfoManager;
    
    @Inject
    public DIEVCacheNodeListProvider(ApplicationInfoManager applicationInfoManager, DiscoveryClient discoveryClient) {
        this.applicationInfoManager = applicationInfoManager;
        this.discoveryClient = discoveryClient;
    }

    @Override
    public EVCacheNodeList get() {
        return new EurekaNodeListProvider(applicationInfoManager, discoveryClient);
    }

}
