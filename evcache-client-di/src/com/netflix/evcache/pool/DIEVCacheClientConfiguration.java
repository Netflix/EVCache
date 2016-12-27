package com.netflix.evcache.pool;


import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.evcache.connection.IConnectionFactoryProvider;

@Singleton
public class DIEVCacheClientConfiguration {

    private final DiscoveryClient discoveryClient;
    private final ApplicationInfoManager applicationInfoManager;
    private final EVCacheClientPoolManager manager;

    @Inject
    public DIEVCacheClientConfiguration(ApplicationInfoManager applicationInfoManager, DiscoveryClient discoveryClient, Provider<IConnectionFactoryProvider> connectionFactoryprovider, Provider<EVCacheNodeList> evcacheNodeListProvider, EVCacheClientPoolManager manager) {
        this.applicationInfoManager = applicationInfoManager;
        this.discoveryClient = discoveryClient;
        this.manager = manager;
    }

    public ApplicationInfoManager getApplicationInfoManager() {
        return this.applicationInfoManager;
    }

    public DiscoveryClient getDiscoveryClient() {
        return this.discoveryClient;
    }
    
    public EVCacheClientPoolManager getEVCacheClientPoolManager() {
    	return this.manager;
    }

}
