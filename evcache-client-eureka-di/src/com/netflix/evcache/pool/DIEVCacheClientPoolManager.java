package com.netflix.evcache.pool;


import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.evcache.connection.IConnectionFactoryProvider;

/**
 * A manager that holds Pools for each EVCache app. When this class is
 * initialized all the EVCache apps defined in the property evcache.appsToInit
 * will be initialized and added to the pool. If a service knows all the EVCache
 * app it uses, then it can define this property and pass a list of EVCache apps
 * that needs to be initialized.
 * 
 * An EVCache app can also be initialized by Injecting
 * <code>EVCacheClientPoolManager</code> and calling <code>    
 *      initEVCache(<app name>)
 *      </code>
 * 
 * This typically should be done in the client libraries that need to initialize
 * an EVCache app. For Example VHSViewingHistoryLibrary in its initLibrary
 * initializes EVCACHE_VH by calling
 * 
 * <pre>
 *      {@literal @}Inject
 *      public VHSViewingHistoryLibrary(EVCacheClientPoolManager instance,...) {
 *          ....
 *          instance.initEVCache("EVCACHE_VH");
 *          ...
 *      }
 * </pre>
 * 
 * @author smadappa
 *
 */
@SuppressWarnings("deprecation")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({ "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", "DM_CONVERT_CASE",
        "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD" })
@Singleton
public class DIEVCacheClientPoolManager extends EVCacheClientPoolManager {

    private final DiscoveryClient discoveryClient;
    private final ApplicationInfoManager applicationInfoManager;

    @Inject
    public DIEVCacheClientPoolManager(ApplicationInfoManager applicationInfoManager, DiscoveryClient discoveryClient, Provider<IConnectionFactoryProvider> connectionFactoryprovider, Provider<EVCacheNodeList> evcacheNodeListProvider) {
        super(connectionFactoryprovider.get(), evcacheNodeListProvider.get());
        this.applicationInfoManager = applicationInfoManager;
        this.discoveryClient = discoveryClient;
    }

    public ApplicationInfoManager getApplicationInfoManager() {
        return this.applicationInfoManager;
    }

    public DiscoveryClient getDiscoveryClient() {
        DiscoveryClient client = discoveryClient;
        if (client == null) client = DiscoveryManager.getInstance().getDiscoveryClient();
        return client;
    }

}
