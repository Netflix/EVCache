package com.netflix.evcache;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.netflix.evcache.connection.ConnectionModule;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.pool.EVCacheNodeList;
import com.netflix.evcache.pool.eureka.DIEVCacheNodeListProvider;

@Singleton
public class EVCacheModule extends AbstractModule {

    public EVCacheModule() {
    }

    @Override
    protected void configure() {
        // Make sure connection factory provider Module is initialized in your Module when you init EVCacheModule 
        install(new ConnectionModule());
        bind(EVCacheNodeList.class).toProvider(DIEVCacheNodeListProvider.class);

        bind(EVCacheClientPoolManager.class).asEagerSingleton();
        //bind(DIEVCacheClientConfiguration.class).asEagerSingleton(); //TODO : remove this and remove the class
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
 
    @Override
    public boolean equals(Object obj) {
        return (obj != null) && (obj.getClass() == getClass());
    }

}