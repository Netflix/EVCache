package com.netflix.evcache;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.netflix.evcache.pool.EVCacheClientPoolManager;

@Singleton
public class EVCacheModule extends AbstractModule {

    public EVCacheModule() {
    }

    @Override
    protected void configure() {
        bind(EVCacheClientPoolManager.class).asEagerSingleton();

        // Make sure connection factory provider Module is initialized in your Module when you init EVCacheModule 
        // bind(IConnectionFactoryProvider.class).toProvider(DefaultFactoryProvider.class);
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
