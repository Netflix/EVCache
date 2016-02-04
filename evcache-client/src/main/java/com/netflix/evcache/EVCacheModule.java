package com.netflix.evcache;

import java.io.IOException;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.netflix.config.ConfigurationManager;
import com.netflix.evcache.pool.EVCacheClientPoolManager;

@Singleton
public class EVCacheModule extends AbstractModule {

    public EVCacheModule() {
    }

    @Override
    protected void configure() {
        try {
            ConfigurationManager.loadAppOverrideProperties("evcache");
        } catch (IOException e) {
            e.printStackTrace();
        }
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
