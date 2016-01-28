package com.netflix.evcache;

import java.io.IOException;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.netflix.config.ConfigurationManager;
import com.netflix.discovery.guice.EurekaModule;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.governator.annotations.Modules;

@SuppressWarnings("deprecation")
@Singleton
@Modules(include={
        EurekaModule.class
})
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

        //Make sure connection factory provider is initialized
        //bind(IConnectionFactoryProvider.class).toProvider(DefaultFactoryProvider.class); 
    }
}
