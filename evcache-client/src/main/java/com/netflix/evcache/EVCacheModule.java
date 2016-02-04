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

        // Make sure connection factory provider is initialized
        // bind(IConnectionFactoryProvider.class).toProvider(DefaultFactoryProvider.class);
    }
}
