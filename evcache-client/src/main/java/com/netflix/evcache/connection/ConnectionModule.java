package com.netflix.evcache.connection;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

@Singleton
public class ConnectionModule extends AbstractModule {
    public ConnectionModule() {
    }

    @Override
    protected void configure() {
        bind(IConnectionFactoryProvider.class).toProvider(DefaultFactoryProvider.class); // Make
                                                                                         // sure
                                                                                         // this
                                                                                         // is
                                                                                         // done
    }
}
