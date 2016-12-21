package com.netflix.evcache.connection;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

@Singleton
public class ConnectionModule extends AbstractModule {
    public ConnectionModule() {
    }

    @Override
    // Make sure this is done
    protected void configure() {
        bind(IConnectionFactoryProvider.class).toProvider(DefaultFactoryProvider.class); 
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
