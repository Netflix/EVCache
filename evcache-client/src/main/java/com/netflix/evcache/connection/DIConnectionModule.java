package com.netflix.evcache.connection;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

@Singleton
public class DIConnectionModule extends AbstractModule {

    public DIConnectionModule() {
    }

    @Override
    protected void configure() {
        bind(IConnectionBuilder.class).toProvider(DIConnectionFactoryBuilderProvider.class);
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
