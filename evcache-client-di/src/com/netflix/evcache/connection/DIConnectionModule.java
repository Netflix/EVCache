package com.netflix.evcache.connection;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

@Singleton
public class DIConnectionModule extends AbstractModule {

    public DIConnectionModule() {
    }

    @Override
    // Make sure this is done
    protected void configure() {
        if(getProvider(IConnectionBuilder.class) == null) bind(IConnectionBuilder.class).toProvider(DIConnectionFactoryBuilderProvider.class);
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
