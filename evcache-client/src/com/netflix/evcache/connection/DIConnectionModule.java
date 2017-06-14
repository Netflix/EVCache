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
//        if(getProvider(IConnectionBuilder.class) == null && getProvider(IConnectionBuilder.class) instanceof DIConnectionFactoryBuilderProvider) {
            bind(IConnectionBuilder.class).toProvider(DIConnectionFactoryBuilderProvider.class);
//        } else {
//            Provider<IConnectionBuilder> provider = getProvider(IConnectionBuilder.class);
//            System.out.println("provider = " + provider);
//            System.out.println("provider.get = " + provider.getClass());
//        }
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
