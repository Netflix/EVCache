package com.netflix.evcache.connection;

import javax.inject.Provider;

public class DefaultFactoryProvider implements Provider<IConnectionFactoryProvider> {

    @Override
    public ConnectionFactoryProvider get() {
        return new ConnectionFactoryProvider();
    }

}
