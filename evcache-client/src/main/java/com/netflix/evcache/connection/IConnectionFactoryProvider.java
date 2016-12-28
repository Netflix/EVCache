package com.netflix.evcache.connection;

import com.netflix.evcache.pool.EVCacheClient;

import net.spy.memcached.ConnectionFactory;

public interface IConnectionFactoryProvider {

    ConnectionFactory getConnectionFactory(EVCacheClient client);

}