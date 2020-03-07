package com.netflix.evcache.connection;

import com.netflix.evcache.pool.EVCacheClient;

import net.spy.memcached.ConnectionFactory;

public interface IConnectionBuilder {

    ConnectionFactory getConnectionFactory(EVCacheClient client);

}