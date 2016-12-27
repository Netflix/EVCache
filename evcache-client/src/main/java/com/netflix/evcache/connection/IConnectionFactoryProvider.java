package com.netflix.evcache.connection;

import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;

import net.spy.memcached.ConnectionFactory;

public interface IConnectionFactoryProvider {

    ConnectionFactory getConnectionFactory(String appName, int id, ServerGroup serverGroup, EVCacheClient client);

}