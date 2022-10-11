package com.netflix.evcache.pool;

import java.net.InetSocketAddress;
import java.util.Set;

public class EVCacheServerGroupConfig {

    private final ServerGroup serverGroup;
    private final Set<InetSocketAddress> inetSocketAddress;
    public EVCacheServerGroupConfig(ServerGroup serverGroup, Set<InetSocketAddress> inetSocketAddress) {
        super();
        this.serverGroup = serverGroup;
        this.inetSocketAddress = inetSocketAddress;
    }

    public ServerGroup getServerGroup() {
        return serverGroup;
    }

    public Set<InetSocketAddress> getInetSocketAddress() {
        return inetSocketAddress;
    }

    @Override
    public String toString() {
        return "EVCacheInstanceConfig [InetSocketAddress=" + inetSocketAddress + "]";
    }

}
