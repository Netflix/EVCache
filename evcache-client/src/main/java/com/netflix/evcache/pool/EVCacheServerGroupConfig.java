package com.netflix.evcache.pool;

import java.net.InetSocketAddress;
import java.util.Set;

public class EVCacheServerGroupConfig {

    private final ServerGroup serverGroup;
    private final Set<InetSocketAddress> inetSocketAddress;
    private final int rendPort;
    private final int rendMemcachedPort;
    private final int rendMememtoPort;
    public EVCacheServerGroupConfig(ServerGroup serverGroup, Set<InetSocketAddress> inetSocketAddress, int rendPort, int rendMemcachedPort, int rendMememtoPort) {
        super();
        this.serverGroup = serverGroup;
        this.inetSocketAddress = inetSocketAddress;
        this.rendPort = rendPort;
        this.rendMemcachedPort = rendMemcachedPort;
        this.rendMememtoPort = rendMememtoPort;
    }

    public ServerGroup getServerGroup() {
        return serverGroup;
    }

    public Set<InetSocketAddress> getInetSocketAddress() {
        return inetSocketAddress;
    }

    public int getRendPort() {
        return rendPort;
    }

    public int getRendMemcachedPort() {
        return rendMemcachedPort;
    }

    public int getRendMememtoPort() {
        return rendMememtoPort;
    }
    
    public boolean isRendInstance() {
        return ( rendPort !=0 );
    }

    @Override
    public String toString() {
        return "EVCacheInstanceConfig [InetSocketAddress=" + inetSocketAddress + ", rendPort=" + rendPort
                + ", rendMemcachedPort=" + rendMemcachedPort + ", rendMememtoPort=" + rendMememtoPort + "]";
    }

}
