package com.netflix.evcache.pool;

import java.net.InetSocketAddress;
import java.util.Set;

public class EVCacheServerGroupConfig {

    private final ServerGroup serverGroup;
    private final Set<InetSocketAddress> inetSocketAddress;
    private final int rendPort;
    private final int udsproxyMemcachedPort;
    private final int updsproxyMememtoPort;
    public EVCacheServerGroupConfig(ServerGroup serverGroup, Set<InetSocketAddress> inetSocketAddress, int rendPort, int rendMemcachedPort, int rendMememtoPort) {
        super();
        this.serverGroup = serverGroup;
        this.inetSocketAddress = inetSocketAddress;
        this.rendPort = rendPort;
        this.udsproxyMemcachedPort = rendMemcachedPort;
        this.updsproxyMememtoPort = rendMememtoPort;
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

    public int getUdsproxyMemcachedPort() {
        return udsproxyMemcachedPort;
    }

    public int getUpdsproxyMememtoPort() {
        return updsproxyMememtoPort;
    }
    
    public boolean isRendInstance() {
        return ( rendPort !=0 );
    }

    @Override
    public String toString() {
        return "EVCacheInstanceConfig [InetSocketAddress=" + inetSocketAddress + ", rendPort=" + rendPort
                + ", rendMemcachedPort=" + udsproxyMemcachedPort + ", rendMememtoPort=" + updsproxyMememtoPort + "]";
    }

}
