package com.netflix.evcache.pool;

import java.io.IOException;
import java.net.UnknownServiceException;
import java.util.Map;

public interface EVCacheNodeList {

    /**
     * Discover memcached instances suitable for our use from the Discovery
     * Service.
     *
     * @throws UnknownServiceException
     *             if no suitable instances can be found
     * @throws IllegalStateException
     *             if an error occurred in the Discovery service
     *
     *  TODO : Add a fallback to get the list say from PersistedProperties
     */
    public abstract Map<ServerGroup, EVCacheServerGroupConfig> discoverInstances() throws IOException;

}