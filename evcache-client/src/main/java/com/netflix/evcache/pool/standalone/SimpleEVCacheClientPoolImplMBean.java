package com.netflix.evcache.pool.standalone;

import java.util.List;

import javax.management.DescriptorKey;

public interface SimpleEVCacheClientPoolImplMBean {

    /**
     * Returns the number of {@link EVCache} instances in this pool.
     */
    @DescriptorKey("The number of EVCache instances(hosts) in this pool")
    int getInstanceCount();
    /**
     * The List of instances for this client.
     */
    @DescriptorKey("The List of instances for this client separated by comma")
    List<String> getInstances();

    /**
     * Force refresh on the pool.
     */
    @DescriptorKey("Force refresh on the pool")
    void refreshPool();
}
