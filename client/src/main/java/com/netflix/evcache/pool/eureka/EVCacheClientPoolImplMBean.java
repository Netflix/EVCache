package com.netflix.evcache.pool.eureka;

import java.util.Map;

import javax.management.DescriptorKey;

public interface EVCacheClientPoolImplMBean {

    /**
     * Returns the number of {@link EVCache} instances in this pool.
     */
    @DescriptorKey("The number of EVCache instances(hosts) in this pool")
    int getInstanceCount();

    /**
     * The Map of instances by availability zone, where the key is zone and value is the comma separated list of instances in that zone.
     */
    @DescriptorKey("The Map of instances by availability zone, where the key is zone and value is the comma separated list of instances in that zone")
    Map<String, String> getInstancesByZone();

    /**
     * The Map of instance count by availability zone, where the key is zone and value is the number of instances in that zone.
     */
    @DescriptorKey("The Map of instance count by availability zone, where the key is zone and value is the number of instances in that zone")
    Map<String, Integer> getInstanceCountByZone();

    /**
     * The Map of instance by availability zone that can perform Read operations, where the key is zone
     * and value is the comma separated list of instances in that zone.
     */
    @DescriptorKey("The Map of instances by availability zone that can do read operations, where the key is zone and value is the comma"
                + "separated list of instances in that zone")
    Map<String, String> getReadZones();

    /**
     * The Map of instance count by availability zone that can perform Read operations, where the key is zone
     * and value is the count of instances in that zone.
     */
    @DescriptorKey("The Map of instances count by availability zone that can do read operations, where the key is zone and value "
            + "is the count of instances in that zone")
    Map<String, Integer> getReadInstanceCountByZone();

    /**
     * The Map of instance by availability zone that can perform write operations, where the key is zone
     * and value is the comma separated list of instances in that zone.
     */
    @DescriptorKey("The Map of instances by availability zone that can do write operations, where the key is zone and value is the comma"
                + "separated list of instances in that zone")
    Map<String, String> getWriteZones();

    /**
     * The Map of instance count by availability zone that can perform write operations, where the key is zone
     * and value is the count of instances in that zone.
     */
    @DescriptorKey("The Map of instances count by availability zone that can do write operations, where the key is zone and value "
            + "is the count of instances in that zone")
    Map<String, Integer> getWriteInstanceCountByZone();

    /**
     * Force refresh on the pool.
     */
    @DescriptorKey("Force refresh on the pool")
    void refreshPool();
}
