package com.netflix.evcache.pool.eureka;

import java.util.Map;

import javax.management.DescriptorKey;

public interface EVCacheClientPoolImplMBean {

	@DescriptorKey("The number of EVCache instances(hosts) in this pool")
    int getInstanceCount();
    
	@DescriptorKey("The Map of instances by availablity zone, where the key is zone and value is the comma separed list of instances in that zone")
    Map<String, String> getInstancesByZone();
    
	@DescriptorKey("The Map of instance count by availablity zone, where the key is zone and value is the number of instances in that zone")
    Map<String, Integer> getInstanceCountByZone();

    Map<String, String> getReadZones();
    
    Map<String, Integer> getReadInstanceCountByZone();
    
    Map<String, String> getWriteZones();
    
    Map<String, Integer> getWriteInstanceCountByZone();
    
    void refreshPool();
}