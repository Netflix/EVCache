package com.netflix.evcache.pool.standalone;

import java.util.Map;

public interface ZoneClusteredEVCacheClientPoolImplMBean {

    int getInstanceCount();
    
    Map<String, String> getInstancesByZone();
    
    Map<String, Integer> getInstanceCountByZone();

    Map<String, String> getReadZones();
    
    Map<String, Integer> getReadInstanceCountByZone();
    
    Map<String, String> getWriteZones();
    
    Map<String, Integer> getWriteInstanceCountByZone();
    
    void refreshPool();
}