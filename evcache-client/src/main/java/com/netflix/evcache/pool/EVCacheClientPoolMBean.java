package com.netflix.evcache.pool;

import java.util.Map;

public interface EVCacheClientPoolMBean {

    int getInstanceCount();

    Map<String, String> getInstancesByZone();

    Map<String, Integer> getInstanceCountByZone();

    Map<String, String> getReadZones();

    Map<String, Integer> getReadInstanceCountByZone();

    Map<String, String> getWriteZones();

    Map<String, Integer> getWriteInstanceCountByZone();

    String getFallbackServerGroup();

    Map<String, String> getReadServerGroupByZone();

    String getLocalServerGroupCircularIterator();

    void refreshPool();

    String getPoolDetails();

}