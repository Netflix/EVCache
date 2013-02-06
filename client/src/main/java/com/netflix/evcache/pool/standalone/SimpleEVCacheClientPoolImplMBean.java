package com.netflix.evcache.pool.standalone;

import java.util.List;

public interface SimpleEVCacheClientPoolImplMBean {

    int getInstanceCount();
    
    List<String> getInstances();
    
    void refreshPool();
}