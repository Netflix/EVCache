package com.netflix.evcache.util;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import com.netflix.archaius.api.PropertyRepository;
import com.netflix.evcache.EVCacheModule.EVCacheModuleConfigLoader;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.tag.Tag;
import com.netflix.servo.tag.TagList;

public class EVCacheConfig {

    private static final EVCacheConfig INSTANCE = new EVCacheConfig();
    private final Map<String, MonitorConfig> monitorConfigMap = new HashMap<String, MonitorConfig>();
	private static PropertyRepository propertyRepository;

    private EVCacheConfig() {
    }

    public static EVCacheConfig getInstance() {
        return INSTANCE;
    }
    
    public PropertyRepository getPropertyRepository() {
    	return propertyRepository;
    }

    @Inject
    private static void ensurePropertiesLoaded(EVCacheModuleConfigLoader configLoader) {}
    
    @Inject
    private static void setPropertyRepository(PropertyRepository propertyRepository) {
		EVCacheConfig.propertyRepository = propertyRepository;
    }
    
    public MonitorConfig getMonitorConfig(final String metricName, final Tag tag) {
        return this.getMonitorConfig(metricName, tag, null);
    }

    public MonitorConfig getMonitorConfig(final String metricName, final Tag tag, final TagList tagList) {
        MonitorConfig mc = monitorConfigMap.get(metricName);
        if (mc != null) return mc;

        final MonitorConfig.Builder monitorConfig = MonitorConfig.builder(metricName);
        if (tagList != null) monitorConfig.withTags(tagList);
        if (tag != null) monitorConfig.withTag(tag);
        mc = monitorConfig.build();
        monitorConfigMap.put(metricName, mc);
        return mc;
    }

//    public DistributionSummary getDistributionSummary(String name) {
//        final Registry registry = Spectator.globalRegistry(); //_poolManager.getRegistry();
//        if (registry != null) {
//            final DistributionSummary ds = registry.distributionSummary(name);
//            if (!Monitors.isObjectRegistered(ds)) Monitors.registerObject(ds);
//            return ds;
//        }
//        return null;
//    }

}
