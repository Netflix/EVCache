package com.netflix.evcache.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.config.ChainedDynamicProperty;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicLongProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.config.DynamicStringSetProperty;
import com.netflix.config.Property;

public class EVCacheConfig {

    private static final EVCacheConfig INSTANCE = new EVCacheConfig();
    private final Map<String, Property<?>> fastPropMap = new ConcurrentHashMap<String, Property<?>>();
//    private final Map<String, MonitorConfig> monitorConfigMap = new HashMap<String, MonitorConfig>();

    private EVCacheConfig() {
    }

    public static EVCacheConfig getInstance() {
        return INSTANCE;
    }

    public DynamicIntProperty getDynamicIntProperty(String name, int defaultValue) {
        DynamicIntProperty prop = (DynamicIntProperty) fastPropMap.get(name);
        if (prop != null) return prop;

        prop = DynamicPropertyFactory.getInstance().getIntProperty(name, defaultValue);
        fastPropMap.put(name, prop);
        return prop;
    }

    public DynamicLongProperty getDynamicLongProperty(String name, long defaultValue) {
        DynamicLongProperty prop = (DynamicLongProperty) fastPropMap.get(name);
        if (prop != null) return prop;

        prop = DynamicPropertyFactory.getInstance().getLongProperty(name, defaultValue);
        fastPropMap.put(name, prop);
        return prop;
    }

    public DynamicStringProperty getDynamicStringProperty(String name, String defaultValue) {
        DynamicStringProperty prop = (DynamicStringProperty) fastPropMap.get(name);
        if (prop != null) return prop;

        prop = DynamicPropertyFactory.getInstance().getStringProperty(name, defaultValue);
        fastPropMap.put(name, prop);
        return prop;
    }

    public DynamicBooleanProperty getDynamicBooleanProperty(String name, Boolean defaultValue) {
        DynamicBooleanProperty prop = (DynamicBooleanProperty) fastPropMap.get(name);
        if (prop != null) return prop;

        prop = DynamicPropertyFactory.getInstance().getBooleanProperty(name, defaultValue);
        fastPropMap.put(name, prop);
        return prop;
    }
    
    public DynamicStringSetProperty getDynamicStringSetProperty(String propertyName, String defaultValue) {
        DynamicStringSetProperty prop = (DynamicStringSetProperty) fastPropMap.get(propertyName);
        if (prop != null) return prop;
        prop = new DynamicStringSetProperty(propertyName, defaultValue);
        fastPropMap.put(propertyName, prop);
        return prop;
    }

    public ChainedDynamicProperty.BooleanProperty getChainedBooleanProperty(String overrideKey, String primaryKey, Boolean defaultValue, Runnable listener) {
        final String mapKey = overrideKey + primaryKey;
        ChainedDynamicProperty.BooleanProperty prop = (ChainedDynamicProperty.BooleanProperty) fastPropMap.get(mapKey);
        if (prop != null) return prop;

        final ChainedDynamicProperty.DynamicBooleanPropertyThatSupportsNull baseProperty = new ChainedDynamicProperty.DynamicBooleanPropertyThatSupportsNull(primaryKey, defaultValue);
        prop = new ChainedDynamicProperty.BooleanProperty(overrideKey, baseProperty);
        fastPropMap.put(mapKey, prop);
        if(listener != null) {
            baseProperty.addCallback(listener);
            prop.addCallback(listener);
        }
        return prop;
    }

    public ChainedDynamicProperty.IntProperty getChainedIntProperty(String overrideKey, String primaryKey, int defaultValue, Runnable listener) {
        final String mapKey = overrideKey + primaryKey;
        ChainedDynamicProperty.IntProperty prop = (ChainedDynamicProperty.IntProperty) fastPropMap.get(mapKey);
        if (prop != null) return prop;

        final DynamicIntProperty baseProp = new DynamicIntProperty(primaryKey, defaultValue);
        prop = new ChainedDynamicProperty.IntProperty(overrideKey, baseProp);
        fastPropMap.put(mapKey, prop);
        if(listener != null) {
            baseProp.addCallback(listener);
            prop.addCallback(listener);
        }
        return prop;
    }

    public ChainedDynamicProperty.StringProperty getChainedStringProperty(String overrideKey, String primaryKey,String defaultValue, Runnable listener) {
        final String mapKey = overrideKey + primaryKey;
        ChainedDynamicProperty.StringProperty prop = (ChainedDynamicProperty.StringProperty) fastPropMap.get(mapKey);
        if (prop != null) return prop;

        final DynamicStringProperty baseProp = new DynamicStringProperty(primaryKey,defaultValue);
        prop = new ChainedDynamicProperty.StringProperty(overrideKey, baseProp);
        fastPropMap.put(mapKey, prop);
        if(listener != null) {
            baseProp.addCallback(listener);
            prop.addCallback(listener);
        }
        return prop;
    }

//    public MonitorConfig getMonitorConfig(final String metricName, final Tag tag) {
//        return this.getMonitorConfig(metricName, tag, null);
//    }
//
//    public MonitorConfig getMonitorConfig(final String metricName, final Tag tag, final TagList tagList) {
//        MonitorConfig mc = monitorConfigMap.get(metricName);
//        if (mc != null) return mc;
//
//        final MonitorConfig.Builder monitorConfig = MonitorConfig.builder(metricName);
//        if (tagList != null) monitorConfig.withTags(tagList);
//        if (tag != null) monitorConfig.withTag(tag);
//        mc = monitorConfig.build();
//        monitorConfigMap.put(metricName, mc);
//        return mc;
//    }
//
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
