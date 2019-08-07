package com.netflix.evcache.util;


import javax.inject.Inject;

import com.netflix.archaius.api.PropertyRepository;
public class EVCacheConfig {

    private static final EVCacheConfig INSTANCE = new EVCacheConfig();
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
    private static void setPropertyRepository(PropertyRepository propertyRepository) {
		EVCacheConfig.propertyRepository = propertyRepository;
    }
}
