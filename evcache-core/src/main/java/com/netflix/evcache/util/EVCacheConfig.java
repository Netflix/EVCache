package com.netflix.evcache.util;


import javax.inject.Inject;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.DefaultSettableConfig;
public class EVCacheConfig {

    private static EVCacheConfig INSTANCE;
	private final PropertyRepository propertyRepository;

	@Inject
    public EVCacheConfig(PropertyRepository propertyRepository) {
        this.propertyRepository = propertyRepository;
        INSTANCE = this;
    }

    private EVCacheConfig() {
        this(new DefaultPropertyFactory(new DefaultSettableConfig()));
        System.err.println("\n\nNon Standard way of initializing EVCache. Creating an instance PropertyRepository in a non standard way. Please Inject EVCache.Builder\n\n");
    }

    public static EVCacheConfig getInstance() {
        if(INSTANCE == null) new EVCacheConfig();
        return INSTANCE;
    }

    public PropertyRepository getPropertyRepository() {
    	return propertyRepository;
    }

}
