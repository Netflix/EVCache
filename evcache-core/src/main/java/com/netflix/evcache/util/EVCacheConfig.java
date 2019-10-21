package com.netflix.evcache.util;


import javax.inject.Inject;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.DefaultSettableConfig;
public class EVCacheConfig {

    private static EVCacheConfig INSTANCE;

    /**
     * This is an hack, should find a better way to do this 
     **/
    private static PropertyRepository propertyRepository;

	@Inject
    public EVCacheConfig(PropertyRepository repository) {
        propertyRepository = repository;
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

    public static void setPropertyRepository(PropertyRepository repository) {
        propertyRepository = repository;
    }

}
