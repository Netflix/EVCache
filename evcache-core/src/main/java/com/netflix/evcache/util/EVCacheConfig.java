package com.netflix.evcache.util;


import javax.inject.Inject;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.api.config.CompositeConfig;
import com.netflix.archaius.config.DefaultCompositeConfig;
import com.netflix.archaius.config.EnvironmentConfig;
import com.netflix.archaius.config.SystemConfig;
public class EVCacheConfig {

    private static EVCacheConfig INSTANCE;

    /**
     * This is an hack, should find a better way to do this 
     **/
    private static PropertyRepository propertyRepository;

	@Inject
    public EVCacheConfig(PropertyRepository repository) {
	    if(repository == null) {
	        try {
    	        final CompositeConfig applicationConfig = new DefaultCompositeConfig();
    	        applicationConfig.addConfig("SYSTEM", SystemConfig.INSTANCE);
    	        applicationConfig.addConfig("ENVIRONMENT",  EnvironmentConfig.INSTANCE);
    	        propertyRepository = new DefaultPropertyFactory(applicationConfig);
	        } catch (Exception e) {
	            e.printStackTrace();
	            propertyRepository = new DefaultPropertyFactory(new DefaultCompositeConfig());
            }
	    } else {
	        propertyRepository = repository;
	    }
        INSTANCE = this;
    }

    private EVCacheConfig() {
        this(null);
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
