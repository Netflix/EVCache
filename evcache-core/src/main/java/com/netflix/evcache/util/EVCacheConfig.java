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
	    
        String userLocation = null;
        if(userLocation == null) userLocation= System.getenv("EC2_INSTANCE_ID");
        if(userLocation == null) userLocation= System.getenv("NETFLIX_INSTANCE_ID");
        if(userLocation == null) userLocation= System.getProperty("EC2_INSTANCE_ID");
        if(userLocation == null) userLocation= System.getProperty("NETFLIX_INSTANCE_ID");
        if(userLocation == null && propertyRepository != null) userLocation = propertyRepository.get("EC2_INSTANCE_ID", String.class).orElse(null).get();
        if(userLocation == null && propertyRepository != null) userLocation = propertyRepository.get("NETFLIX_INSTANCE_ID", String.class).orElse(null).get();

        if(userLocation == null) { //Assuming this is not in cloud so bump up the timeouts
            System.setProperty("default.read.timeout", "750");
            System.setProperty("default.bulk.timeout", "750");
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
