package com.netflix.evcache.util;


import java.lang.reflect.Type;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.inject.Inject;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyListener;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.api.config.CompositeConfig;
import com.netflix.archaius.config.DefaultCompositeConfig;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.archaius.config.EnvironmentConfig;
import com.netflix.archaius.config.SystemConfig;
import com.netflix.evcache.config.EVCachePersistedProperties;
public class EVCacheConfig {

    private static EVCacheConfig INSTANCE;

    /**
     * This is an hack, should find a better way to do this 
     **/
    private static PropertyRepository propertyRepository;

	@Inject
    public EVCacheConfig(PropertyRepository repository) {
	    PropertyRepository _propertyRepository = null;
	    if(repository == null) {
	        try {
    	        final CompositeConfig applicationConfig = new DefaultCompositeConfig(true);
    	        CompositeConfig remoteLayer = new DefaultCompositeConfig(true);
    	        applicationConfig.addConfig("RUNTIME", new DefaultSettableConfig());
                applicationConfig.addConfig("REMOTE", remoteLayer);
    	        applicationConfig.addConfig("SYSTEM", SystemConfig.INSTANCE);
    	        applicationConfig.addConfig("ENVIRONMENT",  EnvironmentConfig.INSTANCE);
    	        final EVCachePersistedProperties remote = new EVCachePersistedProperties();
                remoteLayer.addConfig("remote-1", remote.getPollingDynamicConfig());
    	        _propertyRepository = new DefaultPropertyFactory(applicationConfig);
    	        
	        } catch (Exception e) {
	            e.printStackTrace();
	            _propertyRepository = new DefaultPropertyFactory(new DefaultCompositeConfig());
            }
	    } else {
	        _propertyRepository = repository;
	    }
	    propertyRepository =  new EVCachePropertyRepository(_propertyRepository);
	    //propertyRepository = _propertyRepository;

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

    class EVCachePropertyRepository implements PropertyRepository {
        
        private final PropertyRepository delegate;

        EVCachePropertyRepository(PropertyRepository delegate) {
            this.delegate = delegate;
        }

        @Override
        public <T> Property<T> get(String key, Class<T> type) {
            return new EVCacheProperty<T>(delegate.get(key, type));
        }

        @Override
        public <T> Property<T> get(String key, Type type) {
            return new EVCacheProperty<T>(delegate.get(key, type));
        }
    }

    class EVCacheProperty<T> implements Property<T> {

        private final Property<T> property;
        EVCacheProperty(Property<T> prop) {
            property = prop;
        }

        @Override
        public T get() {
            return property.get();
        }

        @Override
        public String getKey() {
            return property.getKey();
        }

        @Override
        public void addListener(PropertyListener<T> listener) {
            // TODO Auto-generated method stub
            property.addListener(listener);
        }

        @Override
        public void removeListener(PropertyListener<T> listener) {
            // TODO Auto-generated method stub
            property.removeListener(listener);
        }

        @Override
        public Subscription onChange(Consumer<T> consumer) {
            // TODO Auto-generated method stub
            return property.onChange(consumer);
        }

        @Override
        public Subscription subscribe(Consumer<T> consumer) {
            // TODO Auto-generated method stub
            return property.subscribe(consumer);
        }

        @Override
        public Property<T> orElse(T defaultValue) {
            // TODO Auto-generated method stub
            return new EVCacheProperty<T>(property.orElse(defaultValue));
        }

        @Override
        public Property<T> orElseGet(String key) {
            // TODO Auto-generated method stub
            return new EVCacheProperty<T>(property.orElseGet(key));
        }

        @Override
        public <S> Property<S> map(Function<T, S> mapper) {
            // TODO Auto-generated method stub
            return property.map(mapper);
        }

        @Override
        public String toString() {
            return "EVCacheProperty [Key=" + getKey() + ",value="+get() + "]";
        }
        
    }
}
