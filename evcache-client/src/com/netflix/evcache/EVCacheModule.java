package com.netflix.evcache;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.netflix.evcache.connection.DIConnectionModule;
import com.netflix.evcache.event.hotkey.HotKeyListener;
import com.netflix.evcache.event.throttle.ThrottleListener;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.pool.EVCacheNodeList;
import com.netflix.evcache.pool.eureka.DIEVCacheNodeListProvider;

@Singleton
public class EVCacheModule extends AbstractModule {

    public EVCacheModule() {
    }

    @Override
    protected void configure() {
        // Make sure connection factory provider Module is initialized in your Module when you init EVCacheModule 
        install(new DIConnectionModule());
        bind(EVCacheNodeList.class).toProvider(DIEVCacheNodeListProvider.class);
        bind(EVCacheClientPoolManager.class).asEagerSingleton();
        
        bind(HotKeyListener.class).asEagerSingleton();
        bind(ThrottleListener.class).asEagerSingleton();
    }
        

    @PostConstruct
    public void init() {
        EVCacheClientPoolManager.getInstance().initAtStartup();
    }

    @PreDestroy
    public void shutdown() {
        EVCacheClientPoolManager.getInstance().shutdown();
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
 
    @Override
    public boolean equals(Object obj) {
        return (obj != null) && (obj.getClass() == getClass());
    }

}