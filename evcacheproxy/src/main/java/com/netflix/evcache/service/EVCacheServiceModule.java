package com.netflix.evcache.service;

import com.google.inject.AbstractModule;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.config.ConfigurationManager;
import com.netflix.discovery.guice.EurekaModule;
import com.netflix.evcache.EVCacheModule;
import com.netflix.evcache.connection.ConnectionModule;
import com.netflix.evcache.service.resources.EVCacheRESTService;
import com.netflix.governator.ShutdownHookModule;
import com.netflix.spectator.nflx.SpectatorModule;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import netflix.adminresources.resources.KaryonWebAdminModule;

public class EVCacheServiceModule extends AbstractModule {

    @Override
    protected void configure() {
        try {
            ConfigurationManager.loadAppOverrideProperties("evcacheproxy");
            final String env = ConfigurationManager.getConfigInstance().getString("eureka.environment", "test");
            if(env != null && env.length() > 0) {
                ConfigurationManager.loadAppOverrideProperties("evcacheproxy-"+env);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        
        install(new ShutdownHookModule());
        install(new EurekaModule()); 
        install(new SpectatorModule());
        install(new ConnectionModule());
        install(new EVCacheModule()); 
        install(new KaryonWebAdminModule());
        install(new JerseyServletModule() {
            protected void configureServlets() {
                serve("/*").with(GuiceContainer.class);
                binder().bind(GuiceContainer.class).asEagerSingleton();
                bind(EVCacheRESTService.class).asEagerSingleton();
                bind(HealthCheckHandlerImpl.class).asEagerSingleton();
            }
        });
    }
}

