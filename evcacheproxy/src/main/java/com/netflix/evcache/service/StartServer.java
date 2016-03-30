package com.netflix.evcache.service;

import javax.servlet.ServletContextEvent;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.InstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Injector;
import com.netflix.evcache.service.resources.EVCacheRESTService;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.governator.guice.servlet.GovernatorServletContextListener;


public class StartServer extends GovernatorServletContextListener
{
    private Logger logger = LoggerFactory.getLogger(EVCacheRESTService.class);

    
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        super.contextInitialized(servletContextEvent);
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        super.contextDestroyed(servletContextEvent);
    }

    @Override
    protected Injector createInjector() {
        if(logger.isDebugEnabled()) logger.debug("Creating Injector");

        final LifecycleInjector injector = InjectorBuilder.fromModules(new EVCacheServiceModule()).createInjector();
        injector.getInstance(ApplicationInfoManager.class).setInstanceStatus(InstanceInfo.InstanceStatus.UP);
        return injector;
        
    }
}
