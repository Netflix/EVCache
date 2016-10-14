package com.netflix.evcache.service;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContextEvent;

import com.netflix.evcache.service.resources.EVCacheRESTService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Injector;
import com.google.inject.servlet.ServletModule;
import com.netflix.evcache.EVCacheClientLibrary;
import com.netflix.evcservice.service.StatusPage;
import com.netflix.server.base.NFFilter;
import com.netflix.server.base.lifecycle.BaseServerLifecycleListener;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;


public class StartServer extends BaseServerLifecycleListener
{
    private static final Logger logger = LoggerFactory.getLogger(StartServer.class);
    private static final String APP_NAME = "evcacheproxy";
    private static final String CONFIG_NAME = "evcacheproxy";

    /**
     * Creates a new StartServer object.
     */
    public StartServer() {
        super(CONFIG_NAME, APP_NAME, null);
    }

    @Override
    protected void initialize(ServletContextEvent sce) throws Exception {
        Injector injector = getInjector();
        injector.getInstance(EVCacheClientLibrary.class);
        EVCacheRESTService evCacheRESTService = injector.getInstance(EVCacheRESTService.class);
        evCacheRESTService.initializeCaches();
    }

    @Override
    protected ServletModule getServletModule() {
        return new JerseyServletModule() {
            @Override
            protected void configureServlets() {
                logger.info("########## CONFIGURING SERVLETS ##########");

                // initialize NFFilter
                Map<String, String> initParams = new HashMap<String,String>();
//                initParams.put(ServletContainer.JSP_TEMPLATES_BASE_PATH, "/WEB-INF/jsp");
//                initParams.put(ServletContainer.FEATURE_FILTER_FORWARD_ON_404, "true");
//                initParams.put("requestId.accept", "true");
//                initParams.put("requestId.require", "true");
                initParams.put(PackagesResourceConfig.PROPERTY_PACKAGES, "com.netflix.evcservice.resources");
                filter("/*").through(NFFilter.class, initParams);
                filter("/healthcheck", "/status").through(NFFilter.class, initParams);
                serve("/*").with(GuiceContainer.class, initParams);
                binder().bind(GuiceContainer.class).asEagerSingleton();
                
                install(new EVCacheServiceModule());
            }
        };
    }
    
}
