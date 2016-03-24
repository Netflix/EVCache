package com.netflix.evcservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.servlet.ServletModule;
import com.netflix.evcache.EVCacheClientLibrary;
import com.netflix.governator.annotations.AutoBindSingleton;
import com.netflix.logging.ILog;
import com.netflix.logging.LogManager;
import com.netflix.server.base.BaseStatusPage;
import com.netflix.server.base.NFFilter;
import com.netflix.server.base.lifecycle.BaseServerLifecycleListener;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.ServletContainer;

import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletContextEvent;

@AutoBindSingleton
public class StartServer extends BaseServerLifecycleListener
{
    private static final ILog logger = LogManager.getLogger(StartServer.class);
    private static final String APP_NAME = "evcservice";
    private static final String CONFIG_NAME = "evcservice";

    /**
     * Creates a new StartServer object.
     */
    public StartServer() {
        super(CONFIG_NAME, APP_NAME, null);
    }

    @Override
    protected Class<? extends com.netflix.karyon.spi.HealthCheckHandler> getHealthCheckHandler() {
        return HealthCheckHandlerImpl.class;


    }

    @Override
    protected void initialize(ServletContextEvent sce) throws Exception {
        Injector injector = getInjector();
        injector.getInstance(EVCacheClientLibrary.class);
    }

    @Override
    protected ServletModule getServletModule() {
        return new JerseyServletModule() {
            @Override
            protected void configureServlets() {
                logger.info("########## CONFIGURING SERVLETS ##########");

                // initialize NFFilter
                Map<String, String> initParams = new HashMap<>();
                initParams.put(ServletContainer.JSP_TEMPLATES_BASE_PATH, "/WEB-INF/jsp");
                initParams.put(ServletContainer.FEATURE_FILTER_FORWARD_ON_404, "true");
                initParams.put("requestId.accept", "true");
                initParams.put("requestId.require", "true");
                initParams.put(PackagesResourceConfig.PROPERTY_PACKAGES, "com.netflix.evcservice.resources");
                filter("/*").through(NFFilter.class, initParams);
                filter("/healthcheck", "/Healthcheck").through(NFFilter.class, initParams);
                serve("/Status", "/status").with(StatusPage.class);
                serve("/REST/*").with(GuiceContainer.class, createStandardServeParams());

                binder().bind(GuiceContainer.class).asEagerSingleton();
                binder().bind(ObjectMapper.class).asEagerSingleton();

            }
        };
    }
}

