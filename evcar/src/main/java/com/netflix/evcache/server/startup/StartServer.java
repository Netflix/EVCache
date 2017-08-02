package com.netflix.evcache.server.startup;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContextEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Injector;
import com.google.inject.servlet.ServletModule;
import com.netflix.evcache.server.metrics.EVCacheAtlasPlugin;
import com.netflix.evcache.server.resources.EVCacheServerRESTAPI;
import com.netflix.evcache.util.EVCacheS3Util;
import com.netflix.governator.annotations.AutoBindSingleton;
import com.netflix.runtime.health.guice.HealthModule;
import com.netflix.runtime.health.servlet.HealthStatusServlet;
import com.netflix.server.base.NFFilter;
import com.netflix.server.base.lifecycle.BaseServerLifecycleListener;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * Created by senugula on 10/12/16.
 */
@AutoBindSingleton
public class StartServer extends BaseServerLifecycleListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(StartServer.class);

    private static String configName;
    static {
        if ("moneta".equals(System.getenv("EVCACHE_SERVER_TYPE"))) {
            configName = "evcache-moneta";
        } else {
            configName = "evcache";
        }
    }

    //	public static DynamicBooleanProperty rendStatsGate = new DynamicBooleanProperty("evcache.metrics.rend.send.stats", false);

    public StartServer() {
        super(configName);
    }

    @Override
    protected void initialize(ServletContextEvent sce) throws Exception {
        Injector injector = getInjector();
        try {
            injector.getInstance(EVCacheServerRESTAPI.class);
            injector.getInstance(EVCacheAtlasPlugin.class).init();
            injector.getInstance(EVCacheS3Util.class);
        } catch (Throwable th){
            LOGGER.error(th.getMessage());
        }
    }

    @Override
    protected ServletModule getServletModule() {
        return new JerseyServletModule() {
            @Override
            protected void configureServlets() {
                install(new HealthModule(){
                    @Override
                    protected void configureHealth() {
                        bindAdditionalHealthIndicator().to(EVCacheHealthCheckIndicator.class);
                    }
                });
                serve("/healthcheck").with(HealthStatusServlet.class);
                Map<String, String> initParams = new HashMap<String, String>();
                initParams.put("requestId.require", "false");
                initParams.put(PackagesResourceConfig.PROPERTY_PACKAGES, "com.netflix.evcache.server.resources");
                initParams.put(ResourceConfig.FEATURE_DISABLE_WADL, "true");
                initParams.put(ServletContainer.JSP_TEMPLATES_BASE_PATH, "/WEB-INF/jsp");
                filter("/*").through(NFFilter.class, initParams);
                serve("/Status", "/status").with(StatusPage.class);
                serve("/*").with(GuiceContainer.class, initParams);
                binder().bind(GuiceContainer.class).asEagerSingleton();
            }
        };
    }
}
