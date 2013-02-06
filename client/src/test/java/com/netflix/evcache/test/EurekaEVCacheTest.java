package com.netflix.evcache.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import junit.framework.TestCase;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import com.google.inject.Injector;
import com.netflix.appinfo.CloudInstanceConfig;
import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.config.ConfigurationManager;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.evcache.EVCache;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.pool.standalone.SimpleEVCacheClientPoolImpl;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;

@SuppressWarnings("unused")
public class EurekaEVCacheTest extends AbstractEVCacheTest {
	private static Logger log = LoggerFactory.getLogger(EurekaEVCacheTest.class);
	
    @BeforeClass
    public static void initLibraries() {
		try {
			final Properties logProps = new Properties(); 
			logProps.setProperty("log4j.rootLogger", "ERROR, CONSOLE");
			logProps.setProperty("log4j.appender.CONSOLE","org.apache.log4j.ConsoleAppender");
			logProps.setProperty("log4j.appender.CONSOLE.layout","org.apache.log4j.PatternLayout");
			logProps.setProperty("log4j.appender.CONSOLE.layout.ConversionPattern", "%d [%t] %p %c:%L  - %m%n");
			logProps.setProperty("log4j.logger.net.spy.memcached","WARN,CONSOLE");
			logProps.setProperty("log4j.logger.com.netflix.evcache.client.EVCacheConnectionObserver", "INFO,CONSOLE");
			logProps.setProperty("log4j.logger.com.netflix.evcache", "DEBUG,CONSOLE");
			PropertyConfigurator.configure(logProps);
			log.info("Logger intialized");

			BaseConfiguration  props = new BaseConfiguration();
			props.setProperty("evcache.appsToInit", "EVCACHE");
			props.setProperty("EVCACHE.EVCacheClientPool.zoneAffinity","true");
            props.setProperty("EVCACHE.EVCacheClientPool.poolSize", "1");
            props.setProperty("EVCACHE.EVCacheClientPool.readTimeout", "1000");
            props.setProperty("EVCACHE.us-east-1d.EVCacheClientPool.writeOnly", "true");
            props.setProperty("EVCACHE.us-east-1c.EVCacheClientPool.writeOnly", "true");
            props.setProperty("EVCACHE.ping.servers", "false");
//            props.setProperty("EVCACHE.enable.throttling", "true");
//            props.setProperty("EVCACHE.throttle.time", "0");
//            props.setProperty("EVCACHE.throttle.percent", "5");
//
            
			props.setProperty("eureka.datacenter", "cloud");
			props.setProperty("eureka.awsAccessId","AKIAJCK2WUHJ2653GNBQ");
			props.setProperty("eureka.awsSecretKey","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");
			props.setProperty("netflix.appinfo.validateInstanceId","false");
			
			props.setProperty("netflix.discovery.us-east-1.availabilityZones","us-east-1c,us-east-1d,us-east-1e");
			props.setProperty("netflix.discovery.serviceUrl.us-east-1c","http://ec2-204-236-228-165.compute-1.amazonaws.com:7001/discovery/v2/,http://ec2-75-101-165-111.compute-1.amazonaws.com:7001/discovery/v2/");
			props.setProperty("netflix.discovery.serviceUrl.us-east-1d","http://ec2-204-236-228-170.compute-1.amazonaws.com:7001/discovery/v2/");
			props.setProperty("netflix.discovery.serviceUrl.us-east-1e","http://ec2-50-19-255-91.compute-1.amazonaws.com:7001/discovery/v2/");
			ConfigurationManager.install(props);

            Injector injector = LifecycleInjector.builder().createInjector();
            injector.getInstance(LifecycleManager.class).start();

            final EurekaInstanceConfig config = new CloudInstanceConfig("netflix.appinfo.");
            final EurekaClientConfig clientConfig = new DefaultEurekaClientConfig("netflix.discovery.");
            log.info("initializing Eureka");
            DiscoveryManager.getInstance().initComponent(config, clientConfig);            

            log.info("initializing EVCache");
            EVCacheClientPoolManager.getInstance().initEVCache("EVCACHE");
            log.info("All libraries iniialized. Will sleep for 5 seconds");
            Thread.sleep(5000);
		} catch (Exception e) {
			log.error("Exception during init", e);
		}
    }
    
    @Test
    public void runTest() {
		EVCache gCache = (new EVCache.Builder()).setAppName("EVCACHE").setCacheName("test").enableZoneFallback().build();
	    int executeCount = 0;
        boolean bool = true;
        while(executeCount++ < 3) {
			try {
			    int count = 1;
				for(int i = 0; i < 20; i++) {
				    insert(i, gCache);
				    get(i, gCache);
				    getBulk(0, i, gCache);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
	}
}