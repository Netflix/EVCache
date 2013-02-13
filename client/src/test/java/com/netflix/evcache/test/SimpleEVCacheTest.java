package com.netflix.evcache.test;

import org.apache.log4j.BasicConfigurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.EVCache;
import com.netflix.evcache.pool.EVCacheClientPoolManager;

/**
 * Tests SimpleEVCacheClient.
 * @author smadappa
 *
 */
public class SimpleEVCacheTest extends AbstractEVCacheTest {
    private static Logger log = LoggerFactory.getLogger(SimpleEVCacheTest.class);

    @BeforeClass
    public static void initLibraries() {
        try {
            System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
            System.setProperty("log4j.rootLogger", "ERROR");
            BasicConfigurator.configure();
            log.info("Logger intialized");

            System.setProperty("evcache.pool.provider", "com.netflix.evcache.pool.standalone.SimpleEVCacheClientPoolImpl");
            System.setProperty("EVCACHE.EVCacheClientPool.hosts", "ec2-50-16-48-64.compute-1.amazonaws.com:11211");
            log.info("initializing EVCache");
            EVCacheClientPoolManager.getInstance().initEVCache("EVCACHE");
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void runTest() {
        EVCache gCache = (new EVCache.Builder()).setAppName("EVCACHE").setCacheName("test").enableZoneFallback().build();
        int executeCount = 0;
        while (executeCount++ < 3) {
            try {
                for (int i = 0; i < 20; i++) {
                    insert(i, gCache);
                    get(i, gCache);
                    getAndTouch(i, gCache);
                    getBulk(0, i, gCache);
                }
            } catch (Exception e) {
                log.error("Exception", e);
            }
        }
    }
}
