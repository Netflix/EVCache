package com.netflix.evcache.test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheGetOperationListener;
import com.netflix.evcache.operation.EVCacheOperationFuture;

import rx.Observable;
import rx.functions.Action1;

public class EVCacheTestDI extends Base implements EVCacheGetOperationListener<String> {
    private static final Logger log = LoggerFactory.getLogger(EVCacheTestDI.class);
    private int loops = 10;

    public static void main(String args[]) {
        try {
            EVCacheTestDI test = new EVCacheTestDI();
            test.testAll();
        } catch(Throwable t) {
            log.error(t.getMessage(), t);
        }
    }

    public EVCacheTestDI() {
    }

    protected Properties getProps() {
        Properties props = super.getProps();
        props.setProperty("EVCACHE.us-east-1d.EVCacheClientPool.writeOnly", "false");
        props.setProperty("EVCACHE.EVCacheClientPool.poolSize", "1");
        props.setProperty("EVCACHE.ping.servers", "false");
        props.setProperty("EVCACHE.cid.throw.exception", "true");
        props.setProperty("EVCACHE.EVCacheClientPool.readTimeout", "500");
        props.setProperty("EVCACHE.EVCacheClientPool.bulkReadTimeout", "500");
        props.setProperty("EVCACHE.evcache.max.read.queue.length", "20");
        props.setProperty("EVCacheClientPoolManager.log.apps", "EVCACHE");
        props.setProperty("EVCACHE.fallback.zone", "true");
        props.setProperty("EVCACHE.enable.throttling", "false");
        props.setProperty("EVCACHE.throttle.time", "0");
        props.setProperty("EVCACHE.throttle.percent", "0");
        props.setProperty("EVCACHE.log.operation", "1000");
        props.setProperty("EVCACHE.EVCacheClientPool.validate.input.queue", "true");

        return props;
    }

    @Test
    public void testEVCache() {
        this.evCache = getNewBuilder().setAppName("EVCACHE").setCachePrefix("cid").enableRetry().build();
        assertNotNull(evCache);
    }

    @Test(dependsOnMethods = { "testEVCache" })
    public void testInsertBinary() throws Exception {
        for (int i = 0; i < loops; i++) {
            assertTrue(insertBytes(i, evCache));
        }
    }

    private boolean insertBytes(int i, EVCache gCache) throws Exception {
        byte[] val = ("val_" + i).getBytes();
        String key = "key_b_" + i;
        Future<Boolean>[] status = gCache.set(key, val, 24 * 60 * 60);
        for (Future<Boolean> s : status) {
            if (log.isDebugEnabled()) log.debug("SET BYTES : key : " + key + "; success = " + s.get() + "; Future = " + s.toString());
            if (s.get() == Boolean.FALSE) return false;
        }
        return true;
    }

    @Test(dependsOnMethods = { "testInsertBinary" })
    public void testGetBytes() throws Exception {
        for (int i = 0; i < loops; i++) {
            String key = "key_b_" + i;
            byte[] value = evCache.<byte[]> get(key);
            if(value != null) {
                if (log.isDebugEnabled()) log.debug("get : key : " + key + " val length = " + value.length);
            }
            assertNotNull(value);
        }
    }

    @Test(dependsOnMethods = { "testGetBytes" })
    public void testInsert() throws Exception {
        for (int i = 0; i < loops; i++) {
            assertTrue(insert(i, evCache));
        }
    }

    @Test(dependsOnMethods = { "testInsert" })
    public void testGet() throws Exception {
        for (int i = 0; i < loops; i++) {
            final String val = get(i, evCache);
            assertNotNull(val);
            assertTrue(val.equals("val_" + i));
        }
    }

    @Test(dependsOnMethods = { "testGet" })
    public void testGetAndTouch() throws Exception {
        for (int i = 0; i < loops; i++) {
            final String val = getAndTouch(i, evCache);
            assertNotNull(val);
            assertTrue(val.equals("val_" + i));
        }
    }

    @Test(dependsOnMethods = { "testGetAndTouch" })
    public void testBulk() throws Exception {
        final String[] keys = new String[loops];
        for (int i = 0; i < loops; i++) {
            keys[i] = "key_" + i;
        }
        Map<String, String> vals = getBulk(keys, evCache);
        assertNotNull(vals);
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            String val = vals.get(key);
            if (val == null) {
                if (log.isDebugEnabled()) log.debug("key " + key + " returned null");
            } else {
                assertTrue(val.equals("val_" + i));
            }
        }
    }

    @Test(dependsOnMethods = { "testBulk" })
    public void testBulkAndTouch() throws Exception {
        final String[] keys = new String[loops];
        for (int i = 0; i < loops; i++) {
            keys[i] = "key_" + i;
        }
        Map<String, String> vals = getBulkAndTouch(keys, evCache, 24 * 60 * 60);
        assertNotNull(vals);
        for (int i = 0; i < vals.size(); i++) {
            String key = "key_" + i;
            String val = vals.get(key);
            if (val == null) {
                if (log.isDebugEnabled()) log.debug("key " + key + " returned null");
            } else {
                assertTrue(val.equals("val_" + i));
            }
        }
    }

    @Test(dependsOnMethods = { "testBulkAndTouch" })
    public void testGetListener() throws Exception {
        for (int i = 0; i < loops; i++) {
            String key = "key_" + i;
            evCache.<String> get(key, this);
        }
    }

    @Test(dependsOnMethods = { "testGetListener" })
    public void testGetObservable() throws Exception {
        for (int i = 0; i < loops; i++) {
            String key = "key_" + i;
            if (log.isDebugEnabled()) log.debug("testGetObservable : " + "key = " + key);
            Observable<String> obs = evCache.<String> get(key, new HashMap<String, Object>());
            obs.doOnNext(new OnNextHandler(key)).doOnError(new OnErrorHandler(key)).subscribe();
        }
    }

    @Test(dependsOnMethods = { "testGetObservable" })
    public void waitForCallbacks() throws Exception {
        Thread.sleep(500);
    }

    @Test(dependsOnMethods = { "waitForCallbacks" })
    public void testReplace() throws Exception {
        for (int i = 0; i < 10; i++) {
            replace(i, evCache);
        }
    }

    @Test(dependsOnMethods = { "testReplace" })
    public void testDelete() throws Exception {
        for (int i = 0; i < loops; i++) {
            delete(i, evCache);
        }
    }

    public void testAll() {
        try {
            setupEnv();
            testEVCache();
            testInsertBinary();
            testInsert();

            int i = 0;
            while (i++ < loops) {
                try {
                    testInsert();
                    testGet();
                    testGetAndTouch();
                    testBulk();
                    testBulkAndTouch();
                    testGetObservable();
                    testGetListener();
                    waitForCallbacks();
                    testDelete();
                    Thread.sleep(1000);
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
            if (log.isDebugEnabled()) log.debug("All Done!!!. Will exit.");
            System.exit(0);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void onComplete(EVCacheOperationFuture<String> future) throws Exception {
        if (log.isDebugEnabled()) log.debug("getl : key : " + future.getKey() + ", val = " + future.get());
    }

    static class OnErrorHandler implements Action1<Throwable> {
        private final String key;

        public OnErrorHandler(String key) {
            this.key = key;
        }

        @Override
        public void call(Throwable t1) {
            if (log.isDebugEnabled()) log.debug("Could not get value for key: " + key + "; Exception is ", t1);
        }
    }

    static class OnNextHandler implements Action1<String> {
        private final String key;

        public OnNextHandler(String key) {
            this.key = key;
        }

        @Override
        public void call(String val) {
            if (log.isDebugEnabled()) log.debug("Observable : key " + key + "; val = " + val);
        }

    }

}