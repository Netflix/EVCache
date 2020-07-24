package com.netflix.evcache.test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import java.util.Properties;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.netflix.evcache.*;
import com.netflix.evcache.operation.EVCacheItem;
import com.netflix.evcache.operation.EVCacheItemMetaData;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.evcache.util.KeyHasher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.netflix.evcache.operation.EVCacheOperationFuture;
import rx.schedulers.Schedulers;


import static org.testng.Assert.*;

public class EVCacheTestDI extends DIBase implements EVCacheGetOperationListener<String> {
    private static final Logger log = LoggerFactory.getLogger(EVCacheTestDI.class);
    private int loops = 1;
    private Map<String, String> propertiesToSet;
    private String appName = "EVCACHE_TEST";

    public static void main(String args[]) {
        try {
            EVCacheTestDI test = new EVCacheTestDI();
            test.testAll();
        } catch(Throwable t) {
            log.error(t.getMessage(), t);
        }
    }

    public EVCacheTestDI() {
        propertiesToSet = new HashMap<>();
        propertiesToSet.putIfAbsent(appName + ".us-east-1d.EVCacheClientPool.writeOnly", "false");
        propertiesToSet.putIfAbsent(appName + ".EVCacheClientPool.poolSize", "1");
        propertiesToSet.putIfAbsent(appName + ".ping.servers", "false");
        propertiesToSet.putIfAbsent(appName + ".cid.throw.exception", "true");
        propertiesToSet.putIfAbsent(appName + ".EVCacheClientPool.readTimeout", "500");
        propertiesToSet.putIfAbsent(appName + ".EVCacheClientPool.bulkReadTimeout", "500");
        propertiesToSet.putIfAbsent(appName + ".max.read.queue.length", "20");
        propertiesToSet.putIfAbsent("EVCacheClientPoolManager.log.apps", appName);
        propertiesToSet.putIfAbsent(appName + ".fallback.zone", "true");
        propertiesToSet.putIfAbsent(appName + ".enable.throttling", "false");
        propertiesToSet.putIfAbsent(appName + ".throttle.time", "0");
        propertiesToSet.putIfAbsent(appName + ".throttle.percent", "0");
        propertiesToSet.putIfAbsent(appName + ".log.operation", "1000");
        propertiesToSet.putIfAbsent(appName + ".EVCacheClientPool.validate.input.queue", "true");
    }

    protected Properties getProps() {
        Properties props = super.getProps();
        propertiesToSet.entrySet().forEach(entry -> props.setProperty(entry.getKey(), entry.getValue()));
        return props;
    }

    @Test
    public void testEVCache() {
        this.evCache = getNewBuilder().setAppName(appName).setCachePrefix("cid").enableRetry().build();
        assertNotNull(evCache);
    }

    @Test(dependsOnMethods = { "testEVCache" })
    public void testKeySizeCheck() throws Exception {
        final String key = "This is an invalid key";
        boolean exceptionThrown = false;
        for (int i = 0; i < loops; i++) {
            try {
                if (log.isDebugEnabled()) log.debug("Check key : " + key );
                evCache.<String>get(key);
            } catch(Exception e) {
                exceptionThrown = true;
                if (log.isDebugEnabled()) log.debug("Check key : " + key  + ": INVALID");
            }
            assertTrue(exceptionThrown);
        }

        final String longKey = "This_is_an_a_very_long_key.This_is_an_a_very_long_key.This_is_an_a_very_long_key.This_is_an_a_very_long_key.This_is_an_a_very_long_key.This_is_an_a_very_long_key.This_is_an_a_very_long_key.This_is_an_a_very_long_key.This_is_an_a_very_long_key.This_is_an_a_very_long_key.This_is_an_a_very_long_key.This_is_an_a_very_long_key.";
        exceptionThrown = false;
        for (int i = 0; i < loops; i++) {
            try {
                if (log.isDebugEnabled()) log.debug("Check key length : " + longKey );
                evCache.<String>get(longKey);
            } catch(Exception e) {
                exceptionThrown = true;
                if (log.isDebugEnabled()) log.debug("Check key length: " + longKey  + ": INVALID");
            }
            assertTrue(exceptionThrown);
        }

    }

    @Test(dependsOnMethods = { "testKeySizeCheck" })
    public void testTouch() throws Exception {
        for (int i = 0; i < loops; i++) {
            touch(i, evCache);
        }
    }

    @Test(dependsOnMethods = { "testTouch" })
    public void testDelete() throws Exception {
        for (int i = 0; i < loops; i++) {
            delete(i, evCache);
        }
    }

    @Test(dependsOnMethods = { "testDelete" })
    public void testAdd() throws Exception {
        for (int i = 0; i < loops; i++) {
            add(i, evCache);
        }
    }


    @Test(dependsOnMethods = { "testAdd" })
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

    @Test(dependsOnMethods = { "testInsert" })
    public void testGetObservable() throws Exception {
        for (int i = 0; i < loops; i++) {
            final String val = getObservable(i, evCache, Schedulers.computation());
            assertNotNull(val);
            assertTrue(val.equals("val_" + i));
        }
    }

    @Test(dependsOnMethods = { "testGetObservable" })
    public void testGetAndTouchObservable() throws Exception {
        for (int i = 0; i < loops; i++) {
            final String val = getAndTouchObservable(i, evCache, Schedulers.computation());
            assertNotNull(val);
            assertTrue(val.equals("val_" + i));
        }
    }

    @Test(dependsOnMethods = { "testGetAndTouchObservable" })
    public void waitForCallbacks() throws Exception {
        Thread.sleep(1000);
    }

    @Test(dependsOnMethods = { "waitForCallbacks" })
    public void testReplace() throws Exception {
        for (int i = 0; i < 10; i++) {
            replace(i, evCache);
        }
    }

    @Test(dependsOnMethods = { "testReplace" })
    public void testAppendOrAdd() throws Exception {
        for (int i = 0; i < loops; i++) {
            assertTrue(appendOrAdd(i, evCache));
        }
    }

    private void refreshEVCache() {
        setupEnv();
        testEVCache();
    }

    @Test(dependsOnMethods = {"testAppendOrAdd"})
    public void functionalTestsWithAppLevelAndASGLevelHashingScenarios() throws Exception {
        refreshEVCache();

        // no hashing
        assertFalse(manager.getEVCacheConfig().getPropertyRepository().get(appName + ".hash.key", Boolean.class).orElse(false).get());
        doFunctionalTests(false);

        // hashing at app level
        propertiesToSet.put(appName + ".hash.key", "true");
        refreshEVCache();
        assertTrue(manager.getEVCacheConfig().getPropertyRepository().get(appName + ".hash.key", Boolean.class).orElse(false).get());
        doFunctionalTests(true);
        propertiesToSet.remove(appName + ".hash.key");

        // hashing at app level due to auto hashing as a consequence of a large key
        propertiesToSet.put(appName + ".auto.hash.keys", "true");
        refreshEVCache();
        assertTrue(manager.getEVCacheConfig().getPropertyRepository().get(appName + ".auto.hash.keys", Boolean.class).orElse(false).get());
        assertFalse(manager.getEVCacheConfig().getPropertyRepository().get(appName + ".hash.key", Boolean.class).orElse(false).get());
        testWithLargeKey();
        // negative scenario
        propertiesToSet.remove(appName + ".auto.hash.keys");
        refreshEVCache();
        assertFalse(manager.getEVCacheConfig().getPropertyRepository().get(appName + ".auto.hash.keys", Boolean.class).orElse(false).get());
        assertFalse(manager.getEVCacheConfig().getPropertyRepository().get(appName + ".hash.key", Boolean.class).orElse(false).get());
        assertThrows(IllegalArgumentException.class, () -> {
            testWithLargeKey();
        });

        // hashing at app level by choice AND different hashing at each asg
        Map<String, KeyHasher.HashingAlgorithm> hashingAlgorithmsByServerGroup = new HashMap<>();
        propertiesToSet.put(appName + ".hash.key", "true");
        refreshEVCache();
        assertTrue(manager.getEVCacheConfig().getPropertyRepository().get(appName + ".hash.key", Boolean.class).orElse(false).get());

        // get server group names, to be used to configure the ASG level hashing properties
        Map<ServerGroup, List<EVCacheClient>> clientsByServerGroup = manager.getEVCacheClientPool(appName).getAllInstancesByServerGroup();
        int i = 0;
        for (ServerGroup serverGroup : clientsByServerGroup.keySet()) {
            KeyHasher.HashingAlgorithm hashingAlgorithm = KeyHasher.HashingAlgorithm.values()[i++ % KeyHasher.HashingAlgorithm.values().length];
            hashingAlgorithmsByServerGroup.put(serverGroup.getName(), hashingAlgorithm);
            propertiesToSet.put(serverGroup.getName() + ".hash.key", "true");
            propertiesToSet.put(serverGroup.getName() + ".hash.algo", hashingAlgorithm.name());
        }
        refreshEVCache();
        clientsByServerGroup = manager.getEVCacheClientPool(appName).getAllInstancesByServerGroup();
        // validate hashing properties of asgs
        for (ServerGroup serverGroup : clientsByServerGroup.keySet()) {
            assertEquals(clientsByServerGroup.get(serverGroup).get(0).getHashingAlgorithm(), hashingAlgorithmsByServerGroup.get(serverGroup.getName()));
        }
        doFunctionalTests(true);
        for (ServerGroup serverGroup : clientsByServerGroup.keySet()) {
            propertiesToSet.remove(serverGroup.getName());
        }
    }

    private void testWithLargeKey() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i= 0; i < 100; i++) {
            sb.append(Long.toString(System.currentTimeMillis()));
        }
        String key = sb.toString();
        String value = UUID.randomUUID().toString();

        // set
        EVCacheLatch latch = evCache.set(key, value, EVCacheLatch.Policy.ALL);
        latch.await(1000, TimeUnit.MILLISECONDS);

        // get
        assertEquals(evCache.get(key), value);
    }

    private void doFunctionalTests(boolean isHashingEnabled) throws Exception {
        String key1 = Long.toString(System.currentTimeMillis());
        String value1 = UUID.randomUUID().toString();

        // set
        EVCacheLatch latch = evCache.set(key1, value1, EVCacheLatch.Policy.ALL);
        latch.await(1000, TimeUnit.MILLISECONDS);

        // get
        assertEquals(evCache.get(key1), value1);

        // replace
        value1 = UUID.randomUUID().toString();
        latch = evCache.replace(key1, value1, EVCacheLatch.Policy.ALL);
        latch.await(1000, TimeUnit.MILLISECONDS);
        // get
        assertEquals(evCache.get(key1), value1);

        // add a key
        String key2 = Long.toString(System.currentTimeMillis());
        String value2 = UUID.randomUUID().toString();
        latch = evCache.add(key2, value2, null, 1000, EVCacheLatch.Policy.ALL);
        latch.await(1000, TimeUnit.MILLISECONDS);
        // get
        assertEquals(evCache.get(key2), value2);

        // appendoradd - append case
        String value3 = UUID.randomUUID().toString();
        if (isHashingEnabled) {
            assertThrows(EVCacheException.class, () -> {
                evCache.appendOrAdd(key2, value3, null, 1000, EVCacheLatch.Policy.ALL);
            });
        } else {
            latch = evCache.appendOrAdd(key2, value3, null, 1000, EVCacheLatch.Policy.ALL);
            latch.await(3000, TimeUnit.MILLISECONDS);
            assertEquals(evCache.get(key2), value2 + value3);
        }

        // appendoradd - add case
        String key3 = Long.toString(System.currentTimeMillis());
        String value4 = UUID.randomUUID().toString();
        if (isHashingEnabled) {
            assertThrows(EVCacheException.class, () -> {
                evCache.appendOrAdd(key3, value4, null, 1000, EVCacheLatch.Policy.ALL);
            });
        } else {
            latch = evCache.appendOrAdd(key3, value4, null, 1000, EVCacheLatch.Policy.ALL);
            latch.await(3000, TimeUnit.MILLISECONDS);
            // get
            assertEquals(evCache.get(key3), value4);
        }

        // append
        String value5 = UUID.randomUUID().toString();
        if (isHashingEnabled) {
            assertThrows(EVCacheException.class, () -> {
                evCache.append(key3, value5, 1000);
            });
        } else {
            Future<Boolean> futures[] = evCache.append(key3, value5, 1000);
            for (Future future : futures) {
                assertTrue((Boolean) future.get());
            }
            // get
            assertEquals(evCache.get(key3), value4 + value5);
        }

        String key4 = Long.toString(System.currentTimeMillis());
        assertEquals(evCache.incr(key4, 1, 10, 1000), 10);
        assertEquals(evCache.incr(key4, 10, 10, 1000), 20);

        // decr
        String key5 = Long.toString(System.currentTimeMillis());
        assertEquals(evCache.decr(key5, 1, 10, 1000), 10);
        assertEquals(evCache.decr(key5, 20, 10, 1000), 0);

        // delete
        latch = evCache.delete(key1, EVCacheLatch.Policy.ALL);
        latch.await(1000, TimeUnit.MILLISECONDS);
        latch = evCache.delete(key2, EVCacheLatch.Policy.ALL);
        latch.await(1000, TimeUnit.MILLISECONDS);
        latch = evCache.delete(key3, EVCacheLatch.Policy.ALL);
        latch.await(1000, TimeUnit.MILLISECONDS);
        latch = evCache.delete(key4, EVCacheLatch.Policy.ALL);
        latch.await(1000, TimeUnit.MILLISECONDS);
        latch = evCache.delete(key5, EVCacheLatch.Policy.ALL);
        latch.await(1000, TimeUnit.MILLISECONDS);

        assertNull(evCache.get(key1));
        assertNull(evCache.get(key2));
        assertNull(evCache.get(key3));
        assertNull(evCache.get(key4));
        assertNull(evCache.get(key5));
    }

    public void testAll() {
        try {
            setupEnv();
            testEVCache();
            testDelete();
            testAdd();
            Thread.sleep(500);
//            testInsertBinary();
            testInsert();

            int i = 0;
            while (i++ < loops*1000) {
                try {
                    testInsert();
                    testGet();
                    testGetAndTouch();
                    testBulk();
                    testBulkAndTouch();
                    testGetObservable();
                    testGetAndTouchObservable();
                    waitForCallbacks();
                    testAppendOrAdd();
                    testTouch();
                    testDelete();
                    testInsert();
                    if(i % 2 == 0) testDelete();
                    testAdd();

                    Thread.sleep(100);
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
            if (log.isDebugEnabled()) log.debug("All Done!!!. Will exit.");
            System.exit(0);
        } catch (Exception e) {
        	e.printStackTrace();
            log.error(e.getMessage(), e);
        }
    }

    public void onComplete(EVCacheOperationFuture<String> future) throws Exception {
        if (log.isDebugEnabled()) log.debug("getl : key : " + future.getKey() + ", val = " + future.get());
    }
}