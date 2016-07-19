package com.netflix.evcache.test;

import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheException;
import com.netflix.evcache.operation.EVCacheOperationFuture;

import rx.functions.Action1;

public class MockEVCacheTest {
    protected EVCache evCache = null;
    private static final Logger log = LoggerFactory.getLogger(MockEVCacheTest.class);
    private int loops = 10;

    public MockEVCacheTest() {
    }

    @Test
    public void testEVCache() {
        this.evCache = new DummyEVCacheImpl().getDummyCache();
        assertNotNull(evCache);
    }

    public boolean insert(int i, EVCache gCache) throws Exception {
        String val = "val_"+i;
        String key = "key_" + i;
        Future<Boolean>[] status = gCache.set(key, val, 24 * 60 * 60);
        for(Future<Boolean> s : status) {
            if(log.isDebugEnabled()) log.debug("SET : key : " + key + "; success = " + s.get() + "; Future = " + s.toString());
            if(s.get() == Boolean.FALSE) return false;
        }
        return true;
    }
    

    public boolean delete(int i, EVCache gCache) throws Exception {
        String key = "key_" + i;
        Future<Boolean>[] status = gCache.delete(key);
        for(Future<Boolean> s : status) {
            if(log.isDebugEnabled()) log.debug("DELETE : key : " + key + "; success = " + s.get() + "; Future = " + s.toString());
            if(s.get() == Boolean.FALSE) return false;
        }
        return true;
    }

    protected boolean touch(int i, EVCache gCache) throws Exception {
        return touch(i, gCache, 24 * 60 * 60);
    }

    protected boolean touch(int i, EVCache gCache, int ttl) throws Exception {
        String key = "key_" + i;
        Future<Boolean>[] status = gCache.touch(key, ttl);
        for (Future<Boolean> s : status) {
            if (log.isDebugEnabled()) log.debug("TOUCH : key : " + key + "; success = " + s.get() + "; Future = " + s.toString());
            if (s.get() == Boolean.FALSE) return false;
        }
        return true;
    }

    public String get(int i, EVCache gCache) throws Exception {
        String key = "key_" + i;
        String value = gCache.<String>get(key);
        if(log.isDebugEnabled()) log.debug("get : key : " + key + " val = " + value);
        return value;
    }

    public String getAndTouch(int i, EVCache gCache) throws Exception {
        String key = "key_" + i;
        String value = gCache.<String>getAndTouch(key, 24 * 60 * 60);
        if(log.isDebugEnabled()) log.debug("getAndTouch : key : " + key + " val = " + value);
        return value;
    }

    public Map<String, String> getBulk(String keys[], EVCache gCache) throws Exception {
        final Map<String, String> value = gCache.<String>getBulk(keys);
        if(log.isDebugEnabled()) log.debug("getBulk : keys : " + Arrays.toString(keys) + "; values = " + value);
        return value;
    }

    public Map<String, String> getBulkAndTouch(String keys[], EVCache gCache, int ttl) throws Exception {
        final Map<String, String> value = gCache.<String>getBulkAndTouch(Arrays.asList(keys), null, ttl);
        if(log.isDebugEnabled()) log.debug("getBulk : keys : " + Arrays.toString(keys) + "; values = " + value);
        return value;
    }

    @Test(dependsOnMethods = { "testEVCache" })
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
        }
    }

    @Test(dependsOnMethods = { "testGet" })
    public void testGetAndTouch() throws Exception {
        for (int i = 0; i < loops; i++) {
            final String val = getAndTouch(i, evCache);
            assertNotNull(val);
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
            if (log.isDebugEnabled()) log.debug("key " + key + " returned val " + val);
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
    public void testDelete() throws Exception {
        for (int i = 0; i < loops; i++) {
            delete(i, evCache);
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
    
    /**
     * Dummy Cache used for debuging purpose (simple way to disable cache)
     */
    private static class DummyEVCacheImpl  {
        private final EVCache cache;

        @SuppressWarnings("unchecked")
        public DummyEVCacheImpl() {
            cache = mock(EVCache.class);
            try {
                when(cache.set(anyString(), anyObject(), anyInt())).thenReturn(new Future[0]);
                when(cache.get(anyString())).thenReturn("");
                when(cache.getAndTouch(anyString(), anyInt())).thenReturn("");
                when(cache.getBulk(anyCollection())).thenReturn(Collections.emptyMap());
                when(cache.delete(anyString())).thenReturn(new Future[0]);
            } catch (EVCacheException e) {
                log.error("Unable to create mock EVCache", e);
            }
        }

        public EVCache getDummyCache() {
            return cache;
        }
    }

}   