package com.netflix.evcache.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.EVCache;

/**
 * Tests get, set, getAndTouch and getBulk methods.
 *
 * @author smadappa
 *
 */
public class AbstractEVCacheTest {
    private static Logger log = LoggerFactory.getLogger(AbstractEVCacheTest.class);

    protected  void insert(int i, EVCache gCache) throws Exception {
        String val = "val_" + i;
        String key = "key_" + i;
        Future<Boolean>[] status = gCache.set(key, val, null, 900);
        for (Future<Boolean> s : status) {
            log.info("SET : key :" + key + "; status : " + s.get());
            assertTrue("key : " + key , s.get());
        }
    }

    protected  void get(int i, EVCache gCache) throws Exception {
        String key = "key_" + i;
        String value = gCache.<String>get(key);
        log.info("GET : key :" + key + "; Value : " + value);
        assertNotNull("get : key : " + key , value);
    }

    protected  void getAndTouch(int i, EVCache gCache) throws Exception {
        String key = "key_" + i;
        String value = gCache.<String>getAndTouch(key, 900);
        log.info("GET : key :" + key + "; Value : " + value);
        assertNotNull("getAndTouch : key : " + key , value);
    }

    protected  void getBulk(int start, int end, EVCache gCache) throws Exception {
        final List<String> keyList = new ArrayList<String>();
        for (int i = start; i < end; i++) {
            keyList.add("key_" + i);
        }
        Map<String, String> value = gCache.<String>getBulk(keyList);
        log.info("GET : key :" + keyList + "; Value : " + value);
        assertNotNull("getBulk : keys : " + keyList , value);
    }
}
