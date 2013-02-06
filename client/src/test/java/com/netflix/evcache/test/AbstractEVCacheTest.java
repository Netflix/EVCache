package com.netflix.evcache.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.netflix.evcache.EVCache;

public class AbstractEVCacheTest {

	protected  void insert(int i, EVCache gCache) throws Exception {
		String val = "val_"+i;
		String key = "key_" + i;
		Future<Boolean>[] status = gCache.set(key, val, null, 900);
		for(Future<Boolean> s : status) {
			assertTrue("key : " + key , s.get());
		}
	}

	protected  void get(int i, EVCache gCache) throws Exception {
		String key = "key_" + i;
		String value = gCache.<String>get(key);
		assertNotNull("get : key : " + key , value);
	}

	protected  void getAndTouch(int i, EVCache gCache) throws Exception {
		String key = "key_" + i;
		String value = gCache.<String>getAndTouch(key, 900);
		assertNotNull("getAndTouch : key : " + key , value);
	}

	protected  void getBulk(int start, int end, EVCache gCache) throws Exception {
		final List<String> keyList = new ArrayList<String>();
		for(int i = start; i < end; i++) {
			keyList.add("key_" + i);
		}
		Map<String, String> value = gCache.<String>getBulk(keyList);
		assertNotNull("getBulk : keys : " + keyList , value);            
	}
}
