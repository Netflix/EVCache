package com.netflix.evcache.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheException;
import com.netflix.evcache.EVCacheGetOperationListener;
import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.operation.EVCacheOperationFuture;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.evcache.test.transcoder.Movie;
import com.netflix.evcache.test.transcoder.MovieTranscoder;
import com.netflix.evcache.util.KeyHasher;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import rx.schedulers.Schedulers;

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
        propertiesToSet.putIfAbsent("evcache.use.binary.protocol", "false");
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
                assertTrue(e.getMessage().contains(longKey), "Error message should include the invalid key.");
                assertTrue(e.getMessage().contains(Integer.toString(longKey.length())), "Error message should include the key length of the invalid key.");
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
        // Close the previous DI container before building a new one. setupEnv() builds a fresh LifecycleInjector on
        // every call; without closing the old ones they accumulate across refreshes and the suite slows to a crawl,
        // to the point it cannot finish.
        if (lifecycleManager != null) {
            try {
                lifecycleManager.close();
            } catch (Exception e) {
                log.warn("Failed to close previous lifecycle manager during refresh", e);
            }
        }
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
        testBulkHashed();
        propertiesToSet.remove(appName + ".hash.key");

        // hashing at app level due to auto hashing as a consequence of a large key
        propertiesToSet.put(appName + ".auto.hash.keys", "true");
        refreshEVCache();
        assertTrue(manager.getEVCacheConfig().getPropertyRepository().get(appName + ".auto.hash.keys", Boolean.class).orElse(false).get());
        assertFalse(manager.getEVCacheConfig().getPropertyRepository().get(appName + ".hash.key", Boolean.class).orElse(false).get());
        testWithLargeKey();
        testWithMixedKeys();
        testWithMixedKeysAndCustomTranscoder();
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
        KeyHasher.HashingAlgorithm hashingAlgorithm = KeyHasher.HashingAlgorithm.values()[0];
        for (ServerGroup serverGroup : clientsByServerGroup.keySet()) {
            // use below logic to have different hashing per asg once the code supports. Currently the code caches the value that it uses for all the asgs
            // KeyHasher.HashingAlgorithm hashingAlgorithm = KeyHasher.HashingAlgorithm.values()[i++ % KeyHasher.HashingAlgorithm.values().length];
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
            propertiesToSet.remove(serverGroup.getName() + ".hash.key");
            propertiesToSet.remove(serverGroup.getName() + ".hash.algo");
        }

        propertiesToSet.remove(appName + ".hash.key", "true");

        refreshEVCache();
    }

    @Test(dependsOnMethods = { "functionalTestsWithAppLevelAndASGLevelHashingScenarios" })
    public void testChunkingScenarios() throws Exception {
        // chunking only (no hashing): large values are split into chunks and reassembled on read
        propertiesToSet.put(appName + ".chunk.data", "true");
        refreshEVCache();
        assertTrue(manager.getEVCacheConfig().getPropertyRepository().get(appName + ".chunk.data", Boolean.class).orElse(false).get());
        doChunkingTests();

        // chunking + auto-hashing together: with auto.hash.keys, short keys stay plain while keys whose canonical form
        // exceeds max.key.length are hashed. A single getBulk over both exercises the mixed-key, chunk-aware path: plain
        // keys decode in one step, hashed keys are EVCacheValue-wrapped and decode in two steps, all after reassembly.
        propertiesToSet.put(appName + ".auto.hash.keys", "true");
        refreshEVCache();
        assertTrue(manager.getEVCacheConfig().getPropertyRepository().get(appName + ".auto.hash.keys", Boolean.class).orElse(false).get());
        doMixedKeyChunkingTests();
        propertiesToSet.remove(appName + ".auto.hash.keys");

        propertiesToSet.remove(appName + ".chunk.data");
        refreshEVCache();
    }

    private void doChunkingTests() throws Exception {
        final EVCacheClient client = manager.getEVCacheClientPool(appName).getEVCacheClientForRead();

        // single large value -> chunked set/get
        final String largeKey = "chunk_large_" + System.nanoTime();
        final String largeValue = buildLargeValue(4000);
        EVCacheLatch latch = evCache.set(largeKey, largeValue, EVCacheLatch.Policy.ALL);
        latch.await(10000, TimeUnit.MILLISECONDS);

        // verify the value was actually chunked (guards against it being too small / compressed below chunk.size)
        assertChunked(client, "cid:" + largeKey);

        assertEquals(evCache.get(largeKey), largeValue, "chunked single get did not return the written value");

        // bulk get of multiple chunked values (sync getBulk; async bulk does not support chunking)
        final int count = 3;
        final Map<String, String> kv = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            final String key = "chunk_bulk_" + i + "_" + System.nanoTime();
            final String value = buildLargeValue(3000 + i);
            kv.put(key, value);
            EVCacheLatch l = evCache.set(key, value, EVCacheLatch.Policy.ALL);
            l.await(10000, TimeUnit.MILLISECONDS);
        }
        final Map<String, String> results = evCache.getBulk(kv.keySet().toArray(new String[0]));
        assertNotNull(results);
        assertEquals(results.size(), kv.size(), "chunked getBulk returned wrong number of entries");
        for (Map.Entry<String, String> entry : kv.entrySet()) {
            assertEquals(results.get(entry.getKey()), entry.getValue(), "chunked getBulk failed for key " + entry.getKey());
        }

        // cleanup
        for (Future<Boolean> f : evCache.delete(largeKey)) {
            f.get();
        }
        for (String key : kv.keySet()) {
            for (Future<Boolean> f : evCache.delete(key)) {
                f.get();
            }
        }
    }

    // Exercises chunking together with a real mixed-key bulk request. Requires auto.hash.keys=true: short keys whose
    // canonical form ("cid:"+key) stays within max.key.length remain plain, while long keys that exceed it are hashed.
    // Each group has a large (chunked) and a small (stored directly, below chunk.size) value, so the single getBulk
    // drives all four combinations: plain/hashed x chunked/non-chunked. Plain decodes in one step, hashed in two.
    private void doMixedKeyChunkingTests() throws Exception {
        final EVCacheClient client = manager.getEVCacheClientPool(appName).getEVCacheClientForRead();

        final Map<String, String> kv = new HashMap<>();

        // short keys: canonical form stays under max.key.length (default 200) -> remain plain
        final String plainChunkedKey = "chunked_plain_" + System.nanoTime();
        kv.put(plainChunkedKey, buildLargeValue(3000));
        final String plainNonChunkedKey = "nonchunked_plain_" + System.nanoTime();
        kv.put(plainNonChunkedKey, UUID.randomUUID().toString());

        // long keys: canonical form exceeds max.key.length -> auto-hashed (buildLargeValue(220) guarantees > 200)
        final String hashedChunkedKey = "chunked_hashed_" + buildLargeValue(220);
        kv.put(hashedChunkedKey, buildLargeValue(3000));
        final String hashedNonChunkedKey = "nonchunked_hashed_" + buildLargeValue(220);
        kv.put(hashedNonChunkedKey, UUID.randomUUID().toString());

        for (Map.Entry<String, String> entry : kv.entrySet()) {
            evCache.set(entry.getKey(), entry.getValue(), EVCacheLatch.Policy.ALL).await(10000, TimeUnit.MILLISECONDS);
        }

        // structural proof of the storage layout on the plain path (stored verbatim, no hashing, so introspectable):
        // the large value is split into chunks, the small value is stored under a single key. The hashed equivalents use
        // the same value sizes and their correct round-trip below confirms the hashed write/reassembly path.
        assertChunked(client, "cid:" + plainChunkedKey);
        assertNotChunked(client, "cid:" + plainNonChunkedKey);

        // mixed-key, chunk-aware getBulk: plain keys decode in one step, hashed keys in two steps; chunked values are
        // reassembled first, non-chunked values are decoded directly.
        final Map<String, String> results = evCache.getBulk(kv.keySet().toArray(new String[0]));
        assertNotNull(results);
        assertEquals(results.size(), kv.size(), "mixed-key chunked getBulk returned wrong number of entries");
        for (Map.Entry<String, String> entry : kv.entrySet()) {
            assertEquals(results.get(entry.getKey()), entry.getValue(), "mixed-key chunked getBulk failed for key " + entry.getKey());
        }

        // single get round-trip for hashed (two-step decode) keys, both chunked and non-chunked
        assertEquals(evCache.get(hashedChunkedKey), kv.get(hashedChunkedKey), "chunked single get of hashed key failed");
        assertEquals(evCache.get(hashedNonChunkedKey), kv.get(hashedNonChunkedKey), "non-chunked single get of hashed key failed");

        // cleanup
        for (String key : kv.keySet()) {
            for (Future<Boolean> f : evCache.delete(key)) {
                f.get();
            }
        }
    }

    // getAllChunks returns the chunk keys (<storageKey>_01, _02, ...) when the value was chunked, or a single entry
    // keyed by storageKey itself when stored unchunked. So absence of storageKey in the returned map proves chunking.
    private void assertChunked(EVCacheClient client, String storageKey) throws Exception {
        final Map<String, ?> chunks = client.getAllChunks(storageKey);
        assertNotNull(chunks, "large value should exist in cache for key " + storageKey);
        assertFalse(chunks.containsKey(storageKey),
                "value should have been chunked, but was stored as a single key (too small / compressed below chunk.size): " + storageKey);
    }

    // Inverse of assertChunked: a non-chunked value is returned as a single entry keyed by storageKey itself.
    private void assertNotChunked(EVCacheClient client, String storageKey) throws Exception {
        final Map<String, ?> chunks = client.getAllChunks(storageKey);
        assertNotNull(chunks, "small value should exist in cache for key " + storageKey);
        assertTrue(chunks.containsKey(storageKey), "value should have been stored as a single (non-chunked) key: " + storageKey);
    }

    // Builds an incompressible value (random UUIDs) so its encoded size stays above the chunk size and actually
    // triggers chunking. A repeating/low-entropy value would be compressed below chunk.size and never chunk.
    private String buildLargeValue(int approxBytes) {
        final StringBuilder sb = new StringBuilder(approxBytes + 36);
        while (sb.length() < approxBytes) {
            sb.append(UUID.randomUUID().toString());
        }
        return sb.toString();
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

        String val = evCache.get(key);
        // get
        assertEquals(val, value);

        // delete
        Future<Boolean>[] futures = evCache.delete(key);
        for (Future<Boolean> future : futures) {
            future.get();
        }
    }

    private void testBulkHashed() throws Exception {
        final int count = 3;
        Map<String, String> kv = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            String key = "bulkhashed_" + i;
            String value = "val_bulkhashed_" + i;
            kv.put(key, value);
            EVCacheLatch latch = evCache.set(key, value, EVCacheLatch.Policy.ALL);
            latch.await(10000, TimeUnit.MILLISECONDS);
        }

        Map<String, String> results = evCache.getBulk(kv.keySet().toArray(new String[0]));
        assertNotNull(results);
        assertEquals(results.size(), kv.size());
        for (Map.Entry<String, String> entry : kv.entrySet()) {
            assertEquals(results.get(entry.getKey()), entry.getValue(),
                    "getBulk with all hashed keys failed for key " + entry.getKey());
        }

        CompletableFuture<Map<String, String>> future = evCache.getAsyncBulk(kv.keySet().toArray(new String[0]));
        results = future.get(10000, TimeUnit.MILLISECONDS);
        assertNotNull(results);
        assertEquals(results.size(), kv.size());
        for (Map.Entry<String, String> entry : kv.entrySet()) {
            assertEquals(results.get(entry.getKey()), entry.getValue(),
                    "getAsyncBulk with all hashed keys failed for key " + entry.getKey());
        }

        for (String key : kv.keySet()) {
            Future<Boolean>[] deleteFutures = evCache.delete(key);
            for (Future<Boolean> deleteFuture : deleteFutures) {
                deleteFuture.get();
            }
        }
    }

    private void testWithMixedKeys() throws Exception {

        Map<String, String> kv = new HashMap<>(6);
        String oneLargeKey = null;
        String oneSmallKey = null;
        for (int k = 0; k < 3; k ++) {
            StringBuilder sb = new StringBuilder();
            sb.append("testWithSmallAndLargeKeysMixed");
            for (int i= 0; i < 100; i++) {
                sb.append(System.nanoTime());
            }
            oneLargeKey = sb.toString();
            kv.put(oneLargeKey, UUID.randomUUID().toString());
        }
        for (int k = 3; k < 6; k ++) {
            oneSmallKey = "testWithSmallAndLargeKeysMixed" + System.nanoTime();
            kv.put(oneSmallKey, UUID.randomUUID().toString());
        }

        for (Map.Entry<String, String> entry : kv.entrySet()) {
            EVCacheLatch latch = evCache.set(entry.getKey(), entry.getValue(), EVCacheLatch.Policy.ALL);
            latch.await(10000, TimeUnit.MILLISECONDS);
        }

        // get
        String val = evCache.get(oneLargeKey);
        assertEquals(val, kv.get(oneLargeKey));
        val = evCache.get(oneSmallKey);
        assertEquals(val, kv.get(oneSmallKey));

        // async bulk get
        for (int op : new int[]{0, 1}) {
            Map<String, String> results;
            if (op == 0) {
                CompletableFuture<Map<String, String>> future = evCache.getAsyncBulk(kv.keySet().toArray(new String[0]));
                results = future.get(10000, TimeUnit.MILLISECONDS);
            } else {
                results = evCache.getBulk(kv.keySet().toArray(new String[0]));
            }
            assertEquals(results.size(), kv.size());
            for (Map.Entry<String, String> result : results.entrySet()) {
                assertEquals(result.getValue(), kv.get(result.getKey()));
            }
        }

        // delete
        for (Map.Entry<String, String> entry : kv.entrySet()) {
            Future<Boolean>[] deleteFutures = evCache.delete(entry.getKey());
            for (Future<Boolean> deleteFuture : deleteFutures) {
                deleteFuture.get();
            }
        }

    }

    private void testWithMixedKeysAndCustomTranscoder() throws Exception {

        com.netflix.evcache.EVCache evCache = getNewBuilder().setAppName(appName).setCachePrefix("cid").enableRetry()
                .setTranscoder(new MovieTranscoder())
                .build();

        Map<String, Movie> kv = new HashMap<>(6);
        String oneLargeKey = null;
        String oneSmallKey = null;
        for (int k = 0; k < 3; k ++) {
            StringBuilder sb = new StringBuilder();
            sb.append("testWithSmallAndLargeKeysMixed");
            for (int i= 0; i < 100; i++) {
                sb.append(System.nanoTime());
            }
            oneLargeKey = sb.toString();
            kv.put(oneLargeKey, new Movie(k, String.valueOf(k)));
        }
        for (int k = 3; k < 6; k ++) {
            oneSmallKey = "testWithSmallAndLargeKeysMixed" + System.nanoTime();
            kv.put(oneSmallKey, new Movie(k, String.valueOf(k)));
        }

        for (Map.Entry<String, Movie> entry : kv.entrySet()) {
            EVCacheLatch latch = evCache.set(entry.getKey(), entry.getValue(), EVCacheLatch.Policy.ALL);
            latch.await(10000, TimeUnit.MILLISECONDS);
        }

        // get
        Movie val = evCache.get(oneLargeKey);
        assertEquals(val, kv.get(oneLargeKey));
        val = evCache.get(oneSmallKey);
        assertEquals(val, kv.get(oneSmallKey));

        // async bulk get
        for (int op : new int[]{0, 1}) {
            Map<String, Movie> results;
            if (op == 0) {
                CompletableFuture<Map<String, Movie>> future = evCache.getAsyncBulk(kv.keySet().toArray(new String[0]));
                results = future.get(10000, TimeUnit.MILLISECONDS);
            } else {
                results = evCache.getBulk(kv.keySet().toArray(new String[0]));
            }

            assertEquals(results.size(), kv.size());
            for (Map.Entry<String, Movie> result : results.entrySet()) {
                
                assertEquals(result.getValue(), kv.get(result.getKey()), "Did not get the written value back with op " + (op == 0 ? "getAsyncBulk" : "getBulk"));
            }
        }

        // delete
        for (Map.Entry<String, Movie> entry : kv.entrySet()) {
            Future<Boolean>[] deleteFutures = evCache.delete(entry.getKey());
            for (Future<Boolean> deleteFuture : deleteFutures) {
                deleteFuture.get();
            }
        }

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

        // test expiry
        String key6 = Long.toString(System.currentTimeMillis());
        assertEquals(evCache.incr(key6, 1, 10, 5), 10);
        Thread.sleep(5000);
        assertNull(evCache.get(key6));


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
                    // functionalTestsWithAppLevelAndASGLevelHashingScenarios(); // slow, requires EVCache refresh
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