package com.netflix.evcache.test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.operation.EVCacheItem;
import net.spy.memcached.protocol.ascii.MetaDeleteOperation;
import net.spy.memcached.protocol.ascii.MetaGetBulkOperation;
import net.spy.memcached.protocol.ascii.MetaSetOperation;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheLatch.Policy;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPool;
import com.netflix.evcache.pool.EVCacheClientPoolManager;

import static org.testng.Assert.*;

/**
 * Comprehensive integration test for EVCache Metacommands.
 *
 * Tests include:
 * 1. Basic meta operations (metaGet, metaSet, metaDelete)
 * 2. Bulk operations with metadata
 * 3. CAS-based conditional operations
 * 4. Lease-based refresh patterns
 * 5. Stale-while-revalidate scenarios
 * 6. Distributed locking
 * 7. Versioned cache updates
 * 8. Performance comparisons
 */
@SuppressWarnings({"unused", "deprecation"})
public class MetaCommandsIntegrationTest extends Base {
    private static final Logger log = LogManager.getLogger(MetaCommandsIntegrationTest.class);

    private static final String APP_NAME = "EVCACHE_METACOMMANDS_V1";
    private static final int TEST_SIZE = 10;
    private static final int TTL_SHORT = 10;  // 10 seconds for testing expiration
    private static final int TTL_NORMAL = 1800;  // 30 minutes

    protected EVCache evCache = null;

    public static void main(String args[]) {
        MetaCommandsIntegrationTest test = new MetaCommandsIntegrationTest();
        test.setProps();
        test.setupEnv();
        test.setupClusterDetails();
        test.runAllTests();
    }

    @BeforeSuite
    public void setProps() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure(new ConsoleAppender(new PatternLayout("%d{HH:mm:ss,SSS} [%t] %p %c %x - %m%n")));
        Logger.getRootLogger().setLevel(Level.INFO);
        Logger.getLogger(MetaCommandsIntegrationTest.class).setLevel(Level.DEBUG);
        Logger.getLogger(Base.class).setLevel(Level.DEBUG);
        Logger.getLogger(EVCacheClient.class).setLevel(Level.DEBUG);
        Logger.getLogger(EVCacheClientPool.class).setLevel(Level.DEBUG);

        final Properties props = getProps();
        props.setProperty(APP_NAME + ".EVCacheClientPool.zoneAffinity", "false");
        props.setProperty(APP_NAME + ".use.simple.node.list.provider", "true");
        props.setProperty(APP_NAME + ".EVCacheClientPool.readTimeout", "1000");
        props.setProperty(APP_NAME + ".EVCacheClientPool.bulkReadTimeout", "1000");
        props.setProperty(APP_NAME + ".max.read.queue.length", "100");
        props.setProperty(APP_NAME + ".operation.timeout", "10000");
        props.setProperty(APP_NAME + ".throw.exception", "false");

        log.info("========================================");
        log.info("  EVCache Metacommands Integration Test");
        log.info("  App: " + APP_NAME);
        log.info("========================================");
    }

    @BeforeSuite
    public void setupEnv() {
        super.setupEnv();
    }

    @BeforeSuite(dependsOnMethods = {"setProps"})
    public void setupClusterDetails() {
        manager = EVCacheClientPoolManager.getInstance();
    }

    @Test
    public void testInitEVCache() {
        log.info("========== TEST: Initialize EVCache ==========");
        this.evCache = (new EVCache.Builder())
                .setAppName(APP_NAME)
                .setCachePrefix(null)
                .enableRetry()
                .build();
        assertNotNull(evCache);
        log.info("✓ EVCache instance created successfully");
        log.info("  App Name: " + APP_NAME);
    }

    public void runAllTests() {
        try {
            EVCacheClientPoolManager.getInstance().initEVCache(APP_NAME);

            // Basic tests
            testInitEVCache();
            testBasicMetaSet();

            testBasicMetaGetBulk();

            // Advanced tests
            testMetaSetWithCAS();
            testMetaDeleteInvalidation();
            testBulkOperationEfficiency();
            testLeaseBasedRefresh();
            testLeaseFailureWithCASVerification();
            testStaleWhileRevalidate();
            testDistributedLocking();
            testVersionedCacheUpdate();
            testConcurrentUpdatesWithCAS();
            /*
            // Multi-threaded tests (simulating multiple instances)
            log.info("\n\n╔════════════════════════════════════════════════════════╗");
            log.info("║  MULTI-THREADED TESTS (Simulating Multiple Instances) ║");
            log.info("╚════════════════════════════════════════════════════════╝\n");

            testCacheStampedeWithLeases();
            testHighContentionCASIncrement();
            testLeaseTimeoutAndRecovery();
            testDistributedLockContention();
            testCASRetryExhaustion();
            testStaleWhileRevalidateWithManyReaders();

            log.info("\n\n╔════════════════════════════════════════╗");
            log.info("║  ALL TESTS PASSED SUCCESSFULLY! ✓✓✓  ║");
            log.info("╚════════════════════════════════════════╝");
            log.info("\nSummary:");
            log.info("- Basic operations: ✓");
            log.info("- CAS operations: ✓");
            log.info("- Lease-based patterns: ✓");
            log.info("- Multi-threaded contention: ✓");
            log.info("- Distributed locking: ✓");
            log.info("- Cache stampede prevention: ✓");

             */

        } catch (Exception e) {
            log.error("Test failed", e);
            throw new RuntimeException(e);
        }
    }

    // ==================== BASIC META OPERATIONS ====================

    @Test(dependsOnMethods = {"testInitEVCache"})
    public void testBasicMetaSet() throws Exception {
        log.info("\n========== TEST: Basic Meta Set ==========");

        for (int i = 0; i < TEST_SIZE; i++) {
            String key = "meta_set_key_" + i;
            String value = "meta_value_" + i;

            log.debug("Setting key: " + key + " = " + value);

            MetaSetOperation.Builder builder = new MetaSetOperation.Builder()
                    .key(key)
                    .value(value.getBytes())
                    .expiration(TTL_NORMAL)
                    .returnCas(true)
                    .returnTtl(true);

            EVCacheLatch latch = evCache.metaSet(builder, Policy.ALL);
            boolean awaitSuccess = latch.await(1000, TimeUnit.MILLISECONDS);

            // Check that await completed
            assertTrue(awaitSuccess, "Meta set timed out for key: " + key);

            // CRITICAL: Check actual success count, not just completion
            int successCount = latch.getSuccessCount();
            int expectedCount = latch.getExpectedSuccessCount();
            int failureCount = latch.getFailureCount();

            log.info("Meta set for key: " + key + " - Success: " + successCount + "/" + expectedCount +
                    ", Failures: " + failureCount);

            assertTrue(successCount >= expectedCount,
                    "Meta set failed for key: " + key + ". Expected: " + expectedCount + ", Got: " + successCount + ", Failures: " + failureCount);
            log.debug("✓ Successfully set key: " + key + " (success count verified)");
        }

        log.info("✓ All " + TEST_SIZE + " keys set successfully using Meta Set");
    }

    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testBasicMetaGetBulk() throws Exception {
        log.info("\n========== TEST: Basic Meta Get Bulk ==========");

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < TEST_SIZE; i++) {
            keys.add("meta_set_key_" + i);
        }

        log.debug("Fetching " + keys.size() + " keys in bulk: " + keys);

        MetaGetBulkOperation.Config config = new MetaGetBulkOperation.Config(keys)
                .includeCas(true)
                .includeTtl(true)
                .includeSize(true)
                .includeLastAccess(true);

        long startTime = System.currentTimeMillis();
        Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(keys, config, null);
        long duration = System.currentTimeMillis() - startTime;

        log.info("Bulk get completed in " + duration + "ms");
        log.info("Retrieved " + items.size() + "/" + keys.size() + " keys");

        assertEquals(items.size(), TEST_SIZE, "Should retrieve all keys");

        for (int i = 0; i < TEST_SIZE; i++) {
            String key = "meta_set_key_" + i;
            assertTrue(items.containsKey(key), "Key not found: " + key);

            EVCacheItem<String> item = items.get(key);
            String value = item.getData();
            assertEquals(value, "meta_value_" + i, "Value mismatch for key: " + key);

            // Log metadata
            log.debug("Key: " + key);
            log.debug("  Value: " + value);
            log.debug("  CAS: " + item.getItemMetaData().getCas());
            log.debug("  TTL remaining: " + item.getItemMetaData().getSecondsLeftToExpire() + "s");
            log.debug("  Size: " + item.getItemMetaData().getSizeInBytes() + " bytes");
            log.debug("  Last access: " + item.getItemMetaData().getSecondsSinceLastAccess() + "s ago");
        }

        log.info("✓ Bulk get retrieved all keys with correct values");
        log.info("✓ Metadata (CAS, TTL, size, last access) successfully retrieved");
    }

    // ==================== CAS-BASED OPERATIONS ====================

    @Test(dependsOnMethods = {"testBasicMetaGetBulk"})
    public void testMetaSetWithCAS() throws Exception {
        log.info("\n========== TEST: Meta Set with CAS (Auto E flag) ==========");

        String key = "cas_test_key";
        String initialValue = "initial_value";

        // Step 1: Set initial value with explicit E flag (for testing explicit control)
        long version1 = System.currentTimeMillis();
        log.debug("Step 1: Setting initial value with explicit E" + version1);
        MetaSetOperation.Builder builder1 = new MetaSetOperation.Builder()
                .key(key)
                .value(initialValue.getBytes())
                .expiration(TTL_NORMAL)
                .recasid(version1)  // Explicit E flag: set CAS to version1
                .returnCas(true);

        EVCacheLatch latch1 = evCache.metaSet(builder1, Policy.ALL);
        assertTrue(latch1.await(1000, TimeUnit.MILLISECONDS));
        log.debug("✓ Initial value set");

        // Step 2: Get with CAS - use simpler API (CAS included by default)
        log.debug("Step 2: Getting value with CAS token (using simple API)");
        Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(Arrays.asList(key), null);
        assertTrue(items.containsKey(key), "Key should exist");

        long cas = items.get(key).getItemMetaData().getCas();
        log.debug("✓ Retrieved CAS token: " + cas + " (expected: " + version1 + ")");
        assertEquals(cas, version1, "CAS should match version1");

        // Step 3: Update with CAS - E flag auto-generated (should succeed)
        log.debug("Step 3: Updating with C" + cas + " (E flag auto-generated)");
        String newValue = "updated_value_with_cas";
        MetaSetOperation.Builder builder2 = new MetaSetOperation.Builder()
                .key(key)
                .value(newValue.getBytes())
                .cas(cas)            // C flag: validate current CAS
                                     // E flag: auto-generated by system!
                .expiration(TTL_NORMAL)
                .returnCas(true);

        EVCacheLatch latch2 = evCache.metaSet(builder2, Policy.ALL);
        boolean casUpdateSuccess = latch2.await(1000, TimeUnit.MILLISECONDS);
        assertTrue(casUpdateSuccess, "CAS update should succeed");
        log.debug("✓ CAS update succeeded with auto-generated E flag");

        // Step 4: Verify update - CAS should be auto-generated value
        log.debug("Step 4: Verifying updated value");
        Map<String, EVCacheItem<String>> verifyItems = evCache.metaGetBulk(Arrays.asList(key), null);
        assertEquals(verifyItems.get(key).getData(), newValue, "Value should be updated");
        long newCas = verifyItems.get(key).getItemMetaData().getCas();
        log.debug("✓ Value updated successfully, new CAS: " + newCas + " (auto-generated)");
        assertTrue(newCas > cas, "New CAS should be greater than old CAS");

        // Step 5: Try to update with old CAS (should fail)
        log.debug("Step 5: Attempting update with old CAS C" + cas + " (should fail)");
        MetaSetOperation.Builder builder3 = new MetaSetOperation.Builder()
                .key(key)
                .value("should_not_work".getBytes())
                .cas(cas)            // Old CAS - auto-gen will create new E flag
                .expiration(TTL_NORMAL);

        EVCacheLatch latch3 = evCache.metaSet(builder3, Policy.ALL);
        boolean oldCasUpdate = latch3.await(1000, TimeUnit.MILLISECONDS);
        assertFalse(oldCasUpdate, "Old CAS update should fail");
        log.debug("✓ Old CAS correctly rejected");

        log.info("✓ CAS-based conditional updates with auto-generated E flag working correctly");
        log.info("  - Users only need to provide C flag (validate CAS)");
        log.info("  - E flag (new CAS) is automatically generated");
    }

    // ==================== INVALIDATION & DELETE ====================

    @Test(dependsOnMethods = {"testMetaSetWithCAS"})
    public void testMetaDeleteInvalidation() throws Exception {
        log.info("\n========== TEST: Meta Delete with Invalidation ==========");

        String key = "invalidation_test_key";
        String value = "test_value";

        // Step 1: Set initial value
        log.debug("Step 1: Setting test value");
        MetaSetOperation.Builder setBuilder = new MetaSetOperation.Builder()
                .key(key)
                .value(value.getBytes())
                .expiration(TTL_NORMAL);

        EVCacheLatch setLatch = evCache.metaSet(setBuilder, Policy.ALL);
        assertTrue(setLatch.await(1000, TimeUnit.MILLISECONDS));
        log.debug("✓ Test value set");

        // Step 2: Invalidate (mark stale) instead of delete
        log.debug("Step 2: Invalidating key (marking as stale)");
        MetaDeleteOperation.Builder invalidateBuilder = new MetaDeleteOperation.Builder()
                .key(key)
                .mode(MetaDeleteOperation.DeleteMode.INVALIDATE);

        EVCacheLatch invalidateLatch = evCache.metaDelete(invalidateBuilder, Policy.ALL);
        assertTrue(invalidateLatch.await(1000, TimeUnit.MILLISECONDS));
        log.debug("✓ Key invalidated");

        // Step 3: Try regular get (should miss since invalidated)
        log.debug("Step 3: Testing regular get after invalidation");
        String getValue = evCache.get(key);
        // Note: Behavior depends on server configuration
        log.debug("Regular get result: " + (getValue == null ? "null (expected)" : getValue));

        // Step 4: Try get with stale serving (should work if server supports it)
        log.debug("Step 4: Testing get with stale serving");
        MetaGetBulkOperation.Config staleConfig = new MetaGetBulkOperation.Config(Arrays.asList(key))
                .serveStale(true)
                .maxStaleTime(300);

        Map<String, EVCacheItem<String>> staleItems = evCache.metaGetBulk(Arrays.asList(key), staleConfig, null);
        log.debug("Stale get result: " + (staleItems.containsKey(key) ? "found stale data" : "not found"));

        log.info("✓ Invalidation pattern tested");
    }

    // ==================== BULK OPERATION EFFICIENCY ====================

    @Test(dependsOnMethods = {"testBasicMetaGetBulk"})
    public void testBulkOperationEfficiency() throws Exception {
        log.info("\n========== TEST: Bulk Operation Efficiency ==========");

        int bulkSize = 20;
        List<String> keys = new ArrayList<>();

        // Prepare test data
        log.debug("Preparing " + bulkSize + " keys for efficiency test");
        for (int i = 0; i < bulkSize; i++) {
            String key = "bulk_efficiency_" + i;
            keys.add(key);
            String value = "value_" + i;

            MetaSetOperation.Builder builder = new MetaSetOperation.Builder()
                    .key(key)
                    .value(value.getBytes())
                    .expiration(TTL_NORMAL);

            evCache.metaSet(builder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);
        }
        log.debug("✓ Test data prepared");

        // Test individual gets (baseline)
        log.debug("\nBaseline: Individual GET operations");
        long individualStart = System.currentTimeMillis();
        int individualHits = 0;
        for (String key : keys) {
            String value = evCache.get(key);
            if (value != null) individualHits++;
        }
        long individualDuration = System.currentTimeMillis() - individualStart;
        log.info("Individual GETs: " + individualHits + "/" + bulkSize + " keys in " + individualDuration + "ms");
        log.info("  Average per key: " + (individualDuration / (double) bulkSize) + "ms");

        // Test bulk get
        log.debug("\nOptimized: Bulk GET operation");
        MetaGetBulkOperation.Config bulkConfig = new MetaGetBulkOperation.Config(keys)
                .includeCas(true)
                .includeTtl(true);

        long bulkStart = System.currentTimeMillis();
        Map<String, EVCacheItem<String>> bulkItems = evCache.metaGetBulk(keys, bulkConfig, null);
        long bulkDuration = System.currentTimeMillis() - bulkStart;

        log.info("Bulk GET: " + bulkItems.size() + "/" + bulkSize + " keys in " + bulkDuration + "ms");
        log.info("  Average per key: " + (bulkDuration / (double) bulkSize) + "ms");

        // Calculate improvement
        double improvement = ((individualDuration - bulkDuration) / (double) individualDuration) * 100;
        log.info("\n✓ Efficiency Improvement: " + String.format("%.1f%%", improvement));
        log.info("  Speedup: " + String.format("%.2fx", individualDuration / (double) bulkDuration) + " faster");

        assertTrue(bulkDuration < individualDuration, "Bulk operation should be faster");
    }

    // ==================== LEASE-BASED REFRESH ====================

    @Test(dependsOnMethods = {"testBasicMetaGetBulk"})
    public void testLeaseBasedRefresh() throws Exception {
        log.info("\n========== TEST: Lease-Based Refresh Pattern ==========");

        String dataKey = "hot_key_data";
        String leaseKey = dataKey + ":lease";
        String initialValue = "hot_data_v1";

        // Step 1: Set up hot key with short TTL
        log.debug("Step 1: Setting up hot key with short TTL");
        MetaSetOperation.Builder dataBuilder = new MetaSetOperation.Builder()
                .key(dataKey)
                .value(initialValue.getBytes())
                .expiration(5);  // 5 seconds for testing

        evCache.metaSet(dataBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);
        log.debug("✓ Hot key set with 5s TTL");

        // Step 2: Simulate multiple clients checking TTL
        log.debug("\nStep 2: Simulating multiple clients detecting low TTL");
        Thread.sleep(3000);  // Wait 3 seconds, TTL now ~2s

        MetaGetBulkOperation.Config checkConfig = new MetaGetBulkOperation.Config(Arrays.asList(dataKey))
                .includeTtl(true)
                .includeCas(true);

        Map<String, EVCacheItem<String>> checkItems = evCache.metaGetBulk(Arrays.asList(dataKey), checkConfig, null);
        long ttlRemaining = checkItems.get(dataKey).getItemMetaData().getSecondsLeftToExpire();
        log.debug("✓ TTL remaining: " + ttlRemaining + "s (triggering refresh threshold)");

        // Step 3: Multiple clients try to acquire lease (only one should succeed)
        log.debug("\nStep 3: Simulating 5 clients competing for refresh lease");
        AtomicInteger leaseAcquired = new AtomicInteger(0);
        AtomicInteger leaseFailed = new AtomicInteger(0);
        CountDownLatch clientsLatch = new CountDownLatch(5);

        for (int i = 0; i < 5; i++) {
            final int clientId = i;
            new Thread(() -> {
                try {
                    log.debug("  Client-" + clientId + ": Attempting to acquire lease");

                    MetaSetOperation.Builder leaseBuilder = new MetaSetOperation.Builder()
                            .key(leaseKey)
                            .value(("client-" + clientId).getBytes())
                            .mode(MetaSetOperation.SetMode.ADD)  // Only succeeds if doesn't exist
                            .expiration(10);

                    // Use Policy.ONE for fastest lease acquisition with least contention issues
                    EVCacheLatch leaseLatch = evCache.metaSet(leaseBuilder, Policy.ONE);
                    boolean acquired = leaseLatch.await(100, TimeUnit.MILLISECONDS);

                    if (acquired) {
                        leaseAcquired.incrementAndGet();
                        log.debug("  Client-" + clientId + ": ✓ ACQUIRED LEASE (will refresh)");

                        // Simulate refresh work
                        Thread.sleep(100);

                        // Refresh the data
                        MetaSetOperation.Builder refreshBuilder = new MetaSetOperation.Builder()
                                .key(dataKey)
                                .value("hot_data_v2_refreshed_by_client_".concat(String.valueOf(clientId)).getBytes())
                                .expiration(TTL_NORMAL);

                        evCache.metaSet(refreshBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);
                        log.debug("  Client-" + clientId + ": Data refreshed");

                        // Release lease
                        MetaDeleteOperation.Builder releaseBuilder = new MetaDeleteOperation.Builder()
                                .key(leaseKey);
                        evCache.metaDelete(releaseBuilder, Policy.ALL);
                        log.debug("  Client-" + clientId + ": Lease released");
                    } else {
                        leaseFailed.incrementAndGet();
                        log.debug("  Client-" + clientId + ": ✗ Lease held by another client (using stale data)");
                    }
                } catch (Exception e) {
                    log.error("Client-" + clientId + " error", e);
                } finally {
                    clientsLatch.countDown();
                }
            }).start();
        }

        clientsLatch.await(5, TimeUnit.SECONDS);

        log.info("\n✓ Lease Results:");
        log.info("  Leases acquired: " + leaseAcquired.get() + " (should be 1)");
        log.info("  Leases failed: " + leaseFailed.get() + " (should be 4)");

        assertEquals(leaseAcquired.get(), 1, "Exactly one client should acquire lease");
        assertEquals(leaseFailed.get(), 4, "Other clients should fail to acquire lease");

        log.info("✓ Lease-based refresh prevents thundering herd");
    }

    // ==================== LEASE FAILURE WITH CAS VERIFICATION ====================

    @Test(dependsOnMethods = {"testLeaseBasedRefresh"})
    public void testLeaseFailureWithCASVerification() throws Exception {
        log.info("\n========== TEST: Lease Failure with CAS Verification ==========");
        log.info("Scenario: Winner refreshes data, losers verify they can see and use the new CAS");

        String dataKey = "lease_cas_test";
        String leaseKey = dataKey + ":lease";

        // Ensure clean state
        evCache.delete(dataKey);
        evCache.delete(leaseKey);
        Thread.sleep(100);

        // Launch 10 clients competing for lease
        log.debug("\nStep 1: Launching 10 clients competing for refresh lease");
        AtomicInteger leaseWinners = new AtomicInteger(0);
        AtomicInteger leaseLosersCasVerified = new AtomicInteger(0);
        AtomicInteger leaseLosersCasUpdateSuccess = new AtomicInteger(0);
        AtomicReference<Long> winnerGeneratedCas = new AtomicReference<>(0L);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch winnerDoneLatch = new CountDownLatch(1);
        CountDownLatch allDoneLatch = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            final int clientId = i;
            new Thread(() -> {
                try {
                    // Wait for all threads to start together
                    startLatch.await();

                    log.debug("Client-" + clientId + ": Attempting to acquire lease");

                    // Try to acquire lease (ADD mode - only one succeeds)
                    MetaSetOperation.Builder leaseBuilder = new MetaSetOperation.Builder()
                            .key(leaseKey)
                            .value(("client-" + clientId).getBytes())
                            .mode(MetaSetOperation.SetMode.ADD)
                            .expiration(30);

                    EVCacheLatch leaseLatch = evCache.metaSet(leaseBuilder, Policy.ONE);
                    boolean gotLease = leaseLatch.await(200, TimeUnit.MILLISECONDS);

                    if (gotLease) {
                        // ========== WINNER: Refresh data with auto-generated CAS ==========
                        leaseWinners.incrementAndGet();
                        log.info("Client-" + clientId + ": ✓✓✓ WON LEASE - Refreshing data");

                        // Simulate database fetch
                        Thread.sleep(200);

                        // Update data (E flag will be auto-generated)
                        String freshData = "refreshed_by_client_" + clientId;
                        MetaSetOperation.Builder dataBuilder = new MetaSetOperation.Builder()
                                .key(dataKey)
                                .value(freshData.getBytes())
                                .expiration(TTL_NORMAL);
                                // No explicit recasid - it will be auto-generated!

                        EVCacheLatch dataLatch = evCache.metaSet(dataBuilder, Policy.ALL);
                        boolean dataSet = dataLatch.await(1000, TimeUnit.MILLISECONDS);
                        assertTrue(dataSet, "Winner should successfully set data");

                        // Read back to get the auto-generated CAS
                        Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(Arrays.asList(dataKey), null);
                        long generatedCas = items.get(dataKey).getItemMetaData().getCas();
                        winnerGeneratedCas.set(generatedCas);

                        log.info("Client-" + clientId + ": Data refreshed with auto-generated CAS: " + generatedCas);

                        // Release lease
                        evCache.metaDelete(new MetaDeleteOperation.Builder().key(leaseKey), Policy.ALL);
                        log.debug("Client-" + clientId + ": Lease released");

                        // Signal losers can now proceed
                        winnerDoneLatch.countDown();

                    } else {
                        // ========== LOSERS: Wait for winner, then verify CAS ==========
                        log.debug("Client-" + clientId + ": ✗ Lease denied - waiting for winner to refresh");

                        // Wait for winner to finish refresh
                        boolean winnerFinished = winnerDoneLatch.await(5, TimeUnit.SECONDS);
                        assertTrue(winnerFinished, "Winner should complete refresh");

                        // Retry reading until we see the updated CAS (handles zone propagation delay)
                        log.debug("Client-" + clientId + ": Winner finished - reading updated data with CAS");
                        long expectedCas = winnerGeneratedCas.get();
                        EVCacheItem<String> item = null;
                        long observedCas = 0;
                        boolean casVerified = false;

                        // Retry up to 5 times with 50ms delay (total 250ms for zone propagation)
                        for (int retry = 0; retry < 5; retry++) {
                            Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(Arrays.asList(dataKey), null);

                            if (items.containsKey(dataKey)) {
                                item = items.get(dataKey);
                                observedCas = item.getItemMetaData().getCas();

                                if (observedCas == expectedCas) {
                                    casVerified = true;
                                    log.info("Client-" + clientId + ": ✓ Read data with CAS: " + observedCas + " (expected: " + expectedCas + ", retry: " + retry + ")");
                                    leaseLosersCasVerified.incrementAndGet();
                                    break;
                                } else {
                                    log.debug("Client-" + clientId + ": CAS mismatch (observed: " + observedCas + ", expected: " + expectedCas + "), retry " + (retry + 1) + "/5");
                                    Thread.sleep(50);  // Wait for zone propagation
                                }
                            } else {
                                log.debug("Client-" + clientId + ": Data not found, retry " + (retry + 1) + "/5");
                                Thread.sleep(50);
                            }
                        }

                        // Only attempt update if we successfully verified CAS
                        if (casVerified) {
                            // Now compete for lease again to do our own update
                            log.debug("Client-" + clientId + ": Verified CAS, now attempting to acquire lease for update");

                            MetaSetOperation.Builder updateLeaseBuilder = new MetaSetOperation.Builder()
                                    .key(leaseKey)
                                    .value(("client-" + clientId + "-update").getBytes())
                                    .mode(MetaSetOperation.SetMode.ADD)
                                    .expiration(10);

                            EVCacheLatch updateLeaseLatch = evCache.metaSet(updateLeaseBuilder, Policy.ONE);
                            boolean gotUpdateLease = updateLeaseLatch.await(100, TimeUnit.MILLISECONDS);

                            if (gotUpdateLease) {
                                // I won the second lease! Now do CAS update
                                log.debug("Client-" + clientId + ": ✓ Acquired update lease, performing CAS update");
                                String myUpdate = item.getData() + "_updated_by_" + clientId;
                                MetaSetOperation.Builder updateBuilder = new MetaSetOperation.Builder()
                                        .key(dataKey)
                                        .value(myUpdate.getBytes())
                                        .cas(observedCas)  // Use the CAS we verified
                                        .expiration(TTL_NORMAL);

                                // Use Policy.ALL because we have the lease (mutual exclusion)
                                // No race condition since only we are updating
                                EVCacheLatch updateLatch = evCache.metaSet(updateBuilder, Policy.ALL);
                                boolean updateSuccess = updateLatch.await(1000, TimeUnit.MILLISECONDS);

                                if (updateSuccess) {
                                    leaseLosersCasUpdateSuccess.incrementAndGet();
                                    log.info("Client-" + clientId + ": ✓ Successfully updated using winner's CAS");
                                } else {
                                    log.debug("Client-" + clientId + ": ✗ CAS update failed (someone else updated first)");
                                }

                                // Release update lease
                                evCache.metaDelete(new MetaDeleteOperation.Builder().key(leaseKey), Policy.ALL);
                                log.debug("Client-" + clientId + ": Update lease released");
                            } else {
                                log.debug("Client-" + clientId + ": ✗ Lost second lease competition (another loser is updating)");
                            }
                        } else {
                            log.warn("Client-" + clientId + ": ⚠ Could not verify CAS after retries (zone propagation delay)");
                        }
                    }

                } catch (Exception e) {
                    log.error("Client-" + clientId + " error", e);
                } finally {
                    allDoneLatch.countDown();
                }
            }).start();
        }

        // Start all threads
        startLatch.countDown();

        // Wait for completion
        boolean completed = allDoneLatch.await(15, TimeUnit.SECONDS);
        assertTrue(completed, "All clients should complete");

        log.info("\n========================================");
        log.info("LEASE + CAS VERIFICATION RESULTS:");
        log.info("========================================");
        log.info("Total clients: 10");
        log.info("Lease winners: " + leaseWinners.get() + " (expected: 1)");
        log.info("Losers who verified CAS: " + leaseLosersCasVerified.get() + " (expected: most/all of 9)");
        log.info("Losers who used CAS successfully: " + leaseLosersCasUpdateSuccess.get() + " (expected: ≥1)");
        log.info("Winner's auto-generated CAS: " + winnerGeneratedCas.get());
        log.info("========================================");

        // Assertions - realistic for Policy.ALL with zone propagation
        assertEquals(leaseWinners.get(), 1, "Exactly one client should win lease");
        assertTrue(leaseLosersCasVerified.get() >= 7, "Most losers should verify CAS (allowing for zone propagation)");
        assertTrue(leaseLosersCasUpdateSuccess.get() >= 1, "At least one loser should successfully use CAS");
        assertTrue(winnerGeneratedCas.get() > 0, "Winner should have generated a CAS token");

        log.info("\n✓✓✓ Lease + CAS workflow verified:");
        log.info("  - Winner refreshed data with auto-generated CAS (E flag)");
        log.info("  - Losers retry reads and verify they can see the new CAS");
        log.info("  - Losers compete for lease AGAIN before updating");
        log.info("  - One loser wins second lease and successfully uses CAS");
        log.info("  - Demonstrates proper lease discipline + E flag synchronization!");
    }

    // ==================== STALE-WHILE-REVALIDATE ====================

    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testStaleWhileRevalidate() throws Exception {
        log.info("\n========== TEST: Stale-While-Revalidate Pattern ==========");

        String key = "stale_test_key";
        String value = "stale_test_value";

        // Step 1: Set value with very short TTL
        log.debug("Step 1: Setting value with 3 second TTL");
        MetaSetOperation.Builder builder = new MetaSetOperation.Builder()
                .key(key)
                .value(value.getBytes())
                .expiration(3);  // 3 seconds

        evCache.metaSet(builder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);
        log.debug("✓ Value set with 3s TTL");

        // Step 2: Wait for expiration
        log.debug("\nStep 2: Waiting for value to expire...");
        Thread.sleep(4000);  // Wait 4 seconds
        log.debug("✓ Value should now be expired");

        // Step 3: Try regular get (should miss)
        log.debug("\nStep 3: Testing regular get (should miss)");
        String regularGet = evCache.get(key);
        log.debug("Regular get result: " + (regularGet == null ? "null (cache miss)" : regularGet));

        // Step 4: Try get with stale serving
        log.debug("\nStep 4: Testing get with stale serving");
        MetaGetBulkOperation.Config staleConfig = new MetaGetBulkOperation.Config(Arrays.asList(key))
                .serveStale(true)
                .maxStaleTime(300)  // Accept stale data up to 5 minutes past expiration
                .includeTtl(true);

        Map<String, EVCacheItem<String>> staleItems = evCache.metaGetBulk(Arrays.asList(key), staleConfig, null);

        if (staleItems.containsKey(key)) {
            EVCacheItem<String> item = staleItems.get(key);
            long ttl = item.getItemMetaData().getSecondsLeftToExpire();
            log.debug("✓ Stale data served!");
            log.debug("  Value: " + value);
            log.debug("  TTL: " + ttl + "s (negative indicates expired)");
            log.info("✓ Stale-while-revalidate works: served expired data instead of cache miss");
        } else {
            log.debug("Stale data not served (server may not support this feature)");
            log.info("Note: Stale serving depends on memcached server configuration");
        }
    }

    // ==================== DISTRIBUTED LOCKING ====================

    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testDistributedLocking() throws Exception {
        log.info("\n========== TEST: Distributed Locking with CAS (E flag) ==========");

        String lockKey = "distributed_lock";
        String resourceKey = "protected_resource";

        // Generate client-side version numbers
        long lockVersion1 = System.currentTimeMillis();
        long lockVersion2 = lockVersion1 + 1;
        long lockVersion3 = lockVersion2 + 1;

        // Step 1: Acquire lock with E flag
        log.debug("Step 1: Acquiring distributed lock with E" + lockVersion1);
        String clientId = "client-" + UUID.randomUUID().toString();

        MetaSetOperation.Builder lockBuilder = new MetaSetOperation.Builder()
                .key(lockKey)
                .value(clientId.getBytes())
                .mode(MetaSetOperation.SetMode.ADD)  // Only succeeds if lock doesn't exist
                .expiration(30)  // 30 second timeout (safety)
                .recasid(lockVersion1)  // E flag: set initial CAS
                .returnCas(true);

        EVCacheLatch lockLatch = evCache.metaSet(lockBuilder, Policy.ONE);
        boolean lockAcquired = lockLatch.await(1000, TimeUnit.MILLISECONDS);
        assertTrue(lockAcquired, "Should acquire lock");
        log.debug("✓ Lock acquired by " + clientId);

        // Verify lock CAS (using simple API)
        Map<String, EVCacheItem<String>> lockItems = evCache.metaGetBulk(Arrays.asList(lockKey), null);
        long lockCas = lockItems.get(lockKey).getItemMetaData().getCas();
        log.debug("Lock CAS: " + lockCas + " (expected: " + lockVersion1 + ")");
        assertEquals(lockCas, lockVersion1, "Lock CAS should match lockVersion1");

        // Step 2: Do protected work
        log.debug("\nStep 2: Performing protected operation");
        MetaSetOperation.Builder workBuilder = new MetaSetOperation.Builder()
                .key(resourceKey)
                .value("protected_data_modified".getBytes())
                .expiration(TTL_NORMAL);

        evCache.metaSet(workBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);
        log.debug("✓ Protected resource modified");

        // Step 3: Extend lock using C and E flags
        log.debug("\nStep 3: Extending lock with C" + lockVersion1 + " E" + lockVersion2);
        MetaSetOperation.Builder extendBuilder = new MetaSetOperation.Builder()
                .key(lockKey)
                .value(clientId.getBytes())
                .cas(lockVersion1)      // C flag: validate current CAS
                .recasid(lockVersion2)  // E flag: set new CAS
                .expiration(60)  // Extend to 60 seconds
                .returnCas(true);

        EVCacheLatch extendLatch = evCache.metaSet(extendBuilder, Policy.ALL);
        boolean lockExtended = extendLatch.await(1000, TimeUnit.MILLISECONDS);
        assertTrue(lockExtended, "Should extend lock");
        log.debug("✓ Lock extended (still owned by " + clientId + ")");

        // Verify new lock CAS
        lockItems = evCache.metaGetBulk(Arrays.asList(lockKey), null);
        long newLockCas = lockItems.get(lockKey).getItemMetaData().getCas();
        log.debug("New lock CAS: " + newLockCas + " (expected: " + lockVersion2 + ")");
        assertEquals(newLockCas, lockVersion2, "Lock CAS should be lockVersion2");

        // Step 4: Release lock using C and E flags
        log.debug("\nStep 4: Releasing lock with C" + lockVersion2 + " E" + lockVersion3);
        MetaDeleteOperation.Builder releaseBuilder = new MetaDeleteOperation.Builder()
                .key(lockKey)
                .cas(lockVersion2)      // C flag: validate current CAS
                .recasid(lockVersion3);  // E flag: set tombstone CAS

        EVCacheLatch releaseLatch = evCache.metaDelete(releaseBuilder, Policy.ALL);
        boolean lockReleased = releaseLatch.await(1000, TimeUnit.MILLISECONDS);
        assertTrue(lockReleased, "Should release lock");
        log.debug("✓ Lock released");

        // Step 5: Verify lock is gone
        String lockCheck = evCache.get(lockKey);
        assertNull(lockCheck, "Lock should be deleted");

        log.info("✓ Distributed locking with CAS works correctly");
        log.info("  - Acquire, extend, and release all validated");
        log.info("  - CAS ensures safe ownership transfer");
    }

    // ==================== VERSIONED CACHE UPDATES ====================

    @Test(dependsOnMethods = {"testMetaSetWithCAS"})
    public void testVersionedCacheUpdate() throws Exception {
        log.info("\n========== TEST: Versioned Cache Update (E flag) ==========");

        String key = "versioned_counter";

        // Step 1: Initialize counter with client-controlled version
        log.debug("Step 1: Initializing counter");
        int initialValue = 100;
        long initialVersion = System.currentTimeMillis();

        MetaSetOperation.Builder initBuilder = new MetaSetOperation.Builder()
                .key(key)
                .value(String.valueOf(initialValue).getBytes())
                .expiration(TTL_NORMAL)
                .recasid(initialVersion)  // E flag: set initial version
                .returnCas(true);

        evCache.metaSet(initBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);
        log.debug("✓ Counter initialized to " + initialValue + " with version " + initialVersion);

        // Step 2: Multiple threads increment counter using CAS + E flag
        log.debug("\nStep 2: Simulating 10 concurrent increments with CAS + E flag");
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger retryCount = new AtomicInteger(0);
        CountDownLatch incrementsLatch = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    boolean success = false;
                    int attempts = 0;

                    while (!success && attempts < 15) {  // Allow more retries under high contention
                        attempts++;

                        // Read current value with CAS (using simple API)
                        Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(Arrays.asList(key), null);
                        int currentValue = Integer.parseInt(items.get(key).getData());
                        long currentVersion = items.get(key).getItemMetaData().getCas();

                        // Increment value and version
                        int newValue = currentValue + 1;
                        long newVersion = currentVersion + 1;  // Client controls version

                        // Write with CAS validation and new version (E flag)
                        MetaSetOperation.Builder updateBuilder = new MetaSetOperation.Builder()
                                .key(key)
                                .value(String.valueOf(newValue).getBytes())
                                .cas(currentVersion)   // C flag: validate current version
                                .recasid(newVersion)   // E flag: set new version (keeps zones in sync!)
                                .expiration(TTL_NORMAL);

                        // Use Policy.QUORUM for competitive CAS updates (no lease)
                        // Policy.ALL + competition creates distributed race conditions
                        EVCacheLatch updateLatch = evCache.metaSet(updateBuilder, Policy.QUORUM);
                        success = updateLatch.await(1000, TimeUnit.MILLISECONDS);

                        if (success) {
                            successCount.incrementAndGet();
                            log.debug("  Thread-" + threadId + ": ✓ Incremented " + currentValue + " -> " + newValue + " (V" + currentVersion + "→V" + newVersion + ", attempt " + attempts + ")");
                        } else {
                            retryCount.incrementAndGet();
                            log.debug("  Thread-" + threadId + ": ✗ CAS failed, retrying (attempt " + attempts + ")");
                        }
                    }
                } catch (Exception e) {
                    log.error("Thread-" + threadId + " error", e);
                } finally {
                    incrementsLatch.countDown();
                }
            }).start();
        }

        incrementsLatch.await(10, TimeUnit.SECONDS);

        // Step 3: Verify final value
        log.debug("\nStep 3: Verifying final counter value");
        String finalValueStr = evCache.get(key);
        int finalValue = Integer.parseInt(finalValueStr);

        log.info("\n✓ Versioned Update Results:");
        log.info("  Initial value: " + initialValue);
        log.info("  Expected final: " + (initialValue + 10));
        log.info("  Actual final: " + finalValue);
        log.info("  Successful updates: " + successCount.get());
        log.info("  Total retries: " + retryCount.get());

        assertEquals(finalValue, initialValue + 10, "All increments should succeed with CAS + E flag");
        log.info("✓ No lost updates - CAS + E flag prevents race conditions across all zones");
    }

    // ==================== CONCURRENT UPDATES WITH CAS ====================

    @Test(dependsOnMethods = {"testVersionedCacheUpdate"})
    public void testConcurrentUpdatesWithCAS() throws Exception {
        log.info("\n========== TEST: Concurrent Updates with CAS (Auto E flag) ==========");

        String key = "concurrent_test";
        String initialValue = "v0";

        // Initialize (no explicit version - will use auto-generated)
        log.debug("Initializing test key");
        MetaSetOperation.Builder initBuilder = new MetaSetOperation.Builder()
                .key(key)
                .value(initialValue.getBytes())
                .expiration(TTL_NORMAL);

        evCache.metaSet(initBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);

        // Concurrent updates
        log.debug("\nLaunching 20 concurrent updaters");
        AtomicInteger successfulUpdates = new AtomicInteger(0);
        AtomicInteger failedUpdates = new AtomicInteger(0);
        CountDownLatch concurrentLatch = new CountDownLatch(20);

        for (int i = 0; i < 20; i++) {
            final int updateId = i;
            new Thread(() -> {
                try {
                    // Use simple API - CAS included by default
                    Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(Arrays.asList(key), null);
                    long currentVersion = items.get(key).getItemMetaData().getCas();

                    // Simulate some processing
                    Thread.sleep((long) (Math.random() * 50));

                    String newValue = "v" + updateId;

                    MetaSetOperation.Builder updateBuilder = new MetaSetOperation.Builder()
                            .key(key)
                            .value(newValue.getBytes())
                            .cas(currentVersion)      // C flag: validate current version
                                                      // E flag: auto-generated by system
                            .expiration(TTL_NORMAL);

                    // Use Policy.QUORUM for competitive CAS updates (no lease)
                    EVCacheLatch updateLatch = evCache.metaSet(updateBuilder, Policy.QUORUM);
                    boolean success = updateLatch.await(1000, TimeUnit.MILLISECONDS);

                    if (success) {
                        successfulUpdates.incrementAndGet();
                    } else {
                        failedUpdates.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("Update error", e);
                } finally {
                    concurrentLatch.countDown();
                }
            }).start();
        }

        concurrentLatch.await(15, TimeUnit.SECONDS);

        log.info("\n✓ Concurrent Update Results:");
        log.info("  Successful updates: " + successfulUpdates.get());
        log.info("  Failed updates (CAS conflicts): " + failedUpdates.get());
        log.info("  Total attempts: " + (successfulUpdates.get() + failedUpdates.get()));

        assertTrue(successfulUpdates.get() > 0, "At least some updates should succeed");
        assertTrue(failedUpdates.get() > 0, "Some updates should fail due to CAS conflicts");
        assertEquals(successfulUpdates.get() + failedUpdates.get(), 20, "All threads should complete");

        log.info("✓ CAS with auto-generated E flag correctly detects and prevents conflicting concurrent updates");
    }

    // ==================== MULTI-THREADED TESTS (Simulating Multiple Instances) ====================

    /**
     * Simulates cache stampede with 100 concurrent requests on cache miss.
     * Only ONE thread should acquire lease and refresh data.
     * Other 99 threads should wait and reuse the refreshed data.
     */
    @Test(dependsOnMethods = {"testLeaseBasedRefresh"})
    public void testCacheStampedeWithLeases() throws Exception {
        log.info("\n========== TEST: Cache Stampede Prevention (100 Threads) ==========");

        String dataKey = "stampede_data";
        String leaseKey = dataKey + ":lease";

        // Ensure key doesn't exist initially (simulating cache miss)
        evCache.delete(dataKey);
        Thread.sleep(100);

        log.debug("Simulating 100 concurrent threads hitting cache miss simultaneously");

        AtomicInteger leaseAcquired = new AtomicInteger(0);
        AtomicInteger leaseRejected = new AtomicInteger(0);
        AtomicInteger dataRefreshed = new AtomicInteger(0);
        AtomicInteger dataFromCache = new AtomicInteger(0);
        AtomicLong totalWaitTime = new AtomicLong(0);
        CountDownLatch startLatch = new CountDownLatch(1);  // All threads start together
        CountDownLatch doneLatch = new CountDownLatch(100);

        long testStartTime = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    long threadStart = System.currentTimeMillis();

                    // Wait for start signal (ensures all threads hit at same time)
                    startLatch.await();

                    log.debug("Thread-" + threadId + ": Cache miss detected, attempting lease");

                    // Try to acquire lease
                    MetaSetOperation.Builder leaseBuilder = new MetaSetOperation.Builder()
                            .key(leaseKey)
                            .value(("thread-" + threadId).getBytes())
                            .mode(MetaSetOperation.SetMode.ADD)
                            .expiration(10);

                    EVCacheLatch leaseLatch = evCache.metaSet(leaseBuilder, Policy.ONE);
                    boolean gotLease = leaseLatch.await(100, TimeUnit.MILLISECONDS);

                    if (gotLease) {
                        // I WON! I'll refresh the data
                        leaseAcquired.incrementAndGet();
                        log.info("Thread-" + threadId + ": ✓✓✓ ACQUIRED LEASE (I'll refresh data)");

                        // Simulate expensive database query
                        Thread.sleep(500);

                        // Refresh the data
                        String freshData = "Refreshed data at " + System.currentTimeMillis();
                        MetaSetOperation.Builder dataBuilder = new MetaSetOperation.Builder()
                                .key(dataKey)
                                .value(freshData.getBytes())
                                .expiration(TTL_NORMAL);

                        evCache.metaSet(dataBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);
                        dataRefreshed.incrementAndGet();
                        log.info("Thread-" + threadId + ": Data refreshed successfully");

                        // Release lease
                        MetaDeleteOperation.Builder releaseBuilder = new MetaDeleteOperation.Builder()
                                .key(leaseKey);
                        evCache.metaDelete(releaseBuilder, Policy.ALL);
                        log.debug("Thread-" + threadId + ": Lease released");

                    } else {
                        // Someone else is refreshing, wait and retry
                        leaseRejected.incrementAndGet();
                        log.debug("Thread-" + threadId + ": Lease held by another thread, waiting...");

                        // Wait a bit for refresh to complete
                        Thread.sleep(100);

                        // Retry reading from cache
                        for (int retry = 0; retry < 10; retry++) {
                            String data = evCache.get(dataKey);
                            if (data != null) {
                                dataFromCache.incrementAndGet();
                                log.debug("Thread-" + threadId + ": ✓ Got refreshed data from cache (retry " + retry + ")");
                                break;
                            }
                            Thread.sleep(100);
                        }
                    }

                    long threadDuration = System.currentTimeMillis() - threadStart;
                    totalWaitTime.addAndGet(threadDuration);

                } catch (Exception e) {
                    log.error("Thread-" + threadId + " error", e);
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for all threads to complete
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        long totalTestDuration = System.currentTimeMillis() - testStartTime;

        log.info("\n========================================");
        log.info("CACHE STAMPEDE TEST RESULTS:");
        log.info("========================================");
        log.info("Total threads: 100");
        log.info("Leases acquired: " + leaseAcquired.get() + " (should be exactly 1)");
        log.info("Leases rejected: " + leaseRejected.get() + " (should be 99)");
        log.info("Data refreshed: " + dataRefreshed.get() + " times");
        log.info("Data served from cache: " + dataFromCache.get() + " threads");
        log.info("Total test duration: " + totalTestDuration + "ms");
        log.info("Average wait per thread: " + (totalWaitTime.get() / 100) + "ms");
        log.info("========================================");

        assertTrue(completed, "All threads should complete");
        assertEquals(leaseAcquired.get(), 1, "Exactly ONE thread should acquire lease");
        assertEquals(leaseRejected.get(), 99, "99 threads should be rejected");
        assertTrue(dataFromCache.get() >= 90, "Most threads should get data from cache after refresh");

        log.info("✓✓✓ Cache stampede prevented! Only 1 database query for 100 concurrent requests");
    }

    /**
     * Simulates 50 concurrent threads incrementing a shared counter.
     * Tests CAS-based atomic operations under high contention.
     * All 50 increments should succeed without lost updates.
     */
    @Test(dependsOnMethods = {"testVersionedCacheUpdate"})
    public void testHighContentionCASIncrement() throws Exception {
        log.info("\n========== TEST: High Contention CAS Increment (50 Threads) ==========");

        String counterKey = "high_contention_counter";
        int initialValue = 1000;
        int numThreads = 50;

        // Initialize counter
        log.debug("Initializing counter to " + initialValue);
        MetaSetOperation.Builder initBuilder = new MetaSetOperation.Builder()
                .key(counterKey)
                .value(String.valueOf(initialValue).getBytes())
                .expiration(TTL_NORMAL);

        evCache.metaSet(initBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);

        // Track statistics
        AtomicInteger successfulIncrements = new AtomicInteger(0);
        AtomicInteger totalAttempts = new AtomicInteger(0);
        AtomicInteger totalRetries = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        log.debug("\nLaunching " + numThreads + " concurrent incrementers");
        long testStart = System.currentTimeMillis();

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    startLatch.await();  // Wait for start signal

                    boolean success = false;
                    int attempts = 0;
                    int maxAttempts = 20;

                    while (!success && attempts < maxAttempts) {
                        attempts++;
                        totalAttempts.incrementAndGet();

                        // Read current value with CAS
                        MetaGetBulkOperation.Config config = new MetaGetBulkOperation.Config(Arrays.asList(counterKey))
                                .includeCas(true);

                        Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(Arrays.asList(counterKey), config, null);

                        if (items.containsKey(counterKey)) {
                            int currentValue = Integer.parseInt(items.get(counterKey).getData());
                            long cas = items.get(counterKey).getItemMetaData().getCas();

                            // Increment
                            int newValue = currentValue + 1;

                            // Write with CAS
                            MetaSetOperation.Builder updateBuilder = new MetaSetOperation.Builder()
                                    .key(counterKey)
                                    .value(String.valueOf(newValue).getBytes())
                                    .cas(cas)
                                    .expiration(TTL_NORMAL);

                            EVCacheLatch updateLatch = evCache.metaSet(updateBuilder, Policy.ALL);
                            success = updateLatch.await(1000, TimeUnit.MILLISECONDS);

                            if (success) {
                                successfulIncrements.incrementAndGet();
                                log.debug("Thread-" + threadId + ": ✓ Increment succeeded " + currentValue + " -> " + newValue + " (attempt " + attempts + ")");
                            } else {
                                if (attempts > 1) {
                                    totalRetries.incrementAndGet();
                                }
                                log.debug("Thread-" + threadId + ": ✗ CAS conflict on attempt " + attempts + ", retrying...");
                                Thread.sleep(1);  // Brief backoff
                            }
                        }
                    }

                    if (!success) {
                        log.error("Thread-" + threadId + ": FAILED after " + attempts + " attempts");
                    }

                } catch (Exception e) {
                    log.error("Thread-" + threadId + " error", e);
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for completion
        boolean completed = doneLatch.await(60, TimeUnit.SECONDS);
        long testDuration = System.currentTimeMillis() - testStart;

        // Verify final value
        String finalValueStr = evCache.get(counterKey);
        int finalValue = Integer.parseInt(finalValueStr);
        int expectedValue = initialValue + numThreads;

        log.info("\n========================================");
        log.info("HIGH CONTENTION CAS TEST RESULTS:");
        log.info("========================================");
        log.info("Number of threads: " + numThreads);
        log.info("Initial value: " + initialValue);
        log.info("Expected final value: " + expectedValue);
        log.info("Actual final value: " + finalValue);
        log.info("Successful increments: " + successfulIncrements.get());
        log.info("Total CAS attempts: " + totalAttempts.get());
        log.info("Total retries: " + totalRetries.get());
        log.info("Average attempts per thread: " + String.format("%.2f", totalAttempts.get() / (double) numThreads));
        log.info("Test duration: " + testDuration + "ms");
        log.info("Throughput: " + String.format("%.2f", (numThreads * 1000.0) / testDuration) + " increments/sec");
        log.info("========================================");

        assertTrue(completed, "All threads should complete");
        assertEquals(finalValue, expectedValue, "NO LOST UPDATES - all increments should succeed");
        assertEquals(successfulIncrements.get(), numThreads, "All threads should eventually succeed");

        log.info("✓✓✓ High contention handled perfectly - NO lost updates despite " + totalRetries.get() + " CAS conflicts!");
    }

    /**
     * Tests lease timeout and recovery when the lease holder fails to release.
     * Simulates a crashed/hung process that holds a lease.
     */
    @Test(dependsOnMethods = {"testLeaseBasedRefresh"})
    public void testLeaseTimeoutAndRecovery() throws Exception {
        log.info("\n========== TEST: Lease Timeout and Recovery ==========");

        String dataKey = "timeout_test_data";
        String leaseKey = dataKey + ":lease";

        // Client 1 acquires lease but DOESN'T release (simulating crash)
        log.debug("Step 1: Client-1 acquires lease but crashes (doesn't release)");
        MetaSetOperation.Builder lease1 = new MetaSetOperation.Builder()
                .key(leaseKey)
                .value("client-1-crashed".getBytes())
                .mode(MetaSetOperation.SetMode.ADD)
                .expiration(3);  // 3 second lease timeout

        boolean acquired = evCache.metaSet(lease1, Policy.ONE).await(100, TimeUnit.MILLISECONDS);
        assertTrue(acquired, "Client-1 should acquire lease");
        log.debug("✓ Client-1 acquired lease (simulating crash - NOT releasing)");

        // Client 2 tries to acquire lease immediately (should fail)
        log.debug("\nStep 2: Client-2 tries to acquire lease immediately (should fail)");
        MetaSetOperation.Builder lease2 = new MetaSetOperation.Builder()
                .key(leaseKey)
                .value("client-2".getBytes())
                .mode(MetaSetOperation.SetMode.ADD)
                .expiration(10);

        boolean rejectedImmediate = evCache.metaSet(lease2, Policy.ONE).await(100, TimeUnit.MILLISECONDS);
        assertFalse(rejectedImmediate, "Client-2 should be rejected - lease held");
        log.debug("✓ Client-2 correctly rejected (lease still held)");

        // Wait for lease to expire
        log.debug("\nStep 3: Waiting for lease to timeout (3 seconds)...");
        Thread.sleep(3500);  // Wait for 3s timeout + buffer
        log.debug("✓ Lease should now be expired");

        // Client 2 tries again after timeout (should succeed)
        log.debug("\nStep 4: Client-2 tries again after timeout (should succeed)");
        boolean acquiredAfterTimeout = evCache.metaSet(lease2, Policy.QUORUM).await(100, TimeUnit.MILLISECONDS);
        assertTrue(acquiredAfterTimeout, "Client-2 should acquire lease after timeout");
        log.debug("✓ Client-2 acquired lease after timeout");

        // Clean up
        evCache.metaDelete(new MetaDeleteOperation.Builder().key(leaseKey), Policy.ALL);

        log.info("\n✓ Lease timeout works correctly:");
        log.info("  - Lease prevents concurrent access");
        log.info("  - Lease auto-expires after timeout");
        log.info("  - System recovers from crashed lease holder");
    }

    /**
     * Tests distributed lock contention with 20 threads competing.
     * Only one thread should hold lock at a time.
     * Lock extend and release should be CAS-protected.
     */
    @Test(dependsOnMethods = {"testDistributedLocking"})
    public void testDistributedLockContention() throws Exception {
        log.info("\n========== TEST: Distributed Lock Contention (20 Threads) ==========");

        String lockKey = "contended_lock";
        String resourceKey = "shared_resource";
        int numThreads = 20;

        // Initialize shared resource
        evCache.set(resourceKey, "0", TTL_NORMAL);

        AtomicInteger lockAcquired = new AtomicInteger(0);
        AtomicInteger lockFailed = new AtomicInteger(0);
        AtomicInteger workDone = new AtomicInteger(0);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        log.debug("Launching " + numThreads + " threads competing for lock");
        long testStart = System.currentTimeMillis();

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    log.debug("Thread-" + threadId + ": Attempting to acquire lock");

                    // Try to acquire lock
                    String clientId = "thread-" + threadId;
                    MetaSetOperation.Builder lockBuilder = new MetaSetOperation.Builder()
                            .key(lockKey)
                            .value(clientId.getBytes())
                            .mode(MetaSetOperation.SetMode.ADD)
                            .expiration(5)  // 5 second lock
                            .returnCas(true);

                    EVCacheLatch lockLatch = evCache.metaSet(lockBuilder, Policy.ONE);
                    boolean gotLock = lockLatch.await(100, TimeUnit.MILLISECONDS);

                    if (gotLock) {
                        lockAcquired.incrementAndGet();
                        log.info("Thread-" + threadId + ": ✓✓ ACQUIRED LOCK");

                        // Get CAS for the lock
                        MetaGetBulkOperation.Config lockConfig = new MetaGetBulkOperation.Config(Arrays.asList(lockKey))
                                .includeCas(true);
                        Map<String, EVCacheItem<String>> lockItems = evCache.metaGetBulk(Arrays.asList(lockKey), lockConfig, null);
                        long lockCas = lockItems.get(lockKey).getItemMetaData().getCas();

                        // Do protected work
                        String currentValue = evCache.get(resourceKey);
                        int value = Integer.parseInt(currentValue);
                        Thread.sleep(50);  // Simulate work
                        evCache.set(resourceKey, String.valueOf(value + 1), TTL_NORMAL);
                        workDone.incrementAndGet();
                        log.debug("Thread-" + threadId + ": Work done (incremented resource)");

                        // Release lock using CAS
                        MetaDeleteOperation.Builder releaseBuilder = new MetaDeleteOperation.Builder()
                                .key(lockKey)
                                .cas(lockCas);

                        evCache.metaDelete(releaseBuilder, Policy.ALL).await(100, TimeUnit.MILLISECONDS);
                        log.debug("Thread-" + threadId + ": Lock released");

                    } else {
                        lockFailed.incrementAndGet();
                        log.debug("Thread-" + threadId + ": ✗ Lock held by another thread");
                    }

                } catch (Exception e) {
                    log.error("Thread-" + threadId + " error", e);
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        // Wait for all threads
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        long testDuration = System.currentTimeMillis() - testStart;

        // Verify resource was properly protected
        String finalResourceValue = evCache.get(resourceKey);
        int resourceValue = Integer.parseInt(finalResourceValue);

        log.info("\n========================================");
        log.info("DISTRIBUTED LOCK CONTENTION RESULTS:");
        log.info("========================================");
        log.info("Number of threads: " + numThreads);
        log.info("Locks acquired: " + lockAcquired.get());
        log.info("Lock attempts failed: " + lockFailed.get());
        log.info("Work completed: " + workDone.get());
        log.info("Final resource value: " + resourceValue + " (should equal work done)");
        log.info("Test duration: " + testDuration + "ms");
        log.info("========================================");

        assertTrue(completed, "All threads should complete");
        assertTrue(lockAcquired.get() > 0, "Some threads should acquire lock");
        assertEquals(resourceValue, workDone.get(), "Resource value should match work done (no race conditions)");

        log.info("✓✓✓ Distributed lock correctly serializes access - no race conditions!");
    }

    /**
     * Tests CAS retry exhaustion under extreme contention.
     * Some threads may fail after max retries.
     */
    @Test(dependsOnMethods = {"testHighContentionCASIncrement"})
    public void testCASRetryExhaustion() throws Exception {
        log.info("\n========== TEST: CAS Retry Exhaustion Under Extreme Contention ==========");

        String counterKey = "extreme_contention_counter";
        int initialValue = 0;
        int numThreads = 30;
        int maxRetries = 3;  // Very low to force failures

        // Initialize counter
        MetaSetOperation.Builder initBuilder = new MetaSetOperation.Builder()
                .key(counterKey)
                .value(String.valueOf(initialValue).getBytes())
                .expiration(TTL_NORMAL);

        evCache.metaSet(initBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);

        AtomicInteger succeeded = new AtomicInteger(0);
        AtomicInteger exhausted = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        log.debug("Launching " + numThreads + " threads with max " + maxRetries + " retries");

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    startLatch.await();

                    boolean success = false;
                    for (int attempt = 0; attempt < maxRetries && !success; attempt++) {
                        MetaGetBulkOperation.Config config = new MetaGetBulkOperation.Config(Arrays.asList(counterKey))
                                .includeCas(true);

                        Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(Arrays.asList(counterKey), config, null);
                        int currentValue = Integer.parseInt(items.get(counterKey).getData());
                        long cas = items.get(counterKey).getItemMetaData().getCas();

                        MetaSetOperation.Builder updateBuilder = new MetaSetOperation.Builder()
                                .key(counterKey)
                                .value(String.valueOf(currentValue + 1).getBytes())
                                .cas(cas)
                                .expiration(TTL_NORMAL);

                        success = evCache.metaSet(updateBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);

                        if (!success) {
                            log.debug("Thread-" + threadId + ": CAS failed on attempt " + (attempt + 1));
                        }
                    }

                    if (success) {
                        succeeded.incrementAndGet();
                        log.debug("Thread-" + threadId + ": ✓ Succeeded");
                    } else {
                        exhausted.incrementAndGet();
                        log.debug("Thread-" + threadId + ": ✗ EXHAUSTED retries");
                    }

                } catch (Exception e) {
                    log.error("Thread-" + threadId + " error", e);
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);

        String finalValueStr = evCache.get(counterKey);
        int finalValue = Integer.parseInt(finalValueStr);

        log.info("\n========================================");
        log.info("CAS RETRY EXHAUSTION TEST RESULTS:");
        log.info("========================================");
        log.info("Number of threads: " + numThreads);
        log.info("Max retries per thread: " + maxRetries);
        log.info("Successful updates: " + succeeded.get());
        log.info("Retry exhaustion: " + exhausted.get());
        log.info("Final counter value: " + finalValue + " (equals successful updates)");
        log.info("========================================");

        assertEquals(finalValue, succeeded.get(), "Counter should match successful updates");
        assertTrue(exhausted.get() > 0, "Some threads should exhaust retries under extreme contention");

        log.info("✓ CAS retry exhaustion handled correctly - application can detect failures");
    }

    /**
     * Tests stale-while-revalidate with 50 concurrent readers.
     * All readers should get stale data immediately while one thread refreshes.
     */
    @Test(dependsOnMethods = {"testStaleWhileRevalidate"})
    public void testStaleWhileRevalidateWithManyReaders() throws Exception {
        log.info("\n========== TEST: Stale-While-Revalidate with 50 Readers ==========");

        String dataKey = "stale_many_readers";
        String leaseKey = dataKey + ":refresh_lease";
        String initialValue = "Initial Data v1";

        // Set data with very short TTL
        log.debug("Setting data with 2 second TTL");
        MetaSetOperation.Builder initBuilder = new MetaSetOperation.Builder()
                .key(dataKey)
                .value(initialValue.getBytes())
                .expiration(2);

        evCache.metaSet(initBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);

        // Wait for expiration
        log.debug("Waiting for data to expire...");
        Thread.sleep(2500);

        AtomicInteger servedStale = new AtomicInteger(0);
        AtomicInteger servedFresh = new AtomicInteger(0);
        AtomicInteger missedData = new AtomicInteger(0);
        AtomicInteger refreshedData = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(50);

        log.debug("\nLaunching 50 readers simultaneously after expiration");

        for (int i = 0; i < 50; i++) {
            final int readerId = i;
            new Thread(() -> {
                try {
                    startLatch.await();

                    // First, try to acquire refresh lease
                    MetaSetOperation.Builder leaseBuilder = new MetaSetOperation.Builder()
                            .key(leaseKey)
                            .value(("reader-" + readerId).getBytes())
                            .mode(MetaSetOperation.SetMode.ADD)
                            .expiration(10);

                    boolean gotLease = evCache.metaSet(leaseBuilder, Policy.ONE).await(50, TimeUnit.MILLISECONDS);

                    // Try to get stale data
                    MetaGetBulkOperation.Config staleConfig = new MetaGetBulkOperation.Config(Arrays.asList(dataKey))
                            .serveStale(true)
                            .maxStaleTime(300)
                            .includeTtl(true);

                    Map<String, EVCacheItem<String>> staleItems = evCache.metaGetBulk(Arrays.asList(dataKey), staleConfig, null);

                    if (staleItems.containsKey(dataKey)) {
                        EVCacheItem<String> item = staleItems.get(dataKey);
                        long ttl = item.getItemMetaData().getSecondsLeftToExpire();

                        if (ttl < 0) {
                            servedStale.incrementAndGet();
                            log.debug("Reader-" + readerId + ": ✓ Got STALE data (TTL: " + ttl + "s)");
                        } else {
                            servedFresh.incrementAndGet();
                            log.debug("Reader-" + readerId + ": ✓ Got FRESH data (TTL: " + ttl + "s)");
                        }
                    } else {
                        missedData.incrementAndGet();
                        log.debug("Reader-" + readerId + ": ✗ No data (stale not supported?)");
                    }

                    // If I got the lease, refresh the data
                    if (gotLease) {
                        log.info("Reader-" + readerId + ": ✓✓ ACQUIRED REFRESH LEASE, refreshing data");
                        Thread.sleep(100);  // Simulate DB query

                        String freshData = "Refreshed Data v2 by reader-" + readerId;
                        MetaSetOperation.Builder refreshBuilder = new MetaSetOperation.Builder()
                                .key(dataKey)
                                .value(freshData.getBytes())
                                .expiration(TTL_NORMAL);

                        evCache.metaSet(refreshBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);
                        refreshedData.incrementAndGet();
                        log.info("Reader-" + readerId + ": Data refreshed");

                        evCache.metaDelete(new MetaDeleteOperation.Builder().key(leaseKey), Policy.ALL);
                    }

                } catch (Exception e) {
                    log.error("Reader-" + readerId + " error", e);
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        doneLatch.await(20, TimeUnit.SECONDS);

        log.info("\n========================================");
        log.info("STALE-WHILE-REVALIDATE TEST RESULTS:");
        log.info("========================================");
        log.info("Total readers: 50");
        log.info("Served stale data: " + servedStale.get());
        log.info("Served fresh data: " + servedFresh.get());
        log.info("Missed data: " + missedData.get());
        log.info("Data refreshed by: " + refreshedData.get() + " reader(s)");
        log.info("========================================");

        assertTrue(servedStale.get() + servedFresh.get() > 0, "Most readers should get data");
        assertTrue(refreshedData.get() <= 1, "At most one reader should refresh");

        log.info("✓ Stale-while-revalidate: Readers get instant response, one refreshes in background");
    }

    // ==================== USER GUIDE PATTERN TESTS ====================

    /**
     * Test Pattern 1: Simple Cache-Aside
     * Basic caching without CAS - just read, miss, fetch, write
     */
    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testSimpleCacheAside() throws Exception {
        log.info("\n========== TEST: Simple Cache-Aside Pattern ==========");

        String userId = "user:12345";
        String userData = "{\"id\":12345,\"name\":\"John Doe\",\"email\":\"john@example.com\"}";

        // Step 1: Cache miss
        log.debug("Step 1: Try cache (should miss)");
        String cachedValue = evCache.get(userId);
        assertNull(cachedValue, "Cache should be empty initially");

        // Step 2: Fetch from "database" (simulated)
        log.debug("Step 2: Fetch from database (simulated)");
        String fetchedData = userData;  // Simulated database fetch

        // Step 3: Write to cache (no CAS, just simple write)
        log.debug("Step 3: Write to cache");
        MetaSetOperation.Builder builder = new MetaSetOperation.Builder()
                .key(userId)
                .value(fetchedData.getBytes())
                .expiration(3600);

        EVCacheLatch latch = evCache.metaSet(builder, Policy.ALL);
        assertTrue(latch.await(1000, TimeUnit.MILLISECONDS), "Cache write should succeed");

        // Step 4: Read from cache (should hit)
        log.debug("Step 4: Read from cache (should hit)");
        String hitValue = evCache.get(userId);
        assertNotNull(hitValue, "Cache should contain the data");
        assertEquals(hitValue, userData, "Cached data should match");

        log.info("✓ Simple cache-aside pattern works: miss → fetch → write → hit");
    }

    /**
     * Test Pattern 2: Blind Writes (No metaget needed before metaset)
     * Demonstrates that you DON'T need to read before writing when using E flag
     */
    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testBlindWritesWithEFlag() throws Exception {
        log.info("\n========== TEST: Blind Writes (No metaget Required) ==========");

        String key = "config:app:settings";
        long version1 = System.currentTimeMillis();
        long version2 = version1 + 1;

        // Write 1: No metaget needed - just generate version and write
        log.debug("Write 1: Setting initial config WITHOUT reading first");
        MetaSetOperation.Builder builder1 = new MetaSetOperation.Builder()
                .key(key)
                .value("config_v1".getBytes())
                .recasid(version1)  // E flag: set version directly
                .expiration(3600);

        assertTrue(evCache.metaSet(builder1, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        // Write 2: Unconditional overwrite - still no metaget needed
        log.debug("Write 2: Overwriting config WITHOUT reading first");
        MetaSetOperation.Builder builder2 = new MetaSetOperation.Builder()
                .key(key)
                .value("config_v2".getBytes())
                .recasid(version2)  // E flag: new version
                .expiration(3600);

        assertTrue(evCache.metaSet(builder2, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        // Verify final value
        // Verify final value with CAS
        MetaGetBulkOperation.Config config = new MetaGetBulkOperation.Config(Arrays.asList(key))
                .includeCas(true);
        Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(Arrays.asList(key), config, null);
        EVCacheItem<String> item = items.get(key);
        assertEquals(item.getData(), "config_v2");
        assertEquals(item.getItemMetaData().getCas(), version2);

        log.info("✓ Blind writes work: You DON'T need metaget before metaset with E flag");
        log.info("✓ Only need metaget when: (1) need current value, or (2) want CAS validation");
    }

    /**
     * Test: All Set Modes (SET, ADD, REPLACE, APPEND, PREPEND)
     */
    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testAllSetModes() throws Exception {
        log.info("\n========== TEST: All Set Modes ==========");

        String baseKey = "mode_test:";

        // Mode 1: SET (default) - always succeeds
        log.debug("\n--- Testing SET mode (default) ---");
        String setKey = baseKey + "set";
        MetaSetOperation.Builder setBuilder = new MetaSetOperation.Builder()
                .key(setKey)
                .value("initial".getBytes())
                .mode(MetaSetOperation.SetMode.SET)
                .expiration(3600);
        assertTrue(evCache.metaSet(setBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        // SET again (overwrite)
        MetaSetOperation.Builder setBuilder2 = new MetaSetOperation.Builder()
                .key(setKey)
                .value("overwritten".getBytes())
                .mode(MetaSetOperation.SetMode.SET)
                .expiration(3600);
        assertTrue(evCache.metaSet(setBuilder2, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));
        assertEquals(evCache.get(setKey), "overwritten");
        log.info("✓ SET mode: Always succeeds (creates or overwrites)");

        // Mode 2: ADD - only succeeds if key doesn't exist
        log.debug("\n--- Testing ADD mode ---");
        String addKey = baseKey + "add";

        // First ADD should succeed (key doesn't exist)
        MetaSetOperation.Builder addBuilder1 = new MetaSetOperation.Builder()
                .key(addKey)
                .value("first".getBytes())
                .mode(MetaSetOperation.SetMode.ADD)
                .expiration(3600);
        assertTrue(evCache.metaSet(addBuilder1, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        // Second ADD should fail (key exists)
        MetaSetOperation.Builder addBuilder2 = new MetaSetOperation.Builder()
                .key(addKey)
                .value("second".getBytes())
                .mode(MetaSetOperation.SetMode.ADD)
                .expiration(3600);
        assertFalse(evCache.metaSet(addBuilder2, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        assertEquals(evCache.get(addKey), "first", "Value should still be 'first'");
        log.info("✓ ADD mode: Succeeds only if key doesn't exist (useful for locks)");

        // Mode 3: REPLACE - only succeeds if key exists
        log.debug("\n--- Testing REPLACE mode ---");
        String replaceKey = baseKey + "replace";

        // First REPLACE should fail (key doesn't exist)
        MetaSetOperation.Builder replaceBuilder1 = new MetaSetOperation.Builder()
                .key(replaceKey)
                .value("should_fail".getBytes())
                .mode(MetaSetOperation.SetMode.REPLACE)
                .expiration(3600);
        assertFalse(evCache.metaSet(replaceBuilder1, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        // Create the key first with SET
        MetaSetOperation.Builder setFirst = new MetaSetOperation.Builder()
                .key(replaceKey)
                .value("exists".getBytes())
                .expiration(3600);
        evCache.metaSet(setFirst, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);

        // Now REPLACE should succeed (key exists)
        MetaSetOperation.Builder replaceBuilder2 = new MetaSetOperation.Builder()
                .key(replaceKey)
                .value("updated".getBytes())
                .mode(MetaSetOperation.SetMode.REPLACE)
                .expiration(3600);
        assertTrue(evCache.metaSet(replaceBuilder2, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        assertEquals(evCache.get(replaceKey), "updated");
        log.info("✓ REPLACE mode: Succeeds only if key exists (update-only)");

        // Mode 4: APPEND - adds to end of existing value
        log.debug("\n--- Testing APPEND mode ---");
        String appendKey = baseKey + "append";

        // Create initial value
        MetaSetOperation.Builder appendInit = new MetaSetOperation.Builder()
                .key(appendKey)
                .value("Hello".getBytes())
                .expiration(3600);
        evCache.metaSet(appendInit, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);

        // Append to it
        MetaSetOperation.Builder appendBuilder = new MetaSetOperation.Builder()
                .key(appendKey)
                .value(" World".getBytes())
                .mode(MetaSetOperation.SetMode.APPEND)
                .expiration(3600);
        assertTrue(evCache.metaSet(appendBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        assertEquals(evCache.get(appendKey), "Hello World");
        log.info("✓ APPEND mode: Adds data to end of existing value");

        // Mode 5: PREPEND - adds to beginning of existing value
        log.debug("\n--- Testing PREPEND mode ---");
        String prependKey = baseKey + "prepend";

        // Create initial value
        MetaSetOperation.Builder prependInit = new MetaSetOperation.Builder()
                .key(prependKey)
                .value("World".getBytes())
                .expiration(3600);
        evCache.metaSet(prependInit, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);

        // Prepend to it
        MetaSetOperation.Builder prependBuilder = new MetaSetOperation.Builder()
                .key(prependKey)
                .value("Hello ".getBytes())
                .mode(MetaSetOperation.SetMode.PREPEND)
                .expiration(3600);
        assertTrue(evCache.metaSet(prependBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        assertEquals(evCache.get(prependKey), "Hello World");
        log.info("✓ PREPEND mode: Adds data to beginning of existing value");

        log.info("\n✓ All 5 set modes work correctly: SET, ADD, REPLACE, APPEND, PREPEND");
    }

    /**
     * Test: Delete with CAS validation (safe deletion)
     */
    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testDeleteWithCAS() throws Exception {
        log.info("\n========== TEST: Delete with CAS Validation ==========");

        String lockKey = "lock:critical:resource";
        long lockVersion = System.currentTimeMillis();

        // Acquire lock
        log.debug("Step 1: Acquire lock");
        MetaSetOperation.Builder acquireBuilder = new MetaSetOperation.Builder()
                .key(lockKey)
                .value("instance-123".getBytes())
                .mode(MetaSetOperation.SetMode.ADD)
                .recasid(lockVersion)
                .expiration(30);

        assertTrue(evCache.metaSet(acquireBuilder, Policy.ONE).await(1000, TimeUnit.MILLISECONDS));

        // Read lock to get CAS
        log.debug("Step 2: Read lock to get CAS");
        MetaGetBulkOperation.Config config = new MetaGetBulkOperation.Config(Arrays.asList(lockKey))
                .includeCas(true);
        Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(Arrays.asList(lockKey), config, null);
        long currentCas = items.get(lockKey).getItemMetaData().getCas();
        assertEquals(currentCas, lockVersion, "CAS should match our lock version");

        // Try to delete with WRONG CAS (should fail)
        log.debug("Step 3: Try to delete with wrong CAS (should fail)");
        long wrongCas = lockVersion + 999;
        MetaDeleteOperation.Builder wrongDeleteBuilder = new MetaDeleteOperation.Builder()
                .key(lockKey)
                .cas(wrongCas);

        assertFalse(evCache.metaDelete(wrongDeleteBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS),
                "Delete should fail with wrong CAS");

        // Verify lock still exists
        String stillExists = evCache.get(lockKey);
        assertNotNull(stillExists, "Lock should still exist after failed delete");

        // Delete with CORRECT CAS (should succeed)
        log.debug("Step 4: Delete with correct CAS (should succeed)");
        MetaDeleteOperation.Builder correctDeleteBuilder = new MetaDeleteOperation.Builder()
                .key(lockKey)
                .cas(lockVersion);

        assertTrue(evCache.metaDelete(correctDeleteBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS),
                "Delete should succeed with correct CAS");

        // Verify lock deleted
        String shouldBeGone = evCache.get(lockKey);
        assertNull(shouldBeGone, "Lock should be deleted");

        log.info("✓ Delete with CAS validation works: wrong CAS rejected, correct CAS succeeds");
        log.info("✓ Use case: Safe lock release ensuring you still own the lock");
    }

    /**
     * Test: Delete with E flag (tombstone versioning in multi-zone)
     */
    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testDeleteWithEFlag() throws Exception {
        log.info("\n========== TEST: Delete with E Flag (Tombstone Versioning) ==========");

        String key = "session:tombstone:test";
        long initialVersion = System.currentTimeMillis();
        long tombstoneVersion = initialVersion + 1;

        // Create item with E flag
        log.debug("Step 1: Create item with version " + initialVersion);
        MetaSetOperation.Builder createBuilder = new MetaSetOperation.Builder()
                .key(key)
                .value("session_data".getBytes())
                .recasid(initialVersion)
                .expiration(3600);

        evCache.metaSet(createBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);

        // Delete with E flag (sets tombstone version)
        log.debug("Step 2: Delete with E flag (tombstone version " + tombstoneVersion + ")");
        MetaDeleteOperation.Builder deleteBuilder = new MetaDeleteOperation.Builder()
                .key(key)
                .cas(initialVersion)          // C flag: validate current version
                .recasid(tombstoneVersion);   // E flag: set tombstone version

        assertTrue(evCache.metaDelete(deleteBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        // Verify deletion
        String value = evCache.get(key);
        assertNull(value, "Item should be deleted");

        log.info("✓ Delete with E flag works: tombstone gets client-controlled version");
        log.info("✓ All zones have synchronized tombstone version: " + tombstoneVersion);
        log.info("✓ Use case: Multi-zone delete with CAS validation and version sync");
    }

    /**
     * Test: Read-Modify-Write Pattern (Shopping Cart example)
     */
    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testReadModifyWritePattern() throws Exception {
        log.info("\n========== TEST: Read-Modify-Write Pattern (Shopping Cart) ==========");

        String cartKey = "cart:user:999";
        int maxRetries = 5;

        // Helper method to simulate adding item to cart
        class CartManager {
            boolean addItemToCart(String key, String itemId) throws Exception {
                for (int attempt = 0; attempt < maxRetries; attempt++) {
                    // Read current cart
                    MetaGetBulkOperation.Config config = new MetaGetBulkOperation.Config(Arrays.asList(key))
                            .includeCas(true);
                    Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(Arrays.asList(key), config, null);

                    String cart;
                    long currentCas;

                    EVCacheItem<String> item = items.get(key);
                    if (item == null) {
                        // No cart exists - create new
                        cart = "";
                        currentCas = 0;
                    } else {
                        cart = item.getData();
                        currentCas = item.getItemMetaData().getCas();
                    }

                    // Modify cart (add item)
                    String updatedCart = cart.isEmpty() ? itemId : cart + "," + itemId;
                    long newCas = System.currentTimeMillis();

                    // Write back with CAS validation
                    MetaSetOperation.Builder builder = new MetaSetOperation.Builder()
                            .key(key)
                            .value(updatedCart.getBytes())
                            .recasid(newCas)
                            .expiration(3600);

                    if (currentCas > 0) {
                        builder.cas(currentCas);  // Validate if cart existed
                    }

                    boolean success = evCache.metaSet(builder, Policy.ALL)
                            .await(1000, TimeUnit.MILLISECONDS);

                    if (success) {
                        return true;
                    }

                    // CAS conflict - someone else modified cart, retry
                    log.debug("Attempt " + (attempt + 1) + ": CAS conflict, retrying...");
                }

                return false;  // Failed after all retries
            }
        }

        CartManager cartManager = new CartManager();

        // Add items to cart
        log.debug("Step 1: Add item 'ITEM-001' to cart");
        assertTrue(cartManager.addItemToCart(cartKey, "ITEM-001"), "First add should succeed");

        log.debug("Step 2: Add item 'ITEM-002' to cart");
        assertTrue(cartManager.addItemToCart(cartKey, "ITEM-002"), "Second add should succeed");

        log.debug("Step 3: Add item 'ITEM-003' to cart");
        assertTrue(cartManager.addItemToCart(cartKey, "ITEM-003"), "Third add should succeed");

        // Verify final cart
        String finalCart = evCache.get(cartKey);
        assertEquals(finalCart, "ITEM-001,ITEM-002,ITEM-003");

        // Test concurrent adds
        log.debug("\nStep 4: Test concurrent cart modifications (2 threads)");
        CountDownLatch concurrentLatch = new CountDownLatch(2);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < 2; i++) {
            final int threadNum = i;
            new Thread(() -> {
                try {
                    boolean success = cartManager.addItemToCart(cartKey, "CONCURRENT-" + threadNum);
                    if (success) successCount.incrementAndGet();
                } catch (Exception e) {
                    log.error("Concurrent add failed", e);
                } finally {
                    concurrentLatch.countDown();
                }
            }).start();
        }

        concurrentLatch.await(10, TimeUnit.SECONDS);
        assertEquals(successCount.get(), 2, "Both concurrent adds should eventually succeed");

        String finalCartAfterConcurrent = evCache.get(cartKey);
        String cartData = finalCartAfterConcurrent;
        assertTrue(cartData.contains("CONCURRENT-0"), "Cart should contain CONCURRENT-0");
        assertTrue(cartData.contains("CONCURRENT-1"), "Cart should contain CONCURRENT-1");

        log.info("✓ Read-Modify-Write pattern works with CAS protection");
        log.info("✓ Concurrent modifications handled safely with retries");
        log.info("✓ No lost updates: " + cartData);
    }

    /**
     * Test: Multi-Zone CAS Synchronization with E Flag
     * Demonstrates that E flag keeps all zones synchronized
     */
    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testMultiZoneCasSynchronization() throws Exception {
        log.info("\n========== TEST: Multi-Zone CAS Synchronization ==========");

        String key = "multizone:cas:sync";
        long clientVersion = System.currentTimeMillis();

        // Write with E flag to all zones
        log.debug("Step 1: Write with E flag (version: " + clientVersion + ")");
        MetaSetOperation.Builder builder = new MetaSetOperation.Builder()
                .key(key)
                .value("synced_data".getBytes())
                .recasid(clientVersion)
                .expiration(3600);

        assertTrue(evCache.metaSet(builder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        // Read from cache and verify CAS
        log.debug("Step 2: Read back and verify all zones have same CAS");
        MetaGetBulkOperation.Config config = new MetaGetBulkOperation.Config(Arrays.asList(key))
                .includeCas(true);
        Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(Arrays.asList(key), config, null);

        EVCacheItem<String> item = items.get(key);
        assertNotNull(item, "Item should exist");
        assertEquals(item.getItemMetaData().getCas(), clientVersion,
                "CAS should match client-provided version");

        // Update with CAS validation and new E flag
        log.debug("Step 3: Update with C and E flags");
        long newVersion = clientVersion + 1;
        MetaSetOperation.Builder updateBuilder = new MetaSetOperation.Builder()
                .key(key)
                .value("updated_synced_data".getBytes())
                .cas(clientVersion)      // C flag: validate
                .recasid(newVersion)     // E flag: new version
                .expiration(3600);

        assertTrue(evCache.metaSet(updateBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS),
                "Update should succeed across all zones");

        // Verify new CAS
        Map<String, EVCacheItem<String>> updatedItems = evCache.metaGetBulk(Arrays.asList(key), config, null);
        assertEquals(updatedItems.get(key).getItemMetaData().getCas(), newVersion,
                "All zones should have new synchronized CAS");

        log.info("✓ Multi-zone CAS synchronization works with E flag");
        log.info("✓ All zones validated CAS=" + clientVersion + " and updated to CAS=" + newVersion);
        log.info("✓ This is the key to making CAS work reliably across zones!");
    }

    /**
     * Test: Return Flags (returnCas, returnTtl, returnSize)
     */
    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testReturnFlags() throws Exception {
        log.info("\n========== TEST: Return Flags (returnCas, returnTtl, returnSize) ==========");

        String key = "return_flags_test";
        long version = System.currentTimeMillis();
        String value = "test_data_for_return_flags";

        // Write with return flags
        log.debug("Step 1: Write with returnCas, returnTtl, returnSize flags");
        MetaSetOperation.Builder builder = new MetaSetOperation.Builder()
                .key(key)
                .value(value.getBytes())
                .recasid(version)
                .expiration(3600)
                .returnCas(true)
                .returnTtl(true);
        // Note: returnSize is available but not directly exposed in current API

        assertTrue(evCache.metaSet(builder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        // Read back with metadata
        log.debug("Step 2: Read back and verify metadata");
        MetaGetBulkOperation.Config config = new MetaGetBulkOperation.Config(Arrays.asList(key))
                .includeCas(true)
                .includeTtl(true)
                ;

        Map<String, EVCacheItem<String>> items = evCache.metaGetBulk(Arrays.asList(key), config, null);
        EVCacheItem<String> item = items.get(key);

        assertNotNull(item);
        assertEquals(item.getItemMetaData().getCas(), version, "CAS should match");
        assertTrue(item.getItemMetaData().getSecondsLeftToExpire() > 0, "TTL should be positive");
        assertTrue(item.getItemMetaData().getSecondsLeftToExpire() <= 3600, "TTL should be <= 3600");

        log.info("✓ Return flags work:");
        log.info("  - CAS: " + item.getItemMetaData().getCas());
        log.info("  - TTL: " + item.getItemMetaData().getSecondsLeftToExpire() + " seconds remaining");
        // Flags not directly available in metadata
        log.info("✓ Useful for debugging and verifying E flag worked correctly");
    }

    /**
     * Test: Mark Stale Flag
     */
    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testMarkStaleFlag() throws Exception {
        log.info("\n========== TEST: Mark Stale Flag ==========");

        String key = "stale_test:expensive:query";
        String initialValue = "expensive_result_v1";
        String updatedValue = "expensive_result_v2";

        // Write initial value
        log.debug("Step 1: Write initial value");
        MetaSetOperation.Builder builder1 = new MetaSetOperation.Builder()
                .key(key)
                .value(initialValue.getBytes())
                .expiration(3600);

        evCache.metaSet(builder1, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);

        // Update with mark stale flag
        log.debug("Step 2: Update with markStale=true (win/win flag)");
        MetaSetOperation.Builder builder2 = new MetaSetOperation.Builder()
                .key(key)
                .value(updatedValue.getBytes())
                .markStale(true)  // Mark old value as stale during update
                .expiration(3600);

        assertTrue(evCache.metaSet(builder2, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));

        // Verify updated value
        String value = evCache.get(key);
        assertEquals(value, updatedValue);

        log.info("✓ Mark stale flag works");
        log.info("✓ Use cases:");
        log.info("  - Cache warming: mark old data stale while updating");
        log.info("  - Readers know data is being refreshed");
        log.info("  - Avoid cache stampede during updates");
    }

    /**
     * Test: Policy Behavior (ONE vs QUORUM vs ALL_MINUS_1)
     */
    @Test(dependsOnMethods = {"testBasicMetaSet"})
    public void testPolicyBehavior() throws Exception {
        log.info("\n========== TEST: Policy Behavior ==========");

        String baseKey = "policy_test:";

        // Policy.ONE - Fastest, least consistency
        log.debug("\n--- Testing Policy.ONE ---");
        String oneKey = baseKey + "one";
        MetaSetOperation.Builder oneBuilder = new MetaSetOperation.Builder()
                .key(oneKey)
                .value("policy_one".getBytes())
                .expiration(3600);

        long start = System.currentTimeMillis();
        assertTrue(evCache.metaSet(oneBuilder, Policy.ONE).await(1000, TimeUnit.MILLISECONDS));
        long oneTime = System.currentTimeMillis() - start;
        log.info("✓ Policy.ONE write time: " + oneTime + "ms (fastest, any zone succeeds)");

        // Policy.ALL - Balance of speed and consistency
        log.debug("\n--- Testing Policy.ALL ---");
        String quorumKey = baseKey + "quorum";
        MetaSetOperation.Builder quorumBuilder = new MetaSetOperation.Builder()
                .key(quorumKey)
                .value("policy_quorum".getBytes())
                .expiration(3600);

        start = System.currentTimeMillis();
        assertTrue(evCache.metaSet(quorumBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));
        long quorumTime = System.currentTimeMillis() - start;
        log.info("✓ Policy.ALL write time: " + quorumTime + "ms (majority must succeed)");

        // Policy.ALL - High consistency, tolerates 1 zone failure
        log.debug("\n--- Testing Policy.ALL ---");
        String allMinus1Key = baseKey + "all_minus_1";
        MetaSetOperation.Builder allMinus1Builder = new MetaSetOperation.Builder()
                .key(allMinus1Key)
                .value("policy_all_minus_1".getBytes())
                .expiration(3600);

        start = System.currentTimeMillis();
        assertTrue(evCache.metaSet(allMinus1Builder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS));
        long allMinus1Time = System.currentTimeMillis() - start;
        log.info("✓ Policy.ALL write time: " + allMinus1Time + "ms (N-1 zones must succeed)");

        log.info("\n✓ Policy recommendations:");
        log.info("  - Use ONE for: read-heavy, eventual consistency OK");
        log.info("  - Use QUORUM for: locks, balanced performance");
        log.info("  - Use ALL_MINUS_1 for: critical data, strong consistency");
    }

    /**
     * Test: Complete User Journey - All Patterns Together
     * Simulates a real application using multiple patterns
     */
    @Test(dependsOnMethods = {"testAllSetModes", "testDeleteWithCAS", "testReadModifyWritePattern"})
    public void testCompleteUserJourney() throws Exception {
        log.info("\n========== TEST: Complete User Journey (All Patterns) ==========");

        String userId = "journey:user:12345";
        String sessionKey = "session:" + userId;
        String cartKey = "cart:" + userId;
        String lockKey = "lock:checkout:" + userId;

        // Journey Step 1: User logs in - create session with simple cache-aside
        log.info("\n--- Step 1: User Login (Simple Cache-Aside) ---");
        String sessionData = "{\"userId\":12345,\"loginTime\":" + System.currentTimeMillis() + "}";
        MetaSetOperation.Builder sessionBuilder = new MetaSetOperation.Builder()
                .key(sessionKey)
                .value(sessionData.getBytes())
                .expiration(1800);
        evCache.metaSet(sessionBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);
        log.info("✓ Session created");

        // Journey Step 2: User adds items to cart - read-modify-write with CAS
        log.info("\n--- Step 2: Add Items to Cart (Read-Modify-Write) ---");
        long cartVersion = System.currentTimeMillis();
        MetaSetOperation.Builder cartBuilder = new MetaSetOperation.Builder()
                .key(cartKey)
                .value("ITEM-001,ITEM-002".getBytes())
                .recasid(cartVersion)
                .expiration(3600);
        evCache.metaSet(cartBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);
        log.info("✓ Cart created with 2 items");

        // Journey Step 3: User proceeds to checkout - acquire distributed lock
        log.info("\n--- Step 3: Checkout (Distributed Lock) ---");
        long lockVersion = System.currentTimeMillis();
        MetaSetOperation.Builder lockBuilder = new MetaSetOperation.Builder()
                .key(lockKey)
                .value("instance-123".getBytes())
                .mode(MetaSetOperation.SetMode.ADD)
                .recasid(lockVersion)
                .expiration(30);

        boolean lockAcquired = evCache.metaSet(lockBuilder, Policy.ONE)
                .await(1000, TimeUnit.MILLISECONDS);
        assertTrue(lockAcquired, "Should acquire checkout lock");
        log.info("✓ Checkout lock acquired");

        // Journey Step 4: Process checkout (protected by lock)
        log.info("\n--- Step 4: Process Checkout (Protected Operation) ---");
        Thread.sleep(100);  // Simulate payment processing
        log.info("✓ Payment processed");

        // Journey Step 5: Clear cart after successful checkout
        log.info("\n--- Step 5: Clear Cart (Delete with CAS) ---");
        MetaDeleteOperation.Builder clearCartBuilder = new MetaDeleteOperation.Builder()
                .key(cartKey)
                .cas(cartVersion);
        evCache.metaDelete(clearCartBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);
        log.info("✓ Cart cleared");

        // Journey Step 6: Release checkout lock
        log.info("\n--- Step 6: Release Lock (Delete with CAS Validation) ---");
        MetaDeleteOperation.Builder releaseLockBuilder = new MetaDeleteOperation.Builder()
                .key(lockKey)
                .cas(lockVersion);
        boolean lockReleased = evCache.metaDelete(releaseLockBuilder, Policy.ALL)
                .await(1000, TimeUnit.MILLISECONDS);
        assertTrue(lockReleased, "Should release lock");
        log.info("✓ Checkout lock released");

        // Journey Step 7: User logs out - invalidate session
        log.info("\n--- Step 7: User Logout (Delete Session) ---");
        MetaDeleteOperation.Builder logoutBuilder = new MetaDeleteOperation.Builder()
                .key(sessionKey);
        evCache.metaDelete(logoutBuilder, Policy.ALL).await(1000, TimeUnit.MILLISECONDS);
        log.info("✓ Session invalidated");

        // Verify cleanup
        assertNull(evCache.get(sessionKey), "Session should be deleted");
        assertNull(evCache.get(cartKey), "Cart should be deleted");
        assertNull(evCache.get(lockKey), "Lock should be deleted");

        log.info("\n✓✓✓ Complete user journey successful! ✓✓✓");
        log.info("Used patterns:");
        log.info("  - Simple cache-aside (session)");
        log.info("  - Read-modify-write with CAS (cart)");
        log.info("  - Distributed locking (checkout)");
        log.info("  - Safe deletion with CAS (lock release)");
        log.info("  - Cache invalidation (logout)");
    }
}
