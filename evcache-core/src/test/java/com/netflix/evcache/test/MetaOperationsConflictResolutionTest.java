package com.netflix.evcache.test;

import static org.testng.Assert.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import net.spy.memcached.protocol.ascii.MetaSetOperation;
import net.spy.memcached.protocol.ascii.MetaSetOperationImpl;
import net.spy.memcached.protocol.ascii.MetaDeleteOperation;
import net.spy.memcached.protocol.ascii.MetaDeleteOperationImpl;
import net.spy.memcached.ops.OperationCallback;

/**
 * Tests for conflict resolution using CAS (Compare-and-Swap) mechanisms
 * in meta protocol operations.
 */
public class MetaOperationsConflictResolutionTest {

    @Mock
    private OperationCallback mockCallback;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCASBasedSet_Success() throws InterruptedException {
        // Test successful CAS-based set operation
        AtomicBoolean setComplete = new AtomicBoolean(false);
        AtomicLong returnedCas = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(1);

        MetaSetOperation.Callback callback = new MetaSetOperation.Callback() {
            @Override
            public void setComplete(String key, long cas, boolean stored) {
                setComplete.set(stored);
                returnedCas.set(cas);
                latch.countDown();
            }

            @Override
            public void gotMetaData(String key, char flag, String data) {
                if (flag == 'c') {
                    returnedCas.set(Long.parseLong(data));
                }
            }

            @Override
            public void receivedStatus(net.spy.memcached.ops.OperationStatus status) {
                // Mock implementation
            }

            @Override
            public void complete() {
                // Mock implementation
            }
        };

        MetaSetOperation.Builder builder = new MetaSetOperation.Builder()
            .key("test-cas-key")
            .value("test-value".getBytes())
            .cas(12345L)  // Specify expected CAS value
            .returnCas(true);

        MetaSetOperationImpl operation = new MetaSetOperationImpl(builder, callback);

        // Initialize the operation to generate command
        operation.initialize();

        // Simulate successful response with new CAS
        operation.handleLine("HD c67890");

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertTrue(setComplete.get(), "CAS-based set should succeed");
        assertEquals(returnedCas.get(), 67890L, "Should return new CAS value");
    }

    @Test
    public void testCASBasedSet_Conflict() throws InterruptedException {
        // Test CAS conflict (item was modified by another client)
        AtomicBoolean setComplete = new AtomicBoolean(true);
        CountDownLatch latch = new CountDownLatch(1);

        MetaSetOperation.Callback callback = new MetaSetOperation.Callback() {
            @Override
            public void setComplete(String key, long cas, boolean stored) {
                setComplete.set(stored);
                latch.countDown();
            }

            @Override
            public void gotMetaData(String key, char flag, String data) {
                // No metadata expected on conflict
            }

            @Override
            public void receivedStatus(net.spy.memcached.ops.OperationStatus status) {}

            @Override
            public void complete() {}
        };

        MetaSetOperation.Builder builder = new MetaSetOperation.Builder()
            .key("test-cas-conflict")
            .value("test-value".getBytes())
            .cas(12345L)  // This CAS will not match
            .returnCas(true);

        MetaSetOperationImpl operation = new MetaSetOperationImpl(builder, callback);
        operation.initialize();

        // Simulate CAS mismatch response
        operation.handleLine("EX");

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(setComplete.get(), "CAS conflict should prevent set");
    }

    @Test
    public void testConditionalSet_AddOnlyIfNotExists() throws InterruptedException {
        // Test ADD operation - only succeeds if key doesn't exist
        AtomicBoolean addComplete = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        MetaSetOperation.Callback callback = new MetaSetOperation.Callback() {
            @Override
            public void setComplete(String key, long cas, boolean stored) {
                addComplete.set(stored);
                latch.countDown();
            }

            @Override
            public void gotMetaData(String key, char flag, String data) {}

            @Override
            public void receivedStatus(net.spy.memcached.ops.OperationStatus status) {}

            @Override
            public void complete() {}
        };

        MetaSetOperation.Builder builder = new MetaSetOperation.Builder()
            .key("test-add-key")
            .value("new-value".getBytes())
            .mode(MetaSetOperation.SetMode.ADD)
            .returnCas(true);

        MetaSetOperationImpl operation = new MetaSetOperationImpl(builder, callback);
        operation.initialize();

        // Verify the command includes ADD flag (N)
        ByteBuffer buffer = operation.getBuffer();
        String command = new String(buffer.array(), 0, buffer.limit());
        assertTrue(command.contains(" N "), "Should include ADD mode flag");

        // Simulate successful add
        operation.handleLine("HD c54321");

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertTrue(addComplete.get(), "ADD should succeed when key doesn't exist");
    }

    @Test
    public void testConditionalSet_AddFailsIfExists() throws InterruptedException {
        // Test ADD operation fails if key already exists
        AtomicBoolean addComplete = new AtomicBoolean(true);
        CountDownLatch latch = new CountDownLatch(1);

        MetaSetOperation.Callback callback = new MetaSetOperation.Callback() {
            @Override
            public void setComplete(String key, long cas, boolean stored) {
                addComplete.set(stored);
                latch.countDown();
            }

            @Override
            public void gotMetaData(String key, char flag, String data) {}

            @Override
            public void receivedStatus(net.spy.memcached.ops.OperationStatus status) {}

            @Override
            public void complete() {}
        };

        MetaSetOperation.Builder builder = new MetaSetOperation.Builder()
            .key("existing-key")
            .value("new-value".getBytes())
            .mode(MetaSetOperation.SetMode.ADD);

        MetaSetOperationImpl operation = new MetaSetOperationImpl(builder, callback);
        operation.initialize();

        // Simulate ADD failure - key exists
        operation.handleLine("NS");

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(addComplete.get(), "ADD should fail when key exists");
    }

    @Test
    public void testConditionalSet_ReplaceOnlyIfExists() throws InterruptedException {
        // Test REPLACE operation - only succeeds if key exists
        AtomicBoolean replaceComplete = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        MetaSetOperation.Callback callback = new MetaSetOperation.Callback() {
            @Override
            public void setComplete(String key, long cas, boolean stored) {
                replaceComplete.set(stored);
                latch.countDown();
            }

            @Override
            public void gotMetaData(String key, char flag, String data) {}

            @Override
            public void receivedStatus(net.spy.memcached.ops.OperationStatus status) {}

            @Override
            public void complete() {}
        };

        MetaSetOperation.Builder builder = new MetaSetOperation.Builder()
            .key("existing-key")
            .value("updated-value".getBytes())
            .mode(MetaSetOperation.SetMode.REPLACE)
            .returnCas(true);

        MetaSetOperationImpl operation = new MetaSetOperationImpl(builder, callback);
        operation.initialize();

        // Verify the command includes REPLACE flag (R)
        ByteBuffer buffer = operation.getBuffer();
        String command = new String(buffer.array(), 0, buffer.limit());
        assertTrue(command.contains(" R "), "Should include REPLACE mode flag");

        // Simulate successful replace
        operation.handleLine("HD c98765");

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertTrue(replaceComplete.get(), "REPLACE should succeed when key exists");
    }

    @Test
    public void testCASBasedDelete_Success() throws InterruptedException {
        // Test successful CAS-based delete operation
        AtomicBoolean deleteComplete = new AtomicBoolean(false);
        AtomicLong returnedCas = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(1);

        MetaDeleteOperation.Callback callback = new MetaDeleteOperation.Callback() {
            @Override
            public void deleteComplete(String key, boolean deleted) {
                deleteComplete.set(deleted);
                latch.countDown();
            }

            @Override
            public void gotMetaData(String key, char flag, String data) {
                if (flag == 'c') {
                    returnedCas.set(Long.parseLong(data));
                }
            }

            @Override
            public void receivedStatus(net.spy.memcached.ops.OperationStatus status) {}

            @Override
            public void complete() {}
        };

        MetaDeleteOperation.Builder builder = new MetaDeleteOperation.Builder()
            .key("test-cas-delete")
            .cas(12345L)  // Expected CAS value
            .returnCas(true);

        MetaDeleteOperationImpl operation = new MetaDeleteOperationImpl(builder, callback);
        operation.initialize();

        // Verify the command includes CAS flag
        ByteBuffer buffer = operation.getBuffer();
        String command = new String(buffer.array(), 0, buffer.limit());
        assertTrue(command.contains("C12345"), "Should include CAS value");

        // Simulate successful delete
        operation.handleLine("HD c12345");

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertTrue(deleteComplete.get(), "CAS-based delete should succeed");
        assertEquals(returnedCas.get(), 12345L, "Should return CAS value");
    }

    @Test
    public void testCASBasedDelete_Conflict() throws InterruptedException {
        // Test CAS conflict on delete (item was modified)
        AtomicBoolean deleteComplete = new AtomicBoolean(true);
        CountDownLatch latch = new CountDownLatch(1);

        MetaDeleteOperation.Callback callback = new MetaDeleteOperation.Callback() {
            @Override
            public void deleteComplete(String key, boolean deleted) {
                deleteComplete.set(deleted);
                latch.countDown();
            }

            @Override
            public void gotMetaData(String key, char flag, String data) {}

            @Override
            public void receivedStatus(net.spy.memcached.ops.OperationStatus status) {}

            @Override
            public void complete() {}
        };

        MetaDeleteOperation.Builder builder = new MetaDeleteOperation.Builder()
            .key("test-cas-delete-conflict")
            .cas(12345L);  // This CAS won't match

        MetaDeleteOperationImpl operation = new MetaDeleteOperationImpl(builder, callback);
        operation.initialize();

        // Simulate CAS mismatch on delete
        operation.handleLine("EX");

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(deleteComplete.get(), "CAS conflict should prevent delete");
    }

    @Test
    public void testRaceConditionPrevention() throws InterruptedException {
        // Test that CAS prevents race conditions in concurrent updates
        
        // This test simulates two clients trying to update the same key
        // Client 1 gets CAS value, Client 2 updates first, Client 1's update fails
        
        AtomicReference<String> firstResult = new AtomicReference<>();
        AtomicReference<String> secondResult = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(2);

        // First client's operation (will succeed)
        MetaSetOperation.Callback callback1 = new MetaSetOperation.Callback() {
            @Override
            public void setComplete(String key, long cas, boolean stored) {
                firstResult.set(stored ? "SUCCESS" : "FAILED");
                latch.countDown();
            }

            @Override
            public void gotMetaData(String key, char flag, String data) {}

            @Override
            public void receivedStatus(net.spy.memcached.ops.OperationStatus status) {}

            @Override
            public void complete() {}
        };

        // Second client's operation (will fail due to CAS mismatch)
        MetaSetOperation.Callback callback2 = new MetaSetOperation.Callback() {
            @Override
            public void setComplete(String key, long cas, boolean stored) {
                secondResult.set(stored ? "SUCCESS" : "FAILED");
                latch.countDown();
            }

            @Override
            public void gotMetaData(String key, char flag, String data) {}

            @Override
            public void receivedStatus(net.spy.memcached.ops.OperationStatus status) {}

            @Override
            public void complete() {}
        };

        // Both clients got CAS value 12345 before either updated
        MetaSetOperation.Builder builder1 = new MetaSetOperation.Builder()
            .key("race-condition-key")
            .value("client1-value".getBytes())
            .cas(12345L);

        MetaSetOperation.Builder builder2 = new MetaSetOperation.Builder()
            .key("race-condition-key")
            .value("client2-value".getBytes())
            .cas(12345L);  // Same CAS value

        MetaSetOperationImpl operation1 = new MetaSetOperationImpl(builder1, callback1);
        MetaSetOperationImpl operation2 = new MetaSetOperationImpl(builder2, callback2);

        operation1.initialize();
        operation2.initialize();

        // Client 1 succeeds (first to update)
        operation1.handleLine("HD c67890");

        // Client 2 fails (CAS mismatch because client 1 already updated)
        operation2.handleLine("EX");

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(firstResult.get(), "SUCCESS", "First client should succeed");
        assertEquals(secondResult.get(), "FAILED", "Second client should fail due to CAS mismatch");
    }

    @Test
    public void testCommandGeneration_CASFlags() {
        // Test that CAS values are correctly included in commands
        
        MetaSetOperation.Builder setBuilder = new MetaSetOperation.Builder()
            .key("test-key")
            .value("test-value".getBytes())
            .cas(123456789L)
            .returnCas(true);

        MetaSetOperationImpl setOp = new MetaSetOperationImpl(setBuilder, mock(MetaSetOperation.Callback.class));
        setOp.initialize();

        ByteBuffer setBuffer = setOp.getBuffer();
        String setCommand = new String(setBuffer.array(), 0, setBuffer.limit());
        
        assertTrue(setCommand.startsWith("ms test-key"), "Should start with meta set command");
        assertTrue(setCommand.contains("C123456789"), "Should include CAS value");
        assertTrue(setCommand.contains(" c"), "Should request CAS return");

        MetaDeleteOperation.Builder deleteBuilder = new MetaDeleteOperation.Builder()
            .key("test-key")
            .cas(987654321L)
            .returnCas(true);

        MetaDeleteOperationImpl deleteOp = new MetaDeleteOperationImpl(deleteBuilder, mock(MetaDeleteOperation.Callback.class));
        deleteOp.initialize();

        ByteBuffer deleteBuffer = deleteOp.getBuffer();
        String deleteCommand = new String(deleteBuffer.array(), 0, deleteBuffer.limit());
        
        assertTrue(deleteCommand.startsWith("md test-key"), "Should start with meta delete command");
        assertTrue(deleteCommand.contains("C987654321"), "Should include CAS value");
        assertTrue(deleteCommand.contains(" c"), "Should request CAS return");
    }
}