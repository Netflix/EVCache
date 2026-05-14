package com.netflix.evcache.operation;

import com.netflix.evcache.pool.EVCacheClient;
import net.spy.memcached.ops.Operation;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class EVCacheOperationFutureLatencyTest {

    @Test
    public void recordsLatencyOnSignalCompleteForSingleOp() {
        EVCacheClient client = mock(EVCacheClient.class);
        Operation op = mock(Operation.class);
        // pretend the write completed shortly after setOperation()
        when(op.getWriteCompleteTimestamp()).thenReturn(System.nanoTime() + 1_000_000L);

        EVCacheOperationFuture<Boolean> future = new EVCacheOperationFuture<Boolean>(
                "key", new CountDownLatch(1), new AtomicReference<Boolean>(null), 1000L, null, client);

        future.setOperation(op);
        future.signalComplete();

        ArgumentCaptor<Long> startCaptor = ArgumentCaptor.forClass(Long.class);
        verify(client).recordLoopEnqueueToWriteLatency(org.mockito.Matchers.eq(op), startCaptor.capture());
        assertTrue(startCaptor.getValue() > 0L,
                "expected operationAttachedNs to be captured > 0, got " + startCaptor.getValue());
    }

    @Test
    public void recordsLatencyWithLatestAttachedTimestampOnRetry() throws Exception {
        EVCacheClient client = mock(EVCacheClient.class);
        Operation firstOp = mock(Operation.class);
        Operation retryOp = mock(Operation.class);

        EVCacheOperationFuture<Boolean> future = new EVCacheOperationFuture<Boolean>(
                "key", new CountDownLatch(1), new AtomicReference<Boolean>(null), 1000L, null, client);

        future.setOperation(firstOp);
        long firstStart = System.nanoTime();
        Thread.sleep(2);
        future.setOperation(retryOp);
        future.signalComplete();

        ArgumentCaptor<Long> startCaptor = ArgumentCaptor.forClass(Long.class);
        verify(client).recordLoopEnqueueToWriteLatency(org.mockito.Matchers.eq(retryOp), startCaptor.capture());
        assertTrue(startCaptor.getValue() >= firstStart,
                "expected retry timestamp to be >= first attach time");
    }
}
