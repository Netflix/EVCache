package com.netflix.evcache.operation;

import com.netflix.evcache.pool.EVCacheClient;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class EVCacheBulkGetFutureLatencyTest {

    @Test
    public void recordsLatencyAgainstSharedBulkTimestampForEachChunk() {
        EVCacheClient client = mock(EVCacheClient.class);
        long bulkAttachedNs = System.nanoTime();

        Map<String, Future<Object>> rvMap = new HashMap<String, Future<Object>>();
        Collection<Operation> ops = new ArrayList<Operation>();

        EVCacheBulkGetFuture<Object> future = new EVCacheBulkGetFuture<Object>(
                rvMap, ops, new CountDownLatch(1), null, client, bulkAttachedNs);
        future.setExpectedCount(2);

        GetOperation chunkOne = mock(GetOperation.class);
        GetOperation chunkTwo = mock(GetOperation.class);

        future.signalSingleOpComplete(0, chunkOne);
        future.signalSingleOpComplete(1, chunkTwo);

        verify(client).recordLoopEnqueueToWriteLatency(chunkOne, bulkAttachedNs);
        verify(client).recordLoopEnqueueToWriteLatency(chunkTwo, bulkAttachedNs);
    }
}
