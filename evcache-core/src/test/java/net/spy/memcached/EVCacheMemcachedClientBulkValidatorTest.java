package net.spy.memcached;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.operation.EVCacheAsciiOperationFactory;
import com.netflix.evcache.operation.EVCacheBulkGetFuture;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPool;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.Tag;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.ascii.EVCacheAsciiNodeImpl;
import net.spy.memcached.transcoders.SerializingTranscoder;

/**
 * Guards a contract the skipped-key fallback in {@code EVCacheImpl} relies on: {@code asyncGetBulk} runs the node
 * validator for every key, on the calling thread, before it returns. The fallback reads the set of skipped keys right
 * after {@code EVCacheClient.getAsyncBulk} returns, so if validation ever became lazy or moved to another thread the
 * fallback would quietly do nothing. This test fails if that happens.
 *
 * <p>Uses a real {@link EVCacheMemcachedClient} against a socket that accepts connections and never answers, so
 * nothing a request would do after being sent can complete in time to hide an ordering problem.
 */
public class EVCacheMemcachedClientBulkValidatorTest {

    private final DefaultSettableConfig config = new DefaultSettableConfig();
    private final DefaultRegistry registry = new DefaultRegistry();

    private String appName;
    private SilentServer server;
    private EVCacheMemcachedClient memcachedClient;

    @BeforeClass
    public void setUpConfig() {
        new EVCacheConfig(new DefaultPropertyFactory(config));
        Spectator.globalRegistry().add(registry);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownConfig() {
        Spectator.globalRegistry().remove(registry);
    }

    @BeforeMethod
    public void setUp() throws Exception {
        appName = "BULKVALIDATORTEST" + System.nanoTime();
        server = new SilentServer();

        final EVCacheClient client = mock(EVCacheClient.class);
        final EVCacheClientPool pool = mock(EVCacheClientPool.class);
        final EVCacheClientPoolManager poolManager = mock(EVCacheClientPoolManager.class);
        final List<Tag> tags = new ArrayList<Tag>();
        tags.add(new BasicTag(EVCacheMetricsFactory.CACHE, appName));
        when(client.getAppName()).thenReturn(appName);
        when(client.getTagList()).thenReturn(tags);
        when(client.getServerGroupName()).thenReturn("sg");
        when(client.getId()).thenReturn(1);
        when(client.getOperationCounter()).thenReturn(mock(Counter.class));
        when(client.getPool()).thenReturn(pool);
        when(pool.getEVCacheClientPoolManager()).thenReturn(poolManager);

        final long startTime = System.currentTimeMillis();
        final ConnectionFactory factory = new DefaultConnectionFactory() {
            private final OperationFactory operationFactory = new EVCacheAsciiOperationFactory();

            @Override
            public MemcachedConnection createConnection(List<InetSocketAddress> addrs) throws IOException {
                return new EVCacheConnection("bulk-validator-test", getReadBufSize(), this, addrs, getInitialObservers(),
                        getFailureMode(), getOperationFactory());
            }

            @Override
            public OperationFactory getOperationFactory() {
                return operationFactory;
            }

            @Override
            public MemcachedNode createMemcachedNode(SocketAddress sa, SocketChannel c, int bufSize) {
                return new EVCacheAsciiNodeImpl(sa, c, bufSize, createReadOperationQueue(), createWriteOperationQueue(),
                        new ArrayBlockingQueue<Operation>(getOpQueueLen()), getOpQueueMaxBlockTime(),
                        false, getOperationTimeout(), getAuthWaitTime(), this, client, startTime);
            }

            @Override
            public boolean isDaemon() {
                return true;
            }
        };
        memcachedClient = new EVCacheMemcachedClient(factory, Collections.singletonList(server.address()), null, client);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (memcachedClient != null) memcachedClient.shutdown(1, TimeUnit.SECONDS);
        if (server != null) server.close();
    }

    @Test
    public void everyKeyIsValidatedOnTheCallingThreadBeforeTheCallReturns() {
        final List<String> keys = Arrays.asList("key-a", "key-b", "key-c", "key-d");
        final List<String> validated = new CopyOnWriteArrayList<String>();
        final List<Thread> threads = new CopyOnWriteArrayList<Thread>();
        final AtomicBoolean returned = new AtomicBoolean();
        final List<Boolean> validatedBeforeReturn = new CopyOnWriteArrayList<Boolean>();
        final BiPredicate<MemcachedNode, String> validator = (node, key) -> {
            validated.add(key);
            threads.add(Thread.currentThread());
            validatedBeforeReturn.add(!returned.get());
            return true;
        };

        final EVCacheBulkGetFuture<Object> future = memcachedClient.asyncGetBulk(keys, null,
                new SerializingTranscoder(), null, validator, appName, false, null);
        returned.set(true);

        assertEquals(validated.size(), keys.size(), "every key must have been validated by the time the call returned");
        assertTrue(validated.containsAll(keys));
        for (Thread t : threads) {
            assertTrue(t == Thread.currentThread(), "validation must run on the calling thread, not " + t.getName());
        }
        assertFalse(validatedBeforeReturn.contains(Boolean.FALSE));
        assertFalse(future.isDone(), "the silent server never answers, so the request is still outstanding");
    }

    @Test
    public void keysTheValidatorRejectsAreNotSentAndAnAllRejectedCallCompletesAtOnce() throws Exception {
        final List<String> keys = Arrays.asList("key-a", "key-b");
        final List<String> rejected = new CopyOnWriteArrayList<String>();
        final BiPredicate<MemcachedNode, String> rejectAll = (node, key) -> {
            rejected.add(key);
            return false;
        };

        final EVCacheBulkGetFuture<Object> future = memcachedClient.asyncGetBulk(keys, null,
                new SerializingTranscoder(), null, rejectAll, appName, false, null);

        assertEquals(rejected.size(), keys.size());
        // nothing is sent, so there is nothing to wait for: the result is complete and empty right away
        assertTrue(future.isDone());
        final Map<String, Object> result = future.get(1, TimeUnit.SECONDS);
        assertTrue(result.isEmpty());
    }

    /** Accepts connections and never answers. */
    private static class SilentServer implements Closeable {
        private final ServerSocket serverSocket;
        private final List<Socket> sockets = new CopyOnWriteArrayList<Socket>();

        SilentServer() throws IOException {
            serverSocket = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
            final Thread acceptor = new Thread(() -> {
                try {
                    while (!serverSocket.isClosed()) sockets.add(serverSocket.accept());
                } catch (IOException e) {
                    // closed
                }
            }, "silent-memcached-accept");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        InetSocketAddress address() {
            return new InetSocketAddress("127.0.0.1", serverSocket.getLocalPort());
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
            for (Socket s : sockets) s.close();
        }
    }
}
