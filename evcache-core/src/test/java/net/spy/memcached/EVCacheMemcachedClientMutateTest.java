package net.spy.memcached;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.operation.EVCacheAsciiOperationFactory;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPool;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;

import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.MutatorOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.protocol.ascii.EVCacheAsciiNodeImpl;

/**
 * Runs a real {@link EVCacheMemcachedClient} (ascii protocol) against a fake memcached on 127.0.0.1 that
 * stays silent until told to answer.
 */
public class EVCacheMemcachedClientMutateTest {

    private static final long MUTATE_TIMEOUT_MS = 400;
    private static final String KEY = "mutate-test-key";
    private static final String NOT_FOUND = "NF\r\n";

    private final DefaultSettableConfig config = new DefaultSettableConfig();
    private final DefaultRegistry registry = new DefaultRegistry();

    private String appName;
    private TestConnectionFactory connectionFactory;

    private FakeMemcached server;
    private EVCacheMemcachedClient memcachedClient;
    private EVCacheNode node;

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
        // a new app name per test keeps the (static, cached) timers of different tests apart
        appName = "MUTATETEST" + System.nanoTime();
        config.setProperty(appName + ".mutate.timeout", MUTATE_TIMEOUT_MS);
        server = new FakeMemcached(0);
        startClient(server.address(), true);
    }

    private void startClient(InetSocketAddress address, boolean awaitConnected) throws Exception {
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

        final List<InetSocketAddress> addrs = Collections.singletonList(address);
        connectionFactory = new TestConnectionFactory(client);
        memcachedClient = new EVCacheMemcachedClient(connectionFactory, addrs, null, client);
        node = (EVCacheNode) memcachedClient.getEVCacheNode(KEY);
        // the node must be connected so that the op is actually written and the server sees it
        if (awaitConnected) awaitTrue("node connected", () -> node.isActive());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (memcachedClient != null) memcachedClient.shutdown(1, TimeUnit.SECONDS);
        if (server != null) server.close();
    }

    @Test
    public void silentServerTimesOutAndRecordsTimeoutTimer() throws Exception {
        final long start = System.nanoTime();
        final long result = memcachedClient.mutate(Mutator.incr, KEY, 1, 0, 0);
        final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertEquals(result, -1);
        assertTrue(elapsedMs >= MUTATE_TIMEOUT_MS - 50, "returned too early: " + elapsedMs);
        assertTrue(elapsedMs < MUTATE_TIMEOUT_MS + 2000, "returned too late: " + elapsedMs);
        awaitTrue("server received the command", () -> server.received() == 1);
        assertTrue(connectionFactory.lastMutateOp().isTimedOut());
        assertEquals(node.getContinuousTimeout(), 1);

        final Timer timer = findCallTimer("timeout", "failure");
        assertNotNull(timer, "no ipc.client.call timer with ipc.status=timeout, ipc.result=failure");
        assertEquals(timer.count(), 1);
        assertTrue(tagsOf(timer).contains(new BasicTag(EVCacheMetricsFactory.FAILED_HOST, server.address().getHostName())));
    }

    @Test
    public void mutateThatTimedOutBeforeBeingSentIsNeverApplied() throws Exception {
        // nothing listens on the port yet, so the op cannot be written and times out in the queue
        final int port;
        try (ServerSocket probe = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            port = probe.getLocalPort();
        }
        memcachedClient.shutdown(1, TimeUnit.SECONDS);
        server.close();
        startClient(new InetSocketAddress("127.0.0.1", port), false);

        assertEquals(memcachedClient.mutate(Mutator.incr, KEY, 1, 0, 0), -1);
        assertTrue(connectionFactory.lastMutateOp().isTimedOut());
        // the node was not connected, so opTimedOut does not count it (setContinuousTimeout counts only active nodes)
        assertEquals(node.getContinuousTimeout(), 0);

        // bring the server up on that port: once the node reconnects the IO thread drops the op instead of writing it
        server = new FakeMemcached(port);
        awaitTrue("node reconnected", () -> node.isActive());
        awaitTrue("timed out op dropped unsent", () -> connectionFactory.lastMutateOp().isTimedOutUnsent());
        assertEquals(server.received(), 0);

        // the connection is usable and the dropped op was the only one that never reached the server
        server.startReplying(valueReply(5));
        assertEquals(memcachedClient.mutate(Mutator.incr, KEY, 1, 0, 0), 5);
        assertEquals(server.received(), 1);
    }

    @Test
    public void everyTimedOutMutateIsCountedAndAnAnswerResetsTheCounter() throws Exception {
        for (int i = 1; i <= 3; i++) {
            assertEquals(memcachedClient.mutate(Mutator.incr, KEY, 1, 0, 0), -1);
            assertEquals(node.getContinuousTimeout(), i);
        }

        server.startReplying(valueReply(42));

        assertEquals(memcachedClient.mutate(Mutator.incr, KEY, 1, 0, 0), 42);
        assertEquals(node.getContinuousTimeout(), 0);
    }

    @Test
    public void serverErrorAnswerResetsTheCounterAndIsNotATimeout() throws Exception {
        assertEquals(memcachedClient.mutate(Mutator.incr, KEY, 1, 0, 0), -1);
        assertEquals(memcachedClient.mutate(Mutator.incr, KEY, 1, 0, 0), -1);
        assertEquals(node.getContinuousTimeout(), 2);

        server.startReplying(NOT_FOUND);

        assertEquals(memcachedClient.mutate(Mutator.decr, KEY, 1, 0, 0), -1);
        assertFalse(connectionFactory.lastMutateOp().isTimedOut());
        assertEquals(node.getContinuousTimeout(), 0);
    }

    @Test
    public void cancelledOpLeavesTheCounterUnchangedAndIsNotATimeout() throws Exception {
        assertEquals(memcachedClient.mutate(Mutator.incr, KEY, 1, 0, 0), -1);
        assertEquals(memcachedClient.mutate(Mutator.incr, KEY, 1, 0, 0), -1);
        assertEquals(node.getContinuousTimeout(), 2);

        // cancel the op once it is in flight, the way a reconnect or a failure mode would
        final Thread canceller = new Thread(() -> {
            try {
                awaitTrue("mutate op", () -> connectionFactory.lastMutateOp() != null && connectionFactory.mutateOps() == 3);
                connectionFactory.lastMutateOp().cancel();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        canceller.start();
        final long result = memcachedClient.mutate(Mutator.incr, KEY, 1, 0, 0);
        canceller.join();

        assertEquals(result, -1);
        assertFalse(connectionFactory.lastMutateOp().isTimedOut());
        assertEquals(node.getContinuousTimeout(), 2);
        assertNotNull(findCallTimer("cancelled", "failure"));
    }

    @Test
    public void answeredMutateReturnsTheServerValue() throws Exception {
        server.startReplying(valueReply(42));

        assertEquals(memcachedClient.mutate(Mutator.incr, KEY, 1, 0, 0), 42);
        assertFalse(connectionFactory.lastMutateOp().isTimedOut());
        assertEquals(node.getContinuousTimeout(), 0);

        server.startReplying(valueReply(7));
        assertEquals(memcachedClient.incr(KEY, 1, 0, 0), 7);
        assertEquals(memcachedClient.decr(KEY, 1, 0, 0), 7);
    }

    private Timer findCallTimer(String ipcStatus, String ipcResult) {
        for (Timer t : registry.timers().collect(Collectors.toList())) {
            final List<Tag> tags = tagsOf(t);
            if (t.id().name().equals(EVCacheMetricsFactory.IPC_CALL)
                    && tags.contains(new BasicTag(EVCacheMetricsFactory.CACHE, appName))
                    && tags.contains(new BasicTag(EVCacheMetricsFactory.CALL_TAG, "incr"))
                    && tags.contains(new BasicTag(EVCacheMetricsFactory.IPC_STATUS, ipcStatus))
                    && tags.contains(new BasicTag(EVCacheMetricsFactory.IPC_RESULT, ipcResult))) {
                return t;
            }
        }
        return null;
    }

    private static List<Tag> tagsOf(Timer timer) {
        final List<Tag> tags = new ArrayList<Tag>();
        timer.id().tags().forEach(tags::add);
        return tags;
    }

    private static String valueReply(long value) {
        final String s = Long.toString(value);
        return "VA " + s.length() + "\r\n" + s + "\r\n";
    }

    private static void awaitTrue(String what, BooleanSupplier condition) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("Timed out waiting for " + what);
            Thread.sleep(5);
        }
    }

    /** Ascii connection factory that builds the same node, connection and operation factory as BaseAsciiConnectionFactory. */
    private static class TestConnectionFactory extends DefaultConnectionFactory {
        private final EVCacheClient client;
        private final long startTime = System.currentTimeMillis();
        private final AtomicInteger mutateOps = new AtomicInteger();
        private volatile Operation lastMutateOp;
        private final OperationFactory operationFactory = new EVCacheAsciiOperationFactory() {
            @Override
            public MutatorOperation mutate(Mutator m, String key, long by, long def, int exp, OperationCallback cb) {
                final MutatorOperation op = super.mutate(m, key, by, def, exp, cb);
                lastMutateOp = op;
                mutateOps.incrementAndGet();
                return op;
            }
        };

        TestConnectionFactory(EVCacheClient client) {
            super();
            this.client = client;
        }

        @Override
        public MemcachedConnection createConnection(List<InetSocketAddress> addrs) throws IOException {
            return new EVCacheConnection("mutate-test", getReadBufSize(), this, addrs, getInitialObservers(),
                    getFailureMode(), getOperationFactory());
        }

        @Override
        public OperationFactory getOperationFactory() {
            return operationFactory;
        }

        Operation lastMutateOp() {
            return lastMutateOp;
        }

        int mutateOps() {
            return mutateOps.get();
        }

        @Override
        public MemcachedNode createMemcachedNode(java.net.SocketAddress sa, SocketChannel c, int bufSize) {
            return new EVCacheAsciiNodeImpl(sa, c, bufSize, createReadOperationQueue(), createWriteOperationQueue(),
                    new java.util.concurrent.ArrayBlockingQueue<Operation>(getOpQueueLen()), getOpQueueMaxBlockTime(),
                    false, getOperationTimeout(), getAuthWaitTime(), this, client, startTime);
        }

        @Override
        public boolean isDaemon() {
            return true;
        }
    }

    /**
     * Minimal fake memcached: counts the meta-arithmetic ("ma ...") request lines it receives and keeps them
     * unanswered until {@link #startReplying}, which also answers the backlog in order.
     */
    private static class FakeMemcached implements Closeable {
        private final ServerSocket serverSocket;
        private final List<Socket> sockets = new CopyOnWriteArrayList<Socket>();
        private final List<OutputStream> unanswered = new ArrayList<OutputStream>();
        private int received;
        private String reply;

        FakeMemcached(int port) throws IOException {
            serverSocket = new ServerSocket(port, 50, InetAddress.getByName("127.0.0.1"));
            final Thread acceptor = new Thread(this::acceptLoop, "fake-memcached-accept");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        InetSocketAddress address() {
            return new InetSocketAddress("127.0.0.1", serverSocket.getLocalPort());
        }

        synchronized int received() {
            return received;
        }

        synchronized void startReplying(String replyBytes) throws IOException {
            reply = replyBytes;
            for (OutputStream out : unanswered) write(out);
            unanswered.clear();
        }

        private void write(OutputStream out) throws IOException {
            out.write(reply.getBytes(StandardCharsets.ISO_8859_1));
            out.flush();
        }

        private void acceptLoop() {
            try {
                while (!serverSocket.isClosed()) {
                    final Socket s = serverSocket.accept();
                    sockets.add(s);
                    final Thread t = new Thread(() -> serve(s), "fake-memcached-conn");
                    t.setDaemon(true);
                    t.start();
                }
            } catch (IOException e) {
                // closed
            }
        }

        private void serve(Socket s) {
            try {
                final InputStream in = s.getInputStream();
                final OutputStream out = s.getOutputStream();
                final StringBuilder line = new StringBuilder();
                int b;
                while ((b = in.read()) != -1) {
                    if (b == '\n') {
                        if (line.toString().startsWith("ma ")) onCommand(out);
                        line.setLength(0);
                    } else if (b != '\r') {
                        line.append((char) b);
                    }
                }
            } catch (IOException e) {
                // closed
            }
        }

        private synchronized void onCommand(OutputStream out) throws IOException {
            received++;
            if (reply != null) {
                write(out);
            } else {
                unanswered.add(out);
            }
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
            for (Socket s : sockets) s.close();
        }
    }
}
