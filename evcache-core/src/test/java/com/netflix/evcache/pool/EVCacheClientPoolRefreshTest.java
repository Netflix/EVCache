package com.netflix.evcache.pool;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.observer.EVCacheConnectionObserver;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.Tag;

import net.spy.memcached.EVCacheMemcachedClient;
import net.spy.memcached.EVCacheNode;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.ops.Operation;

/**
 * Drives {@link EVCacheClientPool}'s refresh checks with mocked clients and connection observers.
 */
public class EVCacheClientPoolRefreshTest {

    private static final long STALLED_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final Set<InetSocketAddress> NONE = Collections.emptySet();

    private final DefaultSettableConfig config = new DefaultSettableConfig();
    private final DefaultRegistry registry = new DefaultRegistry();

    private String appName;
    private EVCacheNodeList provider;
    private EVCacheClientPool pool;

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
        // a new app name per test keeps the cached gauges of different tests apart
        appName = "POOLREFRESHTEST" + System.nanoTime();
        provider = mock(EVCacheNodeList.class);
        when(provider.discoverInstances(appName)).thenReturn(Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap());
        final EVCacheClientPoolManager manager = mock(EVCacheClientPoolManager.class);
        when(manager.getDefaultReadTimeout()).thenReturn(EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".test.readTimeout", Integer.class).orElse(20));
        pool = new EVCacheClientPool(appName, provider, mock(ThreadPoolExecutor.class), manager, false);
    }

    @Test
    public void replacedNodeIsDetectedWhileTheDeadNodeStillLooksConnected() throws Exception {
        final ServerGroup serverGroup = serverGroup("e");
        final Set<InetSocketAddress> healthy = addresses(1, 9);
        final Set<InetSocketAddress> withDeadNode = with(healthy, address(10));
        // the dead node's connection is half open, so the client still counts it as active
        setClients(serverGroup, client(withDeadNode, NONE, nodes(withDeadNode)));
        assertFalse(pool.haveInstancesInServerGroupChanged(serverGroup, withDeadNode)); // also uses up the first reconcile

        // discovery replaced it, so active, discovered and hashed counts all still match (10)
        assertTrue(pool.haveInstancesInServerGroupChanged(serverGroup, with(healthy, address(11))));
    }

    @Test
    public void reconcilesEachServerGroupOnItsOwnTimer() throws Exception {
        final ServerGroup c = serverGroup("c"), d = serverGroup("d");
        final Set<InetSocketAddress> hostsInC = addresses(1, 3), hostsInD = addresses(4, 6);
        setClients(c, client(hostsInC, NONE, nodes(hostsInC)));
        setClients(d, client(hostsInD, NONE, nodes(hostsInD)));

        pool.haveInstancesInServerGroupChanged(c, hostsInC);
        pool.haveInstancesInServerGroupChanged(d, hostsInD);
        assertEquals(reconcileGauge(c), 1.0);
        assertEquals(reconcileGauge(d), 1.0);

        pool.haveInstancesInServerGroupChanged(c, hostsInC);
        assertEquals(reconcileGauge(c), 0.0);
    }

    @Test
    public void refreshReconnectsOnlyTheNodeThatStoppedReplying() throws Exception {
        final ServerGroup serverGroup = serverGroup("e");
        final Set<InetSocketAddress> hosts = addresses(1, 2);
        final long now = System.nanoTime();
        final EVCacheNode replying = node(address(1), true, op(now - TimeUnit.MILLISECONDS.toNanos(5)));
        final EVCacheNode silent = node(address(2), true, op(now - TimeUnit.SECONDS.toNanos(60)));
        final EVCacheMemcachedClient memcachedClient = mock(EVCacheMemcachedClient.class);
        final EVCacheClient client = client(hosts, NONE, Arrays.<MemcachedNode> asList(replying, silent));
        when(client.getEVCacheMemcachedClient()).thenReturn(memcachedClient);
        when(client.getServerGroupName()).thenReturn(serverGroup.getName());
        setClients(serverGroup, client);
        when(provider.discoverInstances(appName)).thenReturn(Collections.singletonMap(serverGroup, new EVCacheServerGroupConfig(serverGroup, hosts)));

        pool.refreshPool(false, false);

        verify(memcachedClient).reconnectNode(silent);
        verify(memcachedClient, never()).reconnectNode(replying);
    }

    @Test
    public void readQueueIsStalledOnlyWhenAConnectedNodeHasNotAnsweredAWrittenOp() {
        final long now = System.nanoTime();
        final Operation old = op(now - STALLED_NANOS - 1);
        assertTrue(EVCacheClientPool.isReadQueueStalled(node(address(1), true, old), STALLED_NANOS, now));
        assertFalse(EVCacheClientPool.isReadQueueStalled(node(address(1), true, op(now - 1)), STALLED_NANOS, now), "answering");
        assertFalse(EVCacheClientPool.isReadQueueStalled(node(address(1), true, null), STALLED_NANOS, now), "nothing in flight");
        assertFalse(EVCacheClientPool.isReadQueueStalled(node(address(1), true, op(0)), STALLED_NANOS, now), "op not fully written yet");
        assertFalse(EVCacheClientPool.isReadQueueStalled(node(address(1), false, old), STALLED_NANOS, now), "already reconnecting");
    }

    private ServerGroup serverGroup(String zone) {
        return new ServerGroup("us-east-1" + zone, appName.toLowerCase() + "-useast1" + zone + "-v000");
    }

    @SuppressWarnings("unchecked")
    private void setClients(ServerGroup serverGroup, EVCacheClient... clients) throws Exception {
        final Field field = EVCacheClientPool.class.getDeclaredField("memcachedInstancesByServerGroup");
        field.setAccessible(true);
        ((Map<ServerGroup, List<EVCacheClient>>) field.get(pool)).put(serverGroup, Arrays.asList(clients));
    }

    private static EVCacheClient client(Set<InetSocketAddress> active, Set<InetSocketAddress> inactive, List<MemcachedNode> hashed) {
        final EVCacheConnectionObserver observer = mock(EVCacheConnectionObserver.class);
        when(observer.getActiveServerCount()).thenReturn(active.size());
        when(observer.getInActiveServerCount()).thenReturn(inactive.size());
        when(observer.getActiveServers()).thenReturn(seenAt(active));
        when(observer.getInActiveServers()).thenReturn(seenAt(inactive));
        final NodeLocator locator = mock(NodeLocator.class);
        when(locator.getAll()).thenReturn(hashed);
        final EVCacheClient client = mock(EVCacheClient.class);
        when(client.getConnectionObserver()).thenReturn(observer);
        when(client.getNodeLocator()).thenReturn(locator);
        return client;
    }

    private static List<MemcachedNode> nodes(Set<InetSocketAddress> addresses) {
        final MemcachedNode[] nodes = addresses.stream().map(a -> node(a, true, null)).toArray(MemcachedNode[]::new);
        return Arrays.asList(nodes);
    }

    private static EVCacheNode node(InetSocketAddress address, boolean active, Operation currentReadOp) {
        final EVCacheNode node = mock(EVCacheNode.class);
        when(node.getSocketAddress()).thenReturn(address);
        when(node.isActive()).thenReturn(active);
        when(node.getCurrentReadOp()).thenReturn(currentReadOp);
        return node;
    }

    private static Operation op(long writeCompleteNanos) {
        final Operation op = mock(Operation.class);
        when(op.getWriteCompleteTimestamp()).thenReturn(writeCompleteNanos);
        return op;
    }

    private static Map<InetSocketAddress, Long> seenAt(Set<InetSocketAddress> addresses) {
        final Map<InetSocketAddress, Long> map = new HashMap<InetSocketAddress, Long>();
        for (InetSocketAddress address : addresses) map.put(address, Long.valueOf(System.currentTimeMillis()));
        return map;
    }

    private static InetSocketAddress address(int i) {
        return InetSocketAddress.createUnresolved("10.0.0." + i, 11211);
    }

    private static Set<InetSocketAddress> addresses(int from, int to) {
        final Set<InetSocketAddress> set = new LinkedHashSet<InetSocketAddress>();
        for (int i = from; i <= to; i++) set.add(address(i));
        return set;
    }

    private static Set<InetSocketAddress> with(Set<InetSocketAddress> addresses, InetSocketAddress extra) {
        final Set<InetSocketAddress> set = new LinkedHashSet<InetSocketAddress>(addresses);
        set.add(extra);
        return set;
    }

    private double reconcileGauge(ServerGroup serverGroup) {
        return registry.gauges()
                .filter(g -> g.id().name().equals(EVCacheMetricsFactory.INTERNAL_POOL_SG_CONFIG)
                        && hasTag(g.id(), EVCacheMetricsFactory.CONFIG_NAME, EVCacheMetricsFactory.POOL_RECONCILE)
                        && hasTag(g.id(), EVCacheMetricsFactory.SERVERGROUP, serverGroup.getName()))
                .findFirst().map(Gauge::value).orElse(Double.NaN);
    }

    private static boolean hasTag(Id id, String key, String value) {
        for (Tag tag : id.tags()) {
            if (tag.key().equals(key) && tag.value().equals(value)) return true;
        }
        return false;
    }
}
