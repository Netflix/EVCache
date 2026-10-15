package com.netflix.evcache.pool;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.evcache.pool.observer.EVCacheConnectionObserver;
import com.netflix.evcache.util.EVCacheConfig;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EVCacheClientPoolTest {

    private static final String APP = "SECURE_POOL_TEST";
    private DefaultSettableConfig config;
    private RecordingPool pool;

    @BeforeMethod
    public void setUp() {
        config = new DefaultSettableConfig();
        new EVCacheConfig(DefaultPropertyFactory.from(config));
    }

    @AfterMethod
    public void tearDown() {
        if (pool != null) pool.shutdown();
    }

    @Test
    public void disablingSecureModeDiscardsAllReplicaClientsAndForcesRefresh() {
        config.setProperty(APP + ".use.secure", true);
        createPool();
        EVCacheClient[] clients = new EVCacheClient[3];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = mock(EVCacheClient.class);
            when(clients[i].getConnectionObserver()).thenReturn(mock(EVCacheConnectionObserver.class));
            pool.getAllInstancesByServerGroupInternal().put(new ServerGroup("zone-" + i, "replica-" + i),
                    Collections.singletonList(clients[i]));
        }
        int refreshes = pool.refreshCount;

        config.setProperty(APP + ".use.secure", false);

        assertForcedRefresh(refreshes);
        assertTrue(pool.getAllInstancesByServerGroupInternal().isEmpty());
        for (EVCacheClient client : clients) {
            verify(client).shutdown(0, TimeUnit.SECONDS);
            verify(client.getConnectionObserver()).shutdown();
        }
    }

    @Test
    public void enablingSecureModeForcesRefresh() {
        createPool();
        int refreshes = pool.refreshCount;

        config.setProperty(APP + ".use.secure", true);

        assertForcedRefresh(refreshes);
    }

    @Test
    public void globalSecureModeChangeForcesRefresh() {
        config.setProperty("evcache.use.secure", true);
        createPool();
        int refreshes = pool.refreshCount;

        config.setProperty("evcache.use.secure", false);

        assertForcedRefresh(refreshes);
    }

    @Test
    public void removingAppOverrideRefreshesUsingGlobalFallback() {
        config.setProperty("evcache.use.secure", true);
        config.setProperty(APP + ".use.secure", false);
        createPool();
        int refreshes = pool.refreshCount;

        config.clearProperty(APP + ".use.secure");

        assertForcedRefresh(refreshes);
    }

    @Test
    public void appOverrideIgnoresGlobalChanges() {
        config.setProperty("evcache.use.secure", false);
        config.setProperty(APP + ".use.secure", true);
        createPool();
        int refreshes = pool.refreshCount;

        config.setProperty("evcache.use.secure", true);

        assertEquals(pool.refreshCount, refreshes);
    }

    private void createPool() {
        EVCacheClientPoolManager manager = mock(EVCacheClientPoolManager.class);
        when(manager.getDefaultReadTimeout()).thenReturn(
                EVCacheConfig.getInstance().getPropertyRepository().get("default.read.timeout", Integer.class).orElse(20));
        pool = new RecordingPool(manager);
    }

    private void assertForcedRefresh(int previousRefreshes) {
        assertEquals(pool.refreshCount, previousRefreshes + 1);
        assertTrue(pool.async);
        assertTrue(pool.force);
    }

    private static class RecordingPool extends EVCacheClientPool {
        private int refreshCount;
        private boolean async;
        private boolean force;

        RecordingPool(EVCacheClientPoolManager manager) {
            super(APP, mock(EVCacheNodeList.class), mock(ThreadPoolExecutor.class), manager, false);
        }

        @Override
        public void refreshPool(boolean async, boolean force) {
            refreshCount++;
            this.async = async;
            this.force = force;
        }
    }
}
