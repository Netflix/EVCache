package com.netflix.evcache.pool;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;

import org.testng.SkipException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class EVCacheLoopProbeTest {

    @Test
    public void firstSampleEstablishesBaseline() throws Exception {
        assumeThreadCpuTimeAvailable();
        EVCacheLoopProbe probe = new EVCacheLoopProbe();

        probe.tick();

        assertTrue(Double.isNaN(probe.sampleUtilization()));
    }

    @Test
    public void reportsCpuUtilizationForCurrentThread() throws Exception {
        assumeThreadCpuTimeAvailable();
        EVCacheLoopProbe probe = new EVCacheLoopProbe();

        probe.tick();
        assertTrue(Double.isNaN(probe.sampleUtilization()));

        double utilization = samplePositiveUtilization(probe);
        assertTrue(utilization > 0.0, "expected positive utilization, got " + utilization);
        assertTrue(utilization <= 1.05, "expected utilization to be clamped, got " + utilization);
    }

    @Test
    public void returnsZeroWhenNoNewSampleWasPublished() throws Exception {
        assumeThreadCpuTimeAvailable();
        EVCacheLoopProbe probe = new EVCacheLoopProbe();

        probe.tick();
        assertTrue(Double.isNaN(probe.sampleUtilization()));

        assertEquals(probe.sampleUtilization(), 0.0);
    }

    @Test
    public void polledMeterPublishesProbeUtilization() throws Exception {
        assumeThreadCpuTimeAvailable();
        Registry registry = new DefaultRegistry();
        Id id = registry.createId(EVCacheMetricsFactory.INTERNAL_LOOP_CPU_UTILIZATION, "evc.connection.id", "0", "ipc.server.asg", "test");
        EVCacheLoopProbe probe = new EVCacheLoopProbe();

        try {
            PolledMeter.using(registry)
                    .withId(id)
                    .monitorValue(probe, EVCacheLoopProbe::sampleUtilization);

            probe.tick();
            PolledMeter.update(registry);
            Gauge gauge = registry.gauge(id);
            assertTrue(Double.isNaN(gauge.value()));

            double value = pollPositiveUtilization(registry, probe, gauge);
            assertTrue(value > 0.0, "expected polled meter to publish positive utilization, got " + value);
            assertTrue(value <= 1.05, "expected utilization to be clamped, got " + value);
        } finally {
            PolledMeter.remove(registry, id);
        }
    }

    private static double samplePositiveUtilization(EVCacheLoopProbe probe) throws Exception {
        double utilization = 0.0;
        for (int i = 0; i < 10 && utilization <= 0.0; i++) {
            Thread.sleep(1_100);
            busySpinForAtLeastMillis(50);
            probe.tick();
            utilization = probe.sampleUtilization();
        }
        return utilization;
    }

    private static double pollPositiveUtilization(Registry registry, EVCacheLoopProbe probe, Gauge gauge) throws Exception {
        double utilization = 0.0;
        for (int i = 0; i < 10 && utilization <= 0.0; i++) {
            Thread.sleep(1_100);
            busySpinForAtLeastMillis(50);
            probe.tick();
            PolledMeter.update(registry);
            utilization = gauge.value();
        }
        return utilization;
    }

    private static void assumeThreadCpuTimeAvailable() {
        ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
        if (!tmx.isThreadCpuTimeSupported() || !tmx.isThreadCpuTimeEnabled()) {
            throw new SkipException("thread CPU time is not available on this JVM");
        }
    }

    private static void busySpinForAtLeastMillis(long millis) {
        long deadline = System.nanoTime() + millis * 1_000_000L;
        long value = 0L;
        while (System.nanoTime() < deadline) {
            value += System.nanoTime();
        }
        if (value == 42L) {
            throw new AssertionError("unreachable");
        }
    }
}
