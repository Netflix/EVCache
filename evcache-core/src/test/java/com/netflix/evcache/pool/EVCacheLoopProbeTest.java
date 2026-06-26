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

        assertTrue(Double.isNaN(probe.sampleCpuWallTimeRatio()));
    }

    @Test
    public void reportsCpuWallTimeRatioForCurrentThread() throws Exception {
        assumeThreadCpuTimeAvailable();
        EVCacheLoopProbe probe = new EVCacheLoopProbe();

        probe.tick();
        assertTrue(Double.isNaN(probe.sampleCpuWallTimeRatio()));

        double ratio = samplePositiveRatio(probe);
        assertTrue(ratio > 0.0, "expected positive ratio, got " + ratio);
        assertTrue(ratio <= 1.05, "expected ratio to be clamped, got " + ratio);
    }

    @Test
    public void returnsZeroWhenNoNewSampleWasPublished() throws Exception {
        assumeThreadCpuTimeAvailable();
        EVCacheLoopProbe probe = new EVCacheLoopProbe();

        probe.tick();
        assertTrue(Double.isNaN(probe.sampleCpuWallTimeRatio()));

        assertEquals(probe.sampleCpuWallTimeRatio(), 0.0);
    }

    @Test
    public void polledMeterPublishesProbeRatio() throws Exception {
        assumeThreadCpuTimeAvailable();
        Registry registry = new DefaultRegistry();
        Id id = registry.createId(EVCacheMetricsFactory.INTERNAL_LOOP_CPU_WALL_TIME_RATIO, "evc.connection.id", "0", "ipc.server.asg", "test");
        EVCacheLoopProbe probe = new EVCacheLoopProbe();

        try {
            PolledMeter.using(registry)
                    .withId(id)
                    .monitorValue(probe, EVCacheLoopProbe::sampleCpuWallTimeRatio);

            probe.tick();
            PolledMeter.update(registry);
            Gauge gauge = registry.gauge(id);
            assertTrue(Double.isNaN(gauge.value()));

            double value = pollPositiveRatio(registry, probe, gauge);
            assertTrue(value > 0.0, "expected polled meter to publish positive ratio, got " + value);
            assertTrue(value <= 1.05, "expected ratio to be clamped, got " + value);
        } finally {
            PolledMeter.remove(registry, id);
        }
    }

    private static double samplePositiveRatio(EVCacheLoopProbe probe) throws Exception {
        double ratio = 0.0;
        for (int i = 0; i < 10 && ratio <= 0.0; i++) {
            Thread.sleep(1_100);
            busySpinForAtLeastMillis(50);
            probe.tick();
            ratio = probe.sampleCpuWallTimeRatio();
        }
        return ratio;
    }

    private static double pollPositiveRatio(Registry registry, EVCacheLoopProbe probe, Gauge gauge) throws Exception {
        double ratio = 0.0;
        for (int i = 0; i < 10 && ratio <= 0.0; i++) {
            Thread.sleep(1_100);
            busySpinForAtLeastMillis(50);
            probe.tick();
            PolledMeter.update(registry);
            ratio = gauge.value();
        }
        return ratio;
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
