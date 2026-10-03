package com.netflix.evcache.pool;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes the loop thread's CPU-time to wall-time ratio from the loop thread itself.
 *
 * <p>The loop thread periodically publishes an immutable {@code long[]} snapshot
 * containing {@code {threadCpuNs, wallNs}}. Spectator's polling thread reads the
 * latest snapshot and computes the delta ratio without performing cross-thread
 * ThreadMXBean lookups.</p>
 *
 * <p>The reported value is {@code dCpu/dWall} in [0, 1.05]: the fraction of wall
 * time the loop thread was on a CPU. Time parked in {@code selector.select()}
 * is correctly excluded, but time the thread was runnable-but-descheduled
 * (CPU contention) is also excluded, so this metric is a lower bound on true
 * loop demand under CPU pressure.</p>
 */
public final class EVCacheLoopProbe {
    private static final Logger log = LoggerFactory.getLogger(EVCacheLoopProbe.class);
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
    private static final long PUBLISH_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(1_000);
    private static final int CPU_UTILIZATION_WARNING_THRESHOLD = 3;

    private final AtomicReference<long[]> snapshot = new AtomicReference<long[]>(new long[] { 0L, 0L });
    private final boolean cpuTimeAvailable;

    // Loop-thread-private throttle state.
    private long nextPublishNs;
    private boolean tickFailureLogged;
    private boolean negativeCpuTimeLogged;

    // PolledMeter-reader-private state.
    private long prevCpuNs;
    private long prevWallNs;
    private int aboveOneSamples;
    private boolean aboveOneLogged;

    public EVCacheLoopProbe() {
        this.cpuTimeAvailable = isCurrentThreadCpuTimeAvailable();
        if (!cpuTimeAvailable) {
            log.warn("Thread CPU time is not available; EVCache loop cpuWallTimeRatio will report NaN");
        }
    }

    /**
     * Publish the current thread's CPU time and wall time at most every second.
     *
     * <p>This method is intentionally no-throw: it is called from the EVCache IO
     * loop in a finally block and must never terminate the loop.</p>
     */
    public void tick() {
        try {
            tickInternal();
        } catch (Throwable t) {
            if (!tickFailureLogged) {
                tickFailureLogged = true;
                try {
                    log.warn("EVCache loop cpuWallTimeRatio probe failed; suppressing future probe errors", t);
                } catch (Throwable ignored) {
                    // Keep the event loop alive even if logging fails.
                }
            }
        }
    }

    private void tickInternal() {
        if (!cpuTimeAvailable) return;

        final long now = System.nanoTime();
        if (nextPublishNs != 0L && now - nextPublishNs < 0L) return;
        nextPublishNs = now + PUBLISH_INTERVAL_NS;

        final long cpuNs = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        if (cpuNs < 0L) {
            if (!negativeCpuTimeLogged) {
                negativeCpuTimeLogged = true;
                log.warn("Thread CPU time returned a negative value; skipping EVCache loop cpuWallTimeRatio publish");
            }
            return;
        }

        snapshot.lazySet(new long[] { cpuNs, now });
    }

    /**
     * Return loop-thread cpu-time / wall-time ratio over the interval since the previous poll.
     */
    public double sampleCpuWallTimeRatio() {
        if (!cpuTimeAvailable) return Double.NaN;

        final long[] s = snapshot.get();
        final long cpuNs = s[0];
        final long wallNs = s[1];
        if (prevWallNs == 0L) {
            prevCpuNs = cpuNs;
            prevWallNs = wallNs;
            return Double.NaN;
        }

        final long dWall = wallNs - prevWallNs;
        if (dWall <= 0L) return 0.0;

        final long dCpu = cpuNs - prevCpuNs;
        prevCpuNs = cpuNs;
        prevWallNs = wallNs;

        double ratio = (double) dCpu / (double) dWall;
        if (ratio < 0.0) return 0.0;

        if (ratio > 1.0) {
            aboveOneSamples++;
            if (aboveOneSamples >= CPU_UTILIZATION_WARNING_THRESHOLD && !aboveOneLogged) {
                aboveOneLogged = true;
                log.warn("EVCache loop cpuWallTimeRatio exceeded 1.0 for {} consecutive samples; latest value={}",
                        CPU_UTILIZATION_WARNING_THRESHOLD, ratio);
            }
        } else {
            aboveOneSamples = 0;
        }

        return Math.min(ratio, 1.05);
    }

    private static boolean isCurrentThreadCpuTimeAvailable() {
        try {
            return THREAD_MX_BEAN.isThreadCpuTimeSupported() && THREAD_MX_BEAN.isThreadCpuTimeEnabled();
        } catch (Throwable t) {
            log.warn("Unable to determine ThreadMXBean CPU-time capability", t);
            return false;
        }
    }
}
