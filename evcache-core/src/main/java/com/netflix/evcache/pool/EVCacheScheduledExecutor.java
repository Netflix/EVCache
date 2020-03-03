package com.netflix.evcache.pool;

import java.lang.management.ManagementFactory;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.archaius.api.Property;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.patterns.ThreadPoolMonitor;

public class EVCacheScheduledExecutor extends ScheduledThreadPoolExecutor implements EVCacheScheduledExecutorMBean {

    private static final Logger log = LoggerFactory.getLogger(EVCacheScheduledExecutor.class);
    private final Property<Integer> maxAsyncPoolSize;
    private final Property<Integer> coreAsyncPoolSize;
    private final String name;

    public EVCacheScheduledExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, RejectedExecutionHandler handler, String name) {
        super(corePoolSize, handler);
        this.name = name;

        maxAsyncPoolSize = EVCacheConfig.getInstance().getPropertyRepository().get(name + "executor.max.size", Integer.class).orElse(maximumPoolSize);
        setMaximumPoolSize(maxAsyncPoolSize.get());
        coreAsyncPoolSize = EVCacheConfig.getInstance().getPropertyRepository().get(name + "executor.core.size", Integer.class).orElse(corePoolSize);
        setCorePoolSize(coreAsyncPoolSize.get());
        setKeepAliveTime(keepAliveTime, unit);
        final ThreadFactory asyncFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat( "EVCacheScheduledExecutor-" + name + "-%d").build();
        setThreadFactory(asyncFactory);
        maxAsyncPoolSize.subscribe(this::setMaximumPoolSize);
        coreAsyncPoolSize.subscribe(i -> {
                setCorePoolSize(i);
                prestartAllCoreThreads();
        });
        
        setupMonitoring(name);
        ThreadPoolMonitor.attach(EVCacheMetricsFactory.getInstance().getRegistry(), this, EVCacheMetricsFactory.INTERNAL_EXECUTOR_SCHEDULED + "-" + name);
    }

    private void setupMonitoring(String name) {
        try {
            ObjectName mBeanName = ObjectName.getInstance("com.netflix.evcache:Group=ThreadPool,SubGroup="+name);
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mbeanServer.isRegistered(mBeanName)) {
                if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanName + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanName);
            }
            mbeanServer.registerMBean(this, mBeanName);
        } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug("Exception", e);
        }

    }

    public void shutdown() {
        try {
            ObjectName mBeanName = ObjectName.getInstance("com.netflix.evcache:Group=ThreadPool,SubGroup="+name);
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            mbeanServer.unregisterMBean(mBeanName);
        } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug("Exception", e);
        }
        super.shutdown();
    }

    @Override
    public int getQueueSize() {
        return getQueue().size();
    }


}
