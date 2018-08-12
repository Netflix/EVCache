package com.netflix.evcache.pool;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.config.DynamicIntProperty;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Tag;

public class EVCacheExecutor extends ThreadPoolExecutor implements EVCacheExecutorMBean {

    private static final Logger log = LoggerFactory.getLogger(EVCacheExecutor.class);
    private final DynamicIntProperty maxAsyncPoolSize;
    private final DynamicIntProperty coreAsyncPoolSize;
    private final String name;
    private Id completedTaskCount;
    private Gauge currentQueueSize; 

    public EVCacheExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, RejectedExecutionHandler handler, String name) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit,
                new LinkedBlockingQueue<Runnable>(), 
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat( "EVCacheExecutor-" + name + "-%d").build());
        this.name = name;

        maxAsyncPoolSize = EVCacheConfig.getInstance().getDynamicIntProperty("EVCacheExecutor." + name + ".max.size", maximumPoolSize);
        setMaximumPoolSize(maxAsyncPoolSize.get());
        coreAsyncPoolSize = EVCacheConfig.getInstance().getDynamicIntProperty("EVCacheExecutor." + name + ".core.size", corePoolSize);
        setCorePoolSize(coreAsyncPoolSize.get());
        setKeepAliveTime(keepAliveTime, unit);
        maxAsyncPoolSize.addCallback(new Runnable() {
            public void run() {
                setMaximumPoolSize(maxAsyncPoolSize.get());
            }
        });
        coreAsyncPoolSize.addCallback(new Runnable() {
            public void run() {
                setCorePoolSize(coreAsyncPoolSize.get());
                prestartAllCoreThreads();
            }
        });
        
        setupMonitoring(name);
        this.completedTaskCount = EVCacheMetricsFactory.getInstance().getId("EVCacheExecutor.completedTaskCount", Collections.<Tag>emptyList());
        EVCacheMetricsFactory.getInstance().getRegistry().gauge(completedTaskCount, this, EVCacheExecutor::reportMetrics);
        this.currentQueueSize = EVCacheMetricsFactory.getInstance().getRegistry().gauge(EVCacheMetricsFactory.getInstance().getId("EVCacheExecutor.currentQueueSize", Collections.<Tag>emptyList()));
    }
    
    private long reportMetrics() {
        currentQueueSize.set(getQueueSize());
        return getCompletedTaskCount();
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
