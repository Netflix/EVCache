package com.netflix.evcache.pool.observer;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.tag.BasicTagList;
import com.netflix.servo.tag.TagList;

import net.spy.memcached.ConnectionObserver;

public class EVCacheConnectionObserver implements ConnectionObserver, EVCacheConnectionObserverMBean {

    private static final Logger log = LoggerFactory.getLogger(EVCacheConnectionObserver.class);
    private final InstanceInfo instanceInfo;
    private final String appName;
    private final ServerGroup serverGroup;
    private final int id;
    private long lostCount = 0;
    private long connectCount = 0;
    private final Set<SocketAddress> evCacheActiveSet;
    private final Set<SocketAddress> evCacheInActiveSet;
    private final Map<InetSocketAddress, Long> evCacheActiveStringSet;
    private final Map<InetSocketAddress, Long> evCacheInActiveStringSet;
    private final Counter connect, lost;

    private final String monitorName;

    @SuppressWarnings("deprecation")
    public EVCacheConnectionObserver(String appName, ServerGroup serverGroup, int id) {
        this.instanceInfo = ApplicationInfoManager.getInstance().getInfo();
        this.appName = appName;
        this.serverGroup = serverGroup;
        this.evCacheActiveSet = Collections.newSetFromMap(new ConcurrentHashMap<SocketAddress, Boolean>());
        this.evCacheInActiveSet = Collections.newSetFromMap(new ConcurrentHashMap<SocketAddress, Boolean>());
        this.evCacheActiveStringSet = new ConcurrentHashMap<InetSocketAddress, Long>();
        this.evCacheInActiveStringSet = new ConcurrentHashMap<InetSocketAddress, Long>();
        this.id = id;
        monitorName = appName + "_" + serverGroup.getName() + "_" + id + "_connections";

        final TagList tags = BasicTagList.of("ServerGroup", serverGroup.getName(), "AppName", appName);
        this.connect = EVCacheMetricsFactory.getCounter("EVCacheConnectionObserver_CONNECT", tags);
        this.lost = EVCacheMetricsFactory.getCounter("EVCacheConnectionObserver_LOST", tags);

        setupMonitoring(false);
    }

    public void connectionEstablished(SocketAddress sa, int reconnectCount) {
        final String address = sa.toString();
        evCacheActiveSet.add(sa);
        evCacheInActiveSet.remove(sa);
        final InetSocketAddress inetAdd = (InetSocketAddress) sa;
        evCacheActiveStringSet.put(inetAdd, Long.valueOf(System.currentTimeMillis()));
        evCacheInActiveStringSet.remove(inetAdd);
        if (instanceInfo != null) {
            if (log.isDebugEnabled()) log.debug(appName + ":CONNECTION ESTABLISHED : From " + instanceInfo.getHostName()
                    + " to " + address + " was established after " + reconnectCount + " retries");
        }
        connect.increment();
        connectCount++;
    }

    public void connectionLost(SocketAddress sa) {
        final String address = sa.toString();
        evCacheInActiveSet.add(sa);
        evCacheActiveSet.remove(sa);
        final InetSocketAddress inetAdd = (InetSocketAddress) sa;
        evCacheInActiveStringSet.put(inetAdd, Long.valueOf(System.currentTimeMillis()));
        evCacheActiveStringSet.remove(inetAdd);
        if (instanceInfo != null) {
            if (log.isDebugEnabled()) log.debug(appName + ":CONNECTION LOST : From " + instanceInfo.getHostName()
                    + " to " + address);
        }
        lost.increment();
        lostCount++;
    }

    public int getActiveServerCount() {
        return evCacheActiveSet.size();
    }

    public Set<SocketAddress> getActiveServerNames() {
        return evCacheActiveSet;
    }

    public int getInActiveServerCount() {
        return evCacheInActiveSet.size();
    }

    public Set<SocketAddress> getInActiveServerNames() {
        return evCacheInActiveSet;
    }

    public long getLostCount() {
        return lostCount;
    }

    public long getConnectCount() {
        return connectCount;
    }

    public Map<InetSocketAddress, Long> getInActiveServers() {
        return evCacheInActiveStringSet;
    }

    public Map<InetSocketAddress, Long> getActiveServers() {
        return evCacheActiveStringSet;
    }

    private void setupMonitoring(boolean shutdown) {
        try {
            final ObjectName mBeanName = ObjectName.getInstance("com.netflix.evcache:Group=" + appName
                    + ",SubGroup=pool,SubSubGroup=" + serverGroup.getName() + ",SubSubSubGroup=" + id);
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mbeanServer.isRegistered(mBeanName)) {
                if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanName
                        + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanName);
            }
            if (!shutdown) {
                mbeanServer.registerMBean(this, mBeanName);
                Monitors.registerObject(this);
            } else {
                Monitors.unregisterObject(this);
            }
        } catch (Exception e) {
            if (log.isWarnEnabled()) log.warn(e.getMessage(), e);
        }
    }

    private void unRegisterInActiveNodes() {
        try {
            for (SocketAddress sa : evCacheInActiveSet) {
                final ObjectName mBeanName = ObjectName.getInstance("com.netflix.evcache:Group=" + appName
                        + ",SubGroup=pool" + ",SubSubGroup=" + serverGroup.getName() + ",SubSubSubGroup=" + id
                        + ",SubSubSubSubGroup=" + ((InetSocketAddress) sa).getHostName());
                final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
                if (mbeanServer.isRegistered(mBeanName)) {
                    if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanName
                            + " has been registered. Will unregister the previous instance and register a new one.");
                    mbeanServer.unregisterMBean(mBeanName);
                }
            }
        } catch (Exception e) {
            if (log.isWarnEnabled()) log.warn(e.getMessage(), e);
        }
    }

    public void shutdown() {
        unRegisterInActiveNodes();
        setupMonitoring(true);
    }

    public String toString() {
        return "EVCacheConnectionObserver [instanceInfo=" + instanceInfo
                + ", appName=" + appName + ", ServerGroup=" + serverGroup.toString() + ", id=" + id
                + ", evCacheActiveSet=" + evCacheActiveSet
                + ", evCacheInActiveSet=" + evCacheInActiveSet
                + ", evCacheActiveStringSet=" + evCacheActiveStringSet
                + ", evCacheInActiveStringSet=" + evCacheInActiveStringSet
                + ", monitorName=" + monitorName + "]";
    }

    public String getAppName() {
        return appName;
    }

    public String getServerGroup() {
        return serverGroup.toString();
    }

    public int getId() {
        return id;
    }

}