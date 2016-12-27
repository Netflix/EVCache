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

import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.EVCacheClient;
import net.spy.memcached.ConnectionObserver;

public class EVCacheConnectionObserver implements ConnectionObserver, EVCacheConnectionObserverMBean {

    private static final Logger log = LoggerFactory.getLogger(EVCacheConnectionObserver.class);
    private final EVCacheClient client;
    
    private long lostCount = 0;
    private long connectCount = 0;
    private final Set<SocketAddress> evCacheActiveSet;
    private final Set<SocketAddress> evCacheInActiveSet;
    private final Map<InetSocketAddress, Long> evCacheActiveStringSet;
    private final Map<InetSocketAddress, Long> evCacheInActiveStringSet;

    public EVCacheConnectionObserver(EVCacheClient client) {
    	this.client = client;
        this.evCacheActiveSet = Collections.newSetFromMap(new ConcurrentHashMap<SocketAddress, Boolean>());
        this.evCacheInActiveSet = Collections.newSetFromMap(new ConcurrentHashMap<SocketAddress, Boolean>());
        this.evCacheActiveStringSet = new ConcurrentHashMap<InetSocketAddress, Long>();
        this.evCacheInActiveStringSet = new ConcurrentHashMap<InetSocketAddress, Long>();

        setupMonitoring(false);
    }

    public void connectionEstablished(SocketAddress sa, int reconnectCount) {
        final String address = sa.toString();
        evCacheActiveSet.add(sa);
        evCacheInActiveSet.remove(sa);
        final InetSocketAddress inetAdd = (InetSocketAddress) sa;
        evCacheActiveStringSet.put(inetAdd, Long.valueOf(System.currentTimeMillis()));
        evCacheInActiveStringSet.remove(inetAdd);
        if (log.isDebugEnabled()) log.debug(client.getAppName() + ":CONNECTION ESTABLISHED : To " + address + " was established after " + reconnectCount + " retries");
        if(log.isTraceEnabled()) log.trace("Stack", new Exception());
        EVCacheMetricsFactory.getInstance().increment(client.getAppName() + "-CONNECT", client.getTagList());
        connectCount++;
    }

    public void connectionLost(SocketAddress sa) {
        final String address = sa.toString();
        evCacheInActiveSet.add(sa);
        evCacheActiveSet.remove(sa);
        final InetSocketAddress inetAdd = (InetSocketAddress) sa;
        evCacheInActiveStringSet.put(inetAdd, Long.valueOf(System.currentTimeMillis()));
        evCacheActiveStringSet.remove(inetAdd);
        if (log.isDebugEnabled()) log.debug(client.getAppName() + ":CONNECTION LOST : To " + address);
        if(log.isTraceEnabled()) log.trace("Stack", new Exception());
        EVCacheMetricsFactory.getInstance().increment(client.getAppName()+ "-CONNECTION_LOST", client.getTagList());
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
            final ObjectName mBeanName = ObjectName.getInstance("com.netflix.evcache:Group=" + client.getAppName()
                    + ",SubGroup=pool,SubSubGroup=" + client.getServerGroupName()+ ",SubSubSubGroup=" + client.getId());
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mbeanServer.isRegistered(mBeanName)) {
                if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanName
                        + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanName);
            }
            if (!shutdown) {
                mbeanServer.registerMBean(this, mBeanName);
            }
        } catch (Exception e) {
            if (log.isWarnEnabled()) log.warn(e.getMessage(), e);
        }
    }

    private void unRegisterInActiveNodes() {
        try {
            for (SocketAddress sa : evCacheInActiveSet) {
                final ObjectName mBeanName = ObjectName.getInstance("com.netflix.evcache:Group=" + client.getAppName()
                        + ",SubGroup=pool" + ",SubSubGroup=" + client.getServerGroupName() + ",SubSubSubGroup=" + client.getId()
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
        return "EVCacheConnectionObserver [" 
                + "EVCacheClient=" + client 
                + ", evCacheActiveSet=" + evCacheActiveSet
                + ", evCacheInActiveSet=" + evCacheInActiveSet
                + ", evCacheActiveStringSet=" + evCacheActiveStringSet
                + ", evCacheInActiveStringSet=" + evCacheInActiveStringSet
                + "]";
    }

    public String getAppName() {
        return client.getAppName();
    }

    public String getServerGroup() {
        return client.getServerGroup().toString();
    }

    public int getId() {
        return client.getId();
    }
    
    public EVCacheClient getClient() {
    	return client;
    }

}