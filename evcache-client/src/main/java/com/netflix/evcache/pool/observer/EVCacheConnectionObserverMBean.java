package com.netflix.evcache.pool.observer;

import java.net.SocketAddress;
import java.util.Set;

public interface EVCacheConnectionObserverMBean {

    int getActiveServerCount();

    Set<SocketAddress> getActiveServerNames();

    int getInActiveServerCount();

    Set<SocketAddress> getInActiveServerNames();

    long getLostCount();

    long getConnectCount();
}