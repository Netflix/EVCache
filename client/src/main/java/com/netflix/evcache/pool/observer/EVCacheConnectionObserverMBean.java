package com.netflix.evcache.pool.observer;

import java.net.SocketAddress;
import java.util.Set;


public interface EVCacheConnectionObserverMBean {

    /**
     * Returns the number of Active Servers associated with this Observer.
     */
    int getActiveServerCount();

    /**
     * Returns the Set of address of Active Servers associated with this Observer.
     */
    Set<SocketAddress> getActiveServerNames();

    /**
     * Returns the number of InActive Servers associated with this Observer.
     */
    int getInActiveServerCount();

    /**
     * Returns the Set of address of InActive Servers associated with this Observer.
     */
    Set<SocketAddress> getInActiveServerNames();
}
