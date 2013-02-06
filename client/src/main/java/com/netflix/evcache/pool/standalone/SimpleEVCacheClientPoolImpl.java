package com.netflix.evcache.pool.standalone;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownServiceException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

/**
 * A Simple EVCache Client pool given a list of memcached nodes
 *  
 * @author smadappa
 *
 */
public class SimpleEVCacheClientPoolImpl extends AbstractEVCacheClientPoolImpl implements SimpleEVCacheClientPoolImplMBean {
    private static Logger log = LoggerFactory.getLogger(SimpleEVCacheClientPoolImpl.class);

    private DynamicStringProperty _serverList; //List of servers
    private List<EVCacheClient> memcachedInstances;

    public SimpleEVCacheClientPoolImpl() {
    }
    
    public void init(final String appName) {
    	super.init(appName);
        this._serverList = DynamicPropertyFactory.getInstance().getStringProperty(appName + ".EVCacheClientPool.hosts", "");
        _serverList.addCallback(this);
        memcachedInstances = new ArrayList<EVCacheClient>(getPoolSize().get());

        try {
            refresh();
        } catch(Throwable t) {
            if(log.isDebugEnabled()) log.debug("Error Refreshing EVCache Instance list for " + appName , t);
        }
    }

	public EVCacheClient getEVCacheClient() {
		if(memcachedInstances == null || memcachedInstances.isEmpty()) return null;
		try {
			if(memcachedInstances.size() == 1) return memcachedInstances.get(0);//Frequently used scenario
			final long currentVal = getNumOfOps().incrementAndGet();
			final int index = (int)currentVal % memcachedInstances.size();
			return memcachedInstances.get(index);
		}
		catch(Throwable t) {
			log.error("Exception trying to get an readable EVCache Instance.", t);
			return null;
		}
    }

    private List<InetSocketAddress> getMemcachedSocketAddressList(final List<String> discoveredHostsInZone) {
        final List<InetSocketAddress> memcachedNodesInZone = new ArrayList<InetSocketAddress>(discoveredHostsInZone.size());
        for(String hostAddress : discoveredHostsInZone) {
            final int colonIndex = hostAddress.lastIndexOf(':');
            final String hostName = hostAddress.substring(0, colonIndex);
            final String portNum = hostAddress.substring(colonIndex + 1);
            memcachedNodesInZone.add(new InetSocketAddress(hostName, Integer.parseInt(portNum)));
        }
        return memcachedNodesInZone;
    }

    private void setupNewClients(List<EVCacheClient> newClients) {
        final List<EVCacheClient> currentClients = memcachedInstances;
        memcachedInstances = newClients;
        if(currentClients == null || currentClients.isEmpty()) return;

        //Now since we have replace the old instances shutdown all the old clients
        if(log.isDebugEnabled()) log.debug("Replaced an existing Pool for app " + getAppName() + " ;\n\tOldClients : " + currentClients + ";\n\tNewClients : " + newClients); 
        for(EVCacheClient client : currentClients) {
            if(!client.isShutdown()) {
                if(log.isDebugEnabled()) log.debug("Shutting down in Fallback -> AppName : " + getAppName() + "; client {" + client + "};" );
                try {
                    if(client.getConnectionObserver() != null) {
                        final boolean obsRemoved = client.removeConnectionObserver();
                        if(log.isDebugEnabled()) log.debug("Connection observer removed " + obsRemoved);
                    }
                    final boolean status = client.shutdown(60, TimeUnit.SECONDS);
                    if(log.isDebugEnabled()) log.debug("Shutting down {" + client + "} ; status : " + status);
                } catch(Exception ex) {
                    log.error("Exception while shutting down the old Client", ex);
                }
            }
        }
    }

    private synchronized void refresh() throws IOException {
        try {
            final List<String> instances = discoverInstances();
            //if no instances are found then bail immediately.
            if(instances == null || instances.isEmpty()) return; 
                final List<InetSocketAddress> memcachedInstances = getMemcachedSocketAddressList(instances);
                final int poolSize = getPoolSize().get();
                final List<EVCacheClient> newClients = new ArrayList<EVCacheClient>(poolSize);
                for(int i = 0; i < poolSize; i++) {
                    final int maxQueueSize = ConfigurationManager.getConfigInstance().getInt(getAppName() + ".max.queue.length", 16384);
                    final EVCacheClient client = new SimpleEVCacheClientImpl(getAppName(), i, maxQueueSize, getReadTimeout(), memcachedInstances);
                    newClients.add(client);
                    if(log.isDebugEnabled()) log.debug("AppName :" + getAppName() + "; intit : client.getId() : " + client.getId());
                }
                setupNewClients(newClients);
        } catch(Throwable t) {
            log.error("Exception while refreshing the Server list", t);
        }
	}

    /**
     * Discover memcached instances suitable for our use from the Discovery Service.
     *
     * @throws UnknownServiceException if no suitable instances can be
     *    found
     * @throws IllegalStateException if an error occurred in the Discovery
     *    service
     *
     *  TODO : Add a fallback place to get the list say from FastProperties
     */
    private List<String> discoverInstances() throws IOException {
        if(isShutdown()) {
            return Collections.<String>emptyList();
        }

        final List<String> instances = new ArrayList<String>();
        final String serverList = _serverList.get();
        final StringTokenizer stk = new StringTokenizer(serverList, ",");

        /* Iterate all the discovered instances to find usable ones */
        while(stk.hasMoreTokens()) {
            final String token = stk.nextToken();
            final String memcachedHost;
            final String memcachedPort;
            int index = 0;
            if((index = token.indexOf(":")) == -1 ) {
            	memcachedHost = token;
            	memcachedPort = "11211";
            } else {
            	memcachedHost = token.substring(0,index);
            	memcachedPort = token.substring(index+1);
            }
            instances.add(memcachedHost + ":" + memcachedPort);
        }
        return instances;
    }

    public void shutdown() {
    	super.shutdown();
        for(EVCacheClient client : memcachedInstances) {
        	shutdownClient(client);
        }
    }

    @Monitor(name="Instances", type=DataSourceType.COUNTER)
    public int getInstanceCount() {
        if(memcachedInstances.size() == 0) return 0;
        return memcachedInstances.get(0).getConnectionObserver().getActiveServerCount();
    }

    public List<String> getInstances() {
        final List<String> instanceList = new ArrayList<String>();
        for(EVCacheClient client : memcachedInstances) {
        	instanceList.add(client.toString());
        }
        return instanceList;
    }

    public void refreshPool() {
        try {
            refresh();
        } catch(Throwable t) {
            if(log.isDebugEnabled()) log.debug("Error Refreshing EVCache Instance list from MBean : " + getAppName() , t);
        }
    }

	public EVCacheClient getEVCacheClientExcludeZone(String zone) {
		return getEVCacheClient();
	}

	public EVCacheClient[] getAllEVCacheClients() {
		final EVCacheClient[] clientArr = new EVCacheClient[1];
		clientArr[0] = getEVCacheClient(); 
		return clientArr;
	}

	public void run() {
        try {
            refresh();
        } catch(Throwable t) {
            if(log.isDebugEnabled()) log.debug("Error Refreshing EVCache Instance list for " + getAppName() , t);
        }
	}

	public boolean supportsFallback() {
		return false;
	}
	
	public int getClusterSize() {
		return 1;
	}

	public String toString() {
		return "SimpleEVCacheClientPoolImpl ["  + super.toString()
				+ ", _serverList=" + _serverList + ", memcachedInstances=" + memcachedInstances 
				+ "]";
	}
}