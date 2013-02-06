package com.netflix.evcache.pool.standalone;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPool;

/**
 * A Simple EVCache Client pool given a list of memcached nodes
 *  
 * @author smadappa
 *
 */
public abstract class AbstractEVCacheClientPoolImpl implements Runnable, EVCacheClientPool {
    private static Logger log = LoggerFactory.getLogger(AbstractEVCacheClientPoolImpl.class);

	private String _appName;
    private DynamicIntProperty _readTimeout; //Timeout for readOperation
    private DynamicIntProperty _poolSize; //Number of MemcachedClients to each cluster
    private AtomicLong numOfOps = new AtomicLong(0);
	private boolean _shutdown = false;

    public AbstractEVCacheClientPoolImpl() {
    }
    
    public void init(final String appName) {
        this._appName = appName;
        this._readTimeout = DynamicPropertyFactory.getInstance().getIntProperty(appName + ".EVCacheClientPool.readTimeout", 100);
        this._poolSize = DynamicPropertyFactory.getInstance().getIntProperty(appName + ".EVCacheClientPool.poolSize", 1);

        if(log.isInfoEnabled()) log.info(new StringBuilder().append("EVCacheClientPool:init").append("\n\tAPP->").append(appName)
        		.append("\n\tReadTimeout->").append(_readTimeout).append("\n\tPoolSize->").append(_poolSize).toString());
        setupMonitoring();
    }

    protected void shutdownClient(EVCacheClient client) {
        if(!client.isShutdown()) {
            if(log.isDebugEnabled()) log.debug("Shutting down in Fallback -> AppName : " + _appName + "; client {" + client + "};" );
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


    /**
     * {@inheritDoc}
     */
    public void shutdown() {
        if(log.isInfoEnabled()) log.info("EVCacheClientPool for App : " + _appName + " is being shutdown.");
        _shutdown = true;
        setupMonitoring();
    }

    private void setupMonitoring() {
        try {
            final ObjectName mBeanName = ObjectName.getInstance("com.netflix.evcache:Group="+ _appName + ",SubGroup=pool");
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if(mbeanServer.isRegistered(mBeanName)) {
                if(log.isInfoEnabled()) log.info("MBEAN with name " + mBeanName + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanName);
            } 
            if(!_shutdown) mbeanServer.registerMBean(this, mBeanName);
        } catch (Exception e) { 
            if(log.isDebugEnabled()) log.debug("Exception",e);
        }
    }


	protected String getAppName() {
		return _appName;
	}

	protected DynamicIntProperty getReadTimeout() {
		return _readTimeout;
	}

	protected DynamicIntProperty getPoolSize() {
		return _poolSize;
	}
	
	protected boolean isShutdown() {
		return _shutdown;
	}

    protected AtomicLong getNumOfOps() {
		return numOfOps;
	}

	public EVCacheClient getEVCacheClientExcludeZone(String zone) {
		return getEVCacheClient();
	}

	public EVCacheClient[] getAllEVCacheClients() {
		final EVCacheClient[] clientArr = new EVCacheClient[1];
		clientArr[0] = getEVCacheClient(); 
		return clientArr;
	}

	public String toString() {
		return "appName=" + _appName
				+ ", readTimeout=" + _readTimeout + ", poolSize=" + _poolSize
				+ ", numberOfOperations=" + numOfOps + ", shutdown=" + _shutdown;
	}
}