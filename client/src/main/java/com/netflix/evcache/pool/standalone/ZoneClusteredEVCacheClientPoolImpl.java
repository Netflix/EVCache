package com.netflix.evcache.pool.standalone;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownServiceException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.util.ZoneFallbackIterator;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

public class ZoneClusteredEVCacheClientPoolImpl extends AbstractEVCacheClientPoolImpl implements ZoneClusteredEVCacheClientPoolImplMBean {
    private static Logger log = LoggerFactory.getLogger(ZoneClusteredEVCacheClientPoolImpl.class);
    private static final String GLOBAL = "GLOBAL";

    private String _zone;
    private DynamicStringProperty _zoneList; //List of servers
	private Map<String, DynamicStringProperty> hostsByZoneFPMap;
    private AtomicLong numberOfReadOps = new AtomicLong(0);
    private Map<String, List<ZoneClusteredEVCacheClientImpl>> memcachedInstancesByZone = new HashMap<String, List<ZoneClusteredEVCacheClientImpl>>();
    private Map<String, List<ZoneClusteredEVCacheClientImpl>> memcachedReadInstancesByZone = new ConcurrentHashMap<String, List<ZoneClusteredEVCacheClientImpl>>();
    private Map<String, List<ZoneClusteredEVCacheClientImpl>> memcachedWriteInstancesByZone = new ConcurrentHashMap<String, List<ZoneClusteredEVCacheClientImpl>>();
    private ZoneFallbackIterator memcachedFallbackReadInstances = new ZoneFallbackIterator(Collections.<String>emptySet());
    
    @SuppressWarnings("serial")
    private Map<String, DynamicBooleanProperty> writeOnlyFastPropertyMap = new ConcurrentHashMap<String, DynamicBooleanProperty>() {
        public DynamicBooleanProperty get(Object zone) {
            DynamicBooleanProperty isZoneInWriteOnlyMode = super.get(zone.toString());
            if(isZoneInWriteOnlyMode != null) return isZoneInWriteOnlyMode;

            isZoneInWriteOnlyMode = DynamicPropertyFactory.getInstance().getBooleanProperty(getAppName() + "." + zone.toString() + ".EVCacheClientPool.writeOnly", false);
            put((String)zone, isZoneInWriteOnlyMode);
            return isZoneInWriteOnlyMode;
        };
    };

    public ZoneClusteredEVCacheClientPoolImpl() { }

	public void init(String appName) {
		super.init(appName);
        final String ec2Zone = System.getenv("EC2_AVAILABILITY_ZONE");
        this._zone = (ec2Zone == null) ? GLOBAL : ec2Zone;
        this._zoneList = DynamicPropertyFactory.getInstance().getStringProperty(appName + ".EVCacheClientPool.zones", "");
        _zoneList.addCallback(this);
        
        hostsByZoneFPMap = new ConcurrentHashMap<String, DynamicStringProperty>();
        if(log.isInfoEnabled()) {
            final StringBuilder sb = new StringBuilder();
            sb.append("EVCacheClientPool:init");
            sb.append("\n\tAPP - ").append(getAppName());
            sb.append("\n\tLocalZone - ").append(_zone);
            sb.append("\n\tPoolSize - ").append(getPoolSize());
            sb.append("\n\tAllZones - ").append(_zoneList);
            sb.append("\n\tReadTimeout - ").append(getReadTimeout());
            log.info(sb.toString());
        }
        run();
	}

	public EVCacheClient getEVCacheClient() {
        if(memcachedReadInstancesByZone == null || memcachedReadInstancesByZone.isEmpty()) return null;

        try {
        	List<ZoneClusteredEVCacheClientImpl> clients = memcachedReadInstancesByZone.get(_zone);
        	if(clients == null) {
        		final String fallbackZone = memcachedFallbackReadInstances.next();
        		if(fallbackZone == null) return null;
        		clients = memcachedReadInstancesByZone.get(fallbackZone);
        	}

        	if(clients == null) return null;
        	if(clients.size() == 1) return clients.get(0);//Frequently used scenario
        	final long currentVal = numberOfReadOps.incrementAndGet();
        	final int index = (int)currentVal % clients.size();
        	return clients.get(index);
        } catch(Throwable t) {
            log.error("Exception trying to get an readable EVCache Instances for zone " + _zone, t);
            return null;
        }
    }

    public EVCacheClient getEVCacheClientExcludeZone(String zone) {
        if(memcachedReadInstancesByZone == null || memcachedReadInstancesByZone.isEmpty()) return null;
        if(zone == null || zone.length() == 0) return getEVCacheClient();

        try {
        	String fallbackZone = memcachedFallbackReadInstances.next();
        	if(fallbackZone.equals(zone)) fallbackZone = memcachedFallbackReadInstances.next(); //when there are only 2 zones and there are nofallbacks
        	if(fallbackZone == null || fallbackZone.equals(zone)) return null;
        	final List<ZoneClusteredEVCacheClientImpl> clients = memcachedReadInstancesByZone.get(fallbackZone);
        	if(clients == null) return null;
        	if(clients.size() == 1) return clients.get(0);//Frequently used case
        	final long currentVal = numberOfReadOps.incrementAndGet();
        	final int index = (int)currentVal % clients.size();
        	return clients.get(index);
        } catch(Throwable t) {
            log.error("Exception trying to get an readable EVCache Instances for zone " + _zone, t);
            return null;
        }
    }

	public EVCacheClient[] getAllEVCacheClients() {
        try {
            final EVCacheClient[] clientArr = new EVCacheClient[memcachedWriteInstancesByZone.size()];
            int i = 0;
            for(String zone : memcachedWriteInstancesByZone.keySet()) {
                final List<ZoneClusteredEVCacheClientImpl> clients = memcachedWriteInstancesByZone.get(zone);
            	if(clients.size() == 1) { //Frequently used scenario
            		clientArr[i++] = clients.get(0);
            	} else {
	                final long currentVal = numberOfReadOps.incrementAndGet();
	                final int index = (int)currentVal % clients.size();
	                clientArr[i++] = clients.get(index);
            	}
            }
            return clientArr;
        } catch(Throwable t) {
            log.error("Exception trying to get an array of writable EVCache Instances", t);
            return new EVCacheClient[0];
        }
    }

    private void refresh() throws IOException {
        refresh(false);
    }


    private boolean haveInstancesInZoneChanged(String zone, List<String> discoveredHostsInZone) {
        final List<ZoneClusteredEVCacheClientImpl> clients = memcachedInstancesByZone.get(zone);

        //1. if we have discovered instances in zone but not in our map then return immediately
        if(clients == null ) return true; 

        //2. Do a quick check based on count (active, inacative and discovered)
        for(EVCacheClient client : clients) {
            final int activeServerCount = client.getConnectionObserver().getActiveServerCount();
            final int inActiveServerCount = client.getConnectionObserver().getInActiveServerCount();
            final int sizeInDiscovery = discoveredHostsInZone.size();
            if(log.isDebugEnabled()) log.debug("\n\tApp : " + getAppName() + "\n\tActive Count : " + activeServerCount + "\n\tInactive Count : " + inActiveServerCount + "\n\tDiscovery Count : " + sizeInDiscovery);
            if(activeServerCount != sizeInDiscovery || inActiveServerCount > 0) {
                if(log.isInfoEnabled()) log.info("\n\t"+ getAppName() + " & " + zone + " experienced an issue.\n\tActive Server Count : " + activeServerCount);
                if(log.isInfoEnabled()) log.info("\n\tInActive Server Count : " + inActiveServerCount + "\n\tDiscovered Instances : " + sizeInDiscovery);

                //1. If a host is in discovery and we don't have an active or inActive connection to it then we will have to refresh our list. Typical case is we have replaced an existing node or expanded the cluster. 
                for(String instance : discoveredHostsInZone) {
                    final String hostname = instance.substring(0, instance.indexOf(':'));
                    if(!client.getConnectionObserver().getActiveServerInfo().containsKey(hostname) && !client.getConnectionObserver().getInActiveServerInfo().containsKey(hostname)) {
                        if(log.isDebugEnabled()) log.debug("AppName :" + getAppName() + "; Zone : " + zone + "; instance : " + instance + " not found and will shutdown the client and init it again.");
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private List<InetSocketAddress> getMemcachedSocketAddressList(final List<String> discoveredHostsInZone) {
        final List<InetSocketAddress> memcachedNodesInZone = new ArrayList<InetSocketAddress>();
        for(String hostAddress : discoveredHostsInZone) {
            final int colonIndex = hostAddress.lastIndexOf(':');
            final String hostName = hostAddress.substring(0, colonIndex);
            final String portNum = hostAddress.substring(colonIndex + 1);
            memcachedNodesInZone.add(new InetSocketAddress(hostName, Integer.parseInt(portNum)));
        }
        return memcachedNodesInZone;
    }


    private void shutdownClientsInZone(List<ZoneClusteredEVCacheClientImpl> clients) {
        if(clients == null || clients.isEmpty()) return;

        //Shutdown the old clients in 60 seconds, this will give ample time to cleanup anything pending in its queue
        for(EVCacheClient oldClient : clients) {
            try {
                final boolean obsRemoved = oldClient.removeConnectionObserver();
                if(log.isDebugEnabled()) log.debug("Connection observer removed " + obsRemoved);
                final boolean status = oldClient.shutdown(60, TimeUnit.SECONDS);
                if(log.isDebugEnabled()) log.debug("Shutting down -> Client {" + oldClient.toString() + "}; status : " + status);
            } catch(Exception ex) {
                log.error("Exception while shutting down the old Client", ex);
            }
        }
    }

    private void setupNewClientsByZone(String zone, List<ZoneClusteredEVCacheClientImpl> newClients) {
        final List<ZoneClusteredEVCacheClientImpl> currentClients = memcachedInstancesByZone.put(zone, newClients);

        //if the zone is in write only mode then remove it from the Map 
        final DynamicBooleanProperty isZoneInWriteOnlyMode = writeOnlyFastPropertyMap.get(zone);
        if(isZoneInWriteOnlyMode.get()) {
            memcachedReadInstancesByZone.remove(zone);
        } else {
            memcachedReadInstancesByZone.put(zone, newClients);
        }
        memcachedWriteInstancesByZone.put(zone, newClients);

        if(currentClients == null || currentClients.isEmpty()) return;

        //Now since we have replace the old instances shutdown all the old clients
        if(log.isDebugEnabled()) log.debug("Replaced an existing Pool for zone : " + zone + "; and app " + getAppName() + " ;\n\tOldClients : " + currentClients + ";\n\tNewClients : " + newClients); 
        for(EVCacheClient client : currentClients) {
            if(!client.isShutdown()) {
                if(log.isDebugEnabled()) log.debug("Shutting down in Fallback -> AppName : " + getAppName() + "; Zone : " + zone + "; client {" + client + "};" );
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

        //Paranoid Here. Even though we have shutdown the old clients do it again as we noticed issues while shutting down MemcachedNodes 
        shutdownClientsInZone (currentClients);
    }

    // Check if a zone has been moved to Write only. If so, remove the zone from the read map. 
    // Similarly if the zone has been moved to Read+Write from write only add it back to the read map.
    private void updateMemcachedReadInstancesByZone() {
        for(String zone : memcachedInstancesByZone.keySet()) {
            final DynamicBooleanProperty isZoneInWriteOnlyMode = writeOnlyFastPropertyMap.get(zone);
            if(isZoneInWriteOnlyMode.get()) {
                if(memcachedReadInstancesByZone.containsKey(zone)) {
                    memcachedReadInstancesByZone.remove(zone);
                }
            } else {
                if(!memcachedReadInstancesByZone.containsKey(zone)) {
                    memcachedReadInstancesByZone.put(zone, memcachedInstancesByZone.get(zone));
                }
            }
        }
        
        if(memcachedReadInstancesByZone.size() != memcachedFallbackReadInstances.getSize()) {
	        final ZoneFallbackIterator _memcachedFallbackReadInstances = new ZoneFallbackIterator(memcachedReadInstancesByZone.keySet());
	        memcachedFallbackReadInstances = _memcachedFallbackReadInstances;
        }
    }

    private synchronized void refresh(boolean force) throws IOException {
        try {
            final Map<String, List<String>> instances = discoverInstances();
            //if no instances are found then bail immediately.
            if(instances == null || instances.isEmpty()) return; 

            for(Entry<String, List<String>> zoneEntry : instances.entrySet()) {
                final String zone = zoneEntry.getKey();
                final List<String> discoverdInstanceInZone = zoneEntry.getValue();
                final List<String> discoveredHostsInZone = (discoverdInstanceInZone == null) ? Collections.<String>emptyList() : discoverdInstanceInZone;
                if(log.isDebugEnabled()) log.debug("\n\tApp : " + getAppName() + "\n\tZone : " + zone + "\n\tSize : " + discoveredHostsInZone.size() + "\n\tInstances in zone : " + discoveredHostsInZone);
                boolean instanceChangeInZone = force; 
                if(instanceChangeInZone) {
                    if(log.isWarnEnabled()) log.warn("FORCE REFRESH :: AppName :" + getAppName() + "; Zone : " + zone + "; Changed : " + instanceChangeInZone);
                } else {
                    instanceChangeInZone = haveInstancesInZoneChanged(zone, discoveredHostsInZone);
                    if(!instanceChangeInZone) {
                        //quick exit as everything looks fine. No new instances found and were inactive
                        if(log.isDebugEnabled()) log.debug("AppName :" + getAppName() + "; Zone : " + zone + "; Changed : " + instanceChangeInZone);
                        continue;
                    }
                }

                //Let us create a list of SocketAddress from the discovered instaces in zone
                final List<InetSocketAddress> memcachedSAInZone = getMemcachedSocketAddressList(discoveredHostsInZone);

                //now since there is a change with the instances in the zone. let us go ahead and create a new EVCacheClient with the new settings
                final int poolSize = getPoolSize().get();
                final List<ZoneClusteredEVCacheClientImpl> newClients = new ArrayList<ZoneClusteredEVCacheClientImpl>(poolSize);
                for(int i = 0; i < poolSize; i++) {
                    final int maxQueueSize = ConfigurationManager.getConfigInstance().getInt(getAppName() + ".max.queue.length", 16384);
                    final ZoneClusteredEVCacheClientImpl client = new ZoneClusteredEVCacheClientImpl(getAppName(), zone, i, maxQueueSize, getReadTimeout(), memcachedSAInZone);
                    newClients.add(client);
                    if(log.isDebugEnabled()) log.debug("AppName :" + getAppName() + "; Zone : " + zone + "; intit : client.getId() : " + client.getId());
                }
                setupNewClientsByZone(zone, newClients);
            }
            updateMemcachedReadInstancesByZone();
        } catch(Throwable t) {
            log.error("Exception while refreshing the Server list", t);
        }
	}
    
    private DynamicStringProperty getHostsFastProperty(String zone) {
         DynamicStringProperty hostsInZone = hostsByZoneFPMap.get(zone);
         if(hostsInZone != null) return hostsInZone;

         hostsInZone = DynamicPropertyFactory.getInstance().getStringProperty(getAppName() + "." + zone.toString() + ".EVCacheClientPool.hosts", "");
         hostsInZone.addCallback(this);
         hostsByZoneFPMap.put((String)zone, hostsInZone);
         return hostsInZone;
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
    private Map<String, List<String>> discoverInstances() throws IOException {
        if(isShutdown()) {
            return Collections.<String, List<String>>emptyMap();
        }

        final Map<String, List<String>> instancesByZoneMap = new HashMap<String, List<String>>();
        final StringTokenizer zoneListTokenizer = new StringTokenizer(_zoneList.get(), ",");

        /* Iterate all the discovered instances to find usable ones */
        while(zoneListTokenizer.hasMoreTokens()) {
            final String zone = zoneListTokenizer.nextToken();
            
            final DynamicStringProperty hostsInZoneFP = getHostsFastProperty(zone);
            final StringTokenizer hostListTokenizer = new StringTokenizer(hostsInZoneFP.get(), ",");
            
            while(hostListTokenizer.hasMoreTokens()) {
            	final String token = hostListTokenizer.nextToken(); 
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

	            if(!instancesByZoneMap.containsKey(zone)) instancesByZoneMap.put(zone, new ArrayList<String>());
	            final List<String> instancesInZone = instancesByZoneMap.get(zone); 
	            instancesInZone.add(memcachedHost + ":" + memcachedPort);
            }
        }
        return instancesByZoneMap;
    }

    public void run() {
        try {
            refresh();
        } catch(Throwable t) {
            if(log.isDebugEnabled()) log.debug("Error Refreshing EVCache Instance list for " + getAppName() , t);
        }
    }

    public void shutdown() {
        if(log.isInfoEnabled()) log.info("EVCacheClientPool for App : " + getAppName() + " and Zone : " + _zone + " is being shutdown.");
    	super.shutdown();
        for(List<ZoneClusteredEVCacheClientImpl> instancesInAZone : memcachedInstancesByZone.values()) {
            for(EVCacheClient client : instancesInAZone) {
            	shutdownClient(client);
            }
        }
    }

    @Monitor(name="Instances", type=DataSourceType.COUNTER)
    public int getInstanceCount() {
        int instances = 0;
        for(String zone : memcachedInstancesByZone.keySet()) {
            instances += memcachedInstancesByZone.get(zone).get(0).getConnectionObserver().getActiveServerCount();
        }
        return instances;
    }

    public Map<String, String> getInstancesByZone() {
        Map<String, String> instanceMap = new HashMap<String, String>();
        for(String zone : memcachedInstancesByZone.keySet()) {
            final List<ZoneClusteredEVCacheClientImpl> instanceList =  memcachedInstancesByZone.get(zone);
            instanceMap.put(zone, instanceList.toString());
        }
        return instanceMap; 
    }

    @Monitor(name="InstanceCountByZone", type=DataSourceType.INFORMATIONAL)
    public Map<String, Integer> getInstanceCountByZone() {
        final Map<String, Integer> instancesByZone = new HashMap<String, Integer>(memcachedInstancesByZone.size() * 2);
        for(String zone : memcachedInstancesByZone.keySet()) {
            instancesByZone.put(zone, Integer.valueOf(memcachedInstancesByZone.get(zone).get(0).getConnectionObserver().getActiveServerCount()));
        }
        return instancesByZone;
    }

    public Map<String, String> getReadZones() {
        final Map<String, String> instanceMap = new HashMap<String, String>();
        for(String key : memcachedReadInstancesByZone.keySet()){
            instanceMap.put(key, memcachedReadInstancesByZone.get(key).toString());
        }
        return instanceMap; 
    }

    @Monitor(name="ReadInstanceCountByZone", type=DataSourceType.INFORMATIONAL)
    public Map<String, Integer> getReadInstanceCountByZone() {
        final Map<String, Integer> instanceMap = new HashMap<String, Integer>();
        for(String key : memcachedReadInstancesByZone.keySet()){
            instanceMap.put(key, Integer.valueOf(memcachedReadInstancesByZone.get(key).get(0).getConnectionObserver().getActiveServerCount()));
        }
        return instanceMap; 
    }

    public Map<String, String> getWriteZones() {
        final Map<String, String> instanceMap = new HashMap<String, String>();
        for(String key : memcachedWriteInstancesByZone.keySet()){
            instanceMap.put(key, memcachedWriteInstancesByZone.get(key).toString());
        }
        return instanceMap; 
    }

    public Map<String, List<ZoneClusteredEVCacheClientImpl>> getAllInstancesByZone() {
        return Collections.unmodifiableMap(memcachedInstancesByZone);
    }

    @Monitor(name="WriteInstanceCountByZone", type=DataSourceType.INFORMATIONAL)
    public Map<String, Integer> getWriteInstanceCountByZone() {
        final Map<String, Integer> instanceMap = new HashMap<String, Integer>();
        for(String key : memcachedWriteInstancesByZone.keySet()){
            instanceMap.put(key, Integer.valueOf(memcachedWriteInstancesByZone.get(key).get(0).getConnectionObserver().getActiveServerCount()));
        }
        return instanceMap; 
    }
    
	public boolean supportsFallback() {
		return memcachedFallbackReadInstances.getSize() > 1;
	}

	public int getClusterSize() {
		return memcachedInstancesByZone.size();
	}

	public void refreshPool() {
        try {
            refresh(true);
        } catch(Throwable t) {
            if(log.isDebugEnabled()) log.debug("Error Refreshing EVCache Instance list from MBean : " + getAppName() , t);
        }
    }

	public String toString() {
		return "ZoneClusteredEVCacheClientPoolImpl [" + super.toString() 
				+ ", memcachedInstancesByZone=" + memcachedInstancesByZone 
				+ ", memcachedReadInstancesByZone=" + memcachedReadInstancesByZone
				+ ", memcachedWriteInstancesByZone=" + memcachedWriteInstancesByZone
				+ ", memcachedFallbackReadInstances=" + memcachedFallbackReadInstances 
				+ "]";
	}
 }