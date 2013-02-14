package com.netflix.evcache.pool.eureka;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.discovery.shared.Application;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPool;
import com.netflix.evcache.util.ZoneFallbackIterator;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

/**
 * An implementation of {@link EVCacheClientPool} based on Eureka.
 * @author smadappa
 *
 */
public class EVCacheClientPoolImpl implements Runnable, EVCacheClientPoolImplMBean, EVCacheClientPool {
    private static final String GLOBAL = "GLOBAL";

    private static Logger log = LoggerFactory.getLogger(EVCacheClientPoolImpl.class);

    private String _appName;
    private ScheduledThreadPoolExecutor _scheduler;
    private String _zone;
    private DynamicBooleanProperty _zoneAffinity;
    private DynamicIntProperty _poolSize; //Number of MemcachedClients to each cluster
    private DynamicIntProperty _readTimeout; //Timeout for readOperation
    private final AtomicLong numberOfReadOps = new AtomicLong(0);

    private boolean _shutdown = false;
    private final Map<String, List<EVCacheClientImpl>> memcachedInstancesByZone = new HashMap<String, List<EVCacheClientImpl>>();
    private final Map<String, List<EVCacheClientImpl>> memcachedReadInstancesByZone = new ConcurrentHashMap<String, List<EVCacheClientImpl>>();
    private final Map<String, List<EVCacheClientImpl>> memcachedWriteInstancesByZone = new ConcurrentHashMap<String, List<EVCacheClientImpl>>();
    private ZoneFallbackIterator memcachedFallbackReadInstances = new ZoneFallbackIterator(Collections.<String>emptySet());

    @SuppressWarnings("serial")
    private final Map<String, DynamicBooleanProperty> writeOnlyFastPropertyMap = new ConcurrentHashMap<String, DynamicBooleanProperty>() {
        @Override
        public DynamicBooleanProperty get(Object zone) {
            DynamicBooleanProperty isZoneInWriteOnlyMode = super.get(zone.toString());
            if (isZoneInWriteOnlyMode != null) {
                return isZoneInWriteOnlyMode;
            }

            isZoneInWriteOnlyMode = DynamicPropertyFactory.getInstance().getBooleanProperty(_appName + "." + zone.toString()
                    + ".EVCacheClientPool.writeOnly", false);
            put((String) zone, isZoneInWriteOnlyMode);
            return isZoneInWriteOnlyMode;
        };
    };

    /**
     * Default constructor.
     */
    public EVCacheClientPoolImpl() { }

    /**
     *  Initializes the pool.
     *  During initialization a scheduled executor is started which refreshes the list of servers for the given apppName from
     *  Eureka every minute. This ensures if a server is added, removed or replaced then the pool is automatically reconfigured.
     *
     *  @param appName - the name of the EVCache app.
     */
    @Override
    public void init(String appName) {
        this._appName = appName;
        final String ec2Zone = System.getenv("EC2_AVAILABILITY_ZONE");
        this._zone = (ec2Zone == null) ? GLOBAL : ec2Zone;
        this._zoneAffinity = DynamicPropertyFactory.getInstance().getBooleanProperty(appName + ".EVCacheClientPool.zoneAffinity", true);
        this._poolSize = DynamicPropertyFactory.getInstance().getIntProperty(appName + ".EVCacheClientPool.poolSize", 1);
        this._readTimeout = DynamicPropertyFactory.getInstance().getIntProperty(appName + ".EVCacheClientPool.readTimeout", 100);

        if (log.isInfoEnabled()) {
            final StringBuilder sbf = new StringBuilder();
            sbf.append("EVCacheClientPool:init").append("\n\tAPP - ").append(appName).append("\n\tZone - ").append(_zone);
            sbf.append("\n\tZoneAffinity - ").append(_zoneAffinity).append("\n\tPoolSize - ").append(_poolSize);
            sbf.append("\n\tReadTimeout - ").append(_readTimeout);
            log.info(sbf.toString());
        }
        _scheduler = new ScheduledThreadPoolExecutor(1);
        _scheduler.scheduleWithFixedDelay(this, 0, 60, TimeUnit.SECONDS);
        setupMonitoring();
    }

    /**
     * Returns an {@link EVCacheClient} for the local zone. If one is not found for the local zone then an instance
     * from a any zone is returned. If there are none <code>null</code> is returned.
     *
     * @return Instance in the local zone (default) or other zone (fallback) is returned. If the pool is empty then null is returned.
     */
    @Override
    public EVCacheClient getEVCacheClient() {
        if (memcachedReadInstancesByZone.isEmpty()) {
            return null;
        }

        try {
            if (_zoneAffinity.get()) {
                List<EVCacheClientImpl> clients = memcachedReadInstancesByZone.get(_zone);
                if (clients == null) {
                    final String fallbackZone = memcachedFallbackReadInstances.next();
                    if (fallbackZone == null) {
                        return null;
                    }
                    clients = memcachedReadInstancesByZone.get(fallbackZone);
                }

                if (clients == null) {
                    return null;
                }
                if (clients.size() == 1) {
                    return clients.get(0); //Frequently used scenario
                }
                final long currentVal = numberOfReadOps.incrementAndGet();
                final int index = (int) currentVal % clients.size();
                return clients.get(index);
            } else {
                final List<EVCacheClientImpl> clients = memcachedReadInstancesByZone.get(GLOBAL);
                if (clients == null) {
                    return null;
                }
                if (clients.size() == 1) {
                    return clients.get(0); //Frequently used scenario
                }
                final long currentVal = numberOfReadOps.incrementAndGet();
                final int index = (int) currentVal % clients.size();
                return clients.get(index);
            }
        } catch (Throwable t) {
            log.error("Exception trying to get an readable EVCache Instances for zone " + _zone, t);
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EVCacheClient getEVCacheClientExcludeZone(String zone) {
        if (memcachedReadInstancesByZone.isEmpty()) {
            return null;
        }
        if (zone == null || zone.length() == 0) {
            return getEVCacheClient();
        }

        try {
            if (_zoneAffinity.get()) {
                String fallbackZone = memcachedFallbackReadInstances.next(zone);
                if (fallbackZone == null || fallbackZone.equals(zone)) {
                    return null;
                }
                final List<EVCacheClientImpl> clients = memcachedReadInstancesByZone.get(fallbackZone);
                if (clients == null) {
                    return null;
                }
                if (clients.size() == 1) {
                    return clients.get(0); //Frequently used case
                }
                final long currentVal = numberOfReadOps.incrementAndGet();
                final int index = (int) currentVal % clients.size();
                return clients.get(index);
            }
            return null;
        } catch (Throwable t) {
            log.error("Exception trying to get an readable EVCache Instances for zone " + _zone, t);
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EVCacheClient[] getAllEVCacheClients() {
        try {
            if (_zoneAffinity.get()) {
                final EVCacheClient[] clientArr = new EVCacheClient[memcachedWriteInstancesByZone.size()];
                int i = 0;
                for (String zone : memcachedWriteInstancesByZone.keySet()) {
                    final List<EVCacheClientImpl> clients = memcachedWriteInstancesByZone.get(zone);
                    final long currentVal = numberOfReadOps.incrementAndGet();
                    final int index = (int) currentVal % clients.size();
                    clientArr[i++] = clients.get(index);
                }
                return clientArr;
            } else {
                final EVCacheClient[] clientArr = new EVCacheClient[1];
                final List<EVCacheClientImpl> clients = memcachedWriteInstancesByZone.get(GLOBAL);
                if (clients == null) {
                    return new EVCacheClient[0]; //For GLOBAL there will be only one zone so hard coding it to 0
                }
                clientArr[0] = clients.get(0);
                return clientArr;
            }
        } catch (Throwable t) {
            log.error("Exception trying to get an array of writable EVCache Instances", t);
            return null;
        }
    }

    private void refresh() throws IOException {
        refresh(false);
    }


    private boolean haveInstancesInZoneChanged(String zone, List<String> discoveredHostsInZone) {
        final List<EVCacheClientImpl> clients = memcachedInstancesByZone.get(zone);

        //1. if we have discovered instances in zone but not in our map then return immediately
        if (clients == null) {
            return true;
        }

        //2. Do a quick check based on count (active, inacative and discovered)
        for (EVCacheClient client : clients) {
            final int activeServerCount = client.getConnectionObserver().getActiveServerCount();
            final int inActiveServerCount = client.getConnectionObserver().getInActiveServerCount();
            final int sizeInDiscovery = discoveredHostsInZone.size();
            if (log.isDebugEnabled()) {
                log.debug("\n\tApp : " + _appName + "\n\tActive Count : " + activeServerCount
                        + "\n\tInactive Count : " + inActiveServerCount + "\n\tDiscovery Count : " + sizeInDiscovery);
            }
            if (activeServerCount != sizeInDiscovery || inActiveServerCount > 0) {
                if (log.isInfoEnabled()) {
                    log.info("\n\t" + _appName + " & " + zone + " experienced an issue.\n\tActive Server Count : "
                            + activeServerCount);
                }
                if (log.isInfoEnabled()) {
                    log.info("\n\tInActive Server Count : " + inActiveServerCount + "\n\tDiscovered Instances : "
                            + sizeInDiscovery);
                }

                //1. If a host is in discovery and we don't have an active or inActive connection to it then we will have to refresh our list.
                //Typical case is we have replaced an existing node or expanded the cluster.
                for (String instance : discoveredHostsInZone) {
                    final String hostname = instance.substring(0, instance.indexOf(':'));
                    if (!client.getConnectionObserver().getActiveServerInfo().containsKey(hostname)
                            && !client.getConnectionObserver().getInActiveServerInfo().containsKey(hostname)) {
                        if (log.isDebugEnabled()) {
                            log.debug("AppName :" + _appName + "; Zone : " + zone + "; instance : "
                                    + instance + " not found and will shutdown the client and init it again.");
                        }
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private List<InetSocketAddress> getMemcachedSocketAddressList(final List<String> discoveredHostsInZone) {
        final List<InetSocketAddress> memcachedNodesInZone = new ArrayList<InetSocketAddress>();
        for (String hostAddress : discoveredHostsInZone) {
            final int colonIndex = hostAddress.lastIndexOf(':');
            final String hostName = hostAddress.substring(0, colonIndex);
            final String portNum = hostAddress.substring(colonIndex + 1);
            memcachedNodesInZone.add(new InetSocketAddress(hostName, Integer.parseInt(portNum)));
        }
        return memcachedNodesInZone;
    }


    private void shutdownClientsInZone(List<EVCacheClientImpl> clients) {
        if (clients == null || clients.isEmpty()) {
            return;
        }

        //Shutdown the old clients in 60 seconds, this will give ample time to cleanup anything pending in its queue
        for (EVCacheClient oldClient : clients) {
            try {
                final boolean obsRemoved = oldClient.removeConnectionObserver();
                if (log.isDebugEnabled()) {
                    log.debug("Connection observer removed " + obsRemoved);
                }
                final boolean status = oldClient.shutdown(60, TimeUnit.SECONDS);
                if (log.isDebugEnabled()) {
                    log.debug("Shutting down -> Client {" + oldClient.toString() + "}; status : " + status);
                }
            } catch (Exception ex) {
                log.error("Exception while shutting down the old Client", ex);
            }
        }
    }

    private void setupNewClientsByZone(String zone, List<EVCacheClientImpl> newClients) {
        final List<EVCacheClientImpl> currentClients = memcachedInstancesByZone.put(zone, newClients);

        //if the zone is in write only mode then remove it from the Map
        final DynamicBooleanProperty isZoneInWriteOnlyMode = writeOnlyFastPropertyMap.get(zone);
        if (isZoneInWriteOnlyMode.get()) {
            memcachedReadInstancesByZone.remove(zone);
        } else {
            memcachedReadInstancesByZone.put(zone, newClients);
        }
        memcachedWriteInstancesByZone.put(zone, newClients);

        if (currentClients == null || currentClients.isEmpty()) {
            return;
        }

        //Now since we have replace the old instances shutdown all the old clients
        if (log.isDebugEnabled()) {
            log.debug("Replaced an existing Pool for zone : " + zone + "; and app " + _appName
                    + " ;\n\tOldClients : " + currentClients + ";\n\tNewClients : " + newClients);
        }
        for (EVCacheClient client : currentClients) {
            if (!client.isShutdown()) {
                if (log.isDebugEnabled()) {
                    log.debug("Shutting down in Fallback -> AppName : " + _appName
                            + "; Zone : " + zone + "; client {" + client + "};");
                }
                try {
                    if (client.getConnectionObserver() != null) {
                        final boolean obsRemoved = client.removeConnectionObserver();
                        if (log.isDebugEnabled()) {
                            log.debug("Connection observer removed " + obsRemoved);
                        }
                    }
                    final boolean status = client.shutdown(60, TimeUnit.SECONDS);
                    if (log.isDebugEnabled()) {
                        log.debug("Shutting down {" + client + "} ; status : " + status);
                    }
                } catch (Exception ex) {
                    log.error("Exception while shutting down the old Client", ex);
                }
            }
        }

        //Paranoid Here. Even though we have shutdown the old clients do it again as we noticed issues while shutting down MemcachedNodes
        shutdownClientsInZone(currentClients);
    }

    // Check if a zone has been moved to Write only. If so, remove the zone from the read map.
    // Similarly if the zone has been moved to Read+Write from write only add it back to the read map.
    private void updateMemcachedReadInstancesByZone() {
        for (String zone : memcachedInstancesByZone.keySet()) {
            final DynamicBooleanProperty isZoneInWriteOnlyMode = writeOnlyFastPropertyMap.get(zone);
            if (isZoneInWriteOnlyMode.get()) {
                if (memcachedReadInstancesByZone.containsKey(zone)) {
                    memcachedReadInstancesByZone.remove(zone);
                }
            } else {
                if (!memcachedReadInstancesByZone.containsKey(zone)) {
                    memcachedReadInstancesByZone.put(zone, memcachedInstancesByZone.get(zone));
                }
            }
        }

        if (memcachedReadInstancesByZone.size() != memcachedFallbackReadInstances.getSize()) {
            final ZoneFallbackIterator _memcachedFallbackReadInstances = new ZoneFallbackIterator(memcachedReadInstancesByZone.keySet());
            memcachedFallbackReadInstances = _memcachedFallbackReadInstances;
        }
    }


    private synchronized void refresh(boolean force) throws IOException {
        try {
            final Map<String, List<String>> instances = discoverInstances();
            //if no instances are found then bail immediately.
            if (instances == null || instances.isEmpty()) {
                return;
            }

            for (Entry<String, List<String>> zoneEntry : instances.entrySet()) {
                final String zone = zoneEntry.getKey();
                final List<String> discoverdInstanceInZone = zoneEntry.getValue();
                final List<String> discoveredHostsInZone = (discoverdInstanceInZone == null)
                        ? Collections.<String>emptyList() : discoverdInstanceInZone;
                if (log.isDebugEnabled()) {
                    log.debug("\n\tApp : " + _appName + "\n\tZone : " + zone + "\n\tSize : " + discoveredHostsInZone.size()
                            + "\n\tInstances in zone : " + discoveredHostsInZone);
                }
                boolean instanceChangeInZone = force;
                if (instanceChangeInZone) {
                    if (log.isWarnEnabled()) {
                        log.warn("FORCE REFRESH :: AppName :" + _appName + "; Zone : " + zone + "; Changed : " + instanceChangeInZone);
                    }
                } else {
                    instanceChangeInZone = haveInstancesInZoneChanged(zone, discoveredHostsInZone);
                    if (!instanceChangeInZone) {
                        //quick exit as everything looks fine. No new instances found and were inactive
                        if (log.isDebugEnabled()) {
                            log.debug("AppName :" + _appName + "; Zone : " + zone + "; Changed : " + instanceChangeInZone);
                        }
                        continue;
                    }
                }

                //Let us create a list of SocketAddress from the discovered instaces in zone
                final List<InetSocketAddress> memcachedSAInZone = getMemcachedSocketAddressList(discoveredHostsInZone);

                //now since there is a change with the instances in the zone. let us go ahead and create a new EVCacheClient with the new settings
                final int poolSize = _poolSize.get();
                final List<EVCacheClientImpl> newClients = new ArrayList<EVCacheClientImpl>(poolSize);
                for (int i = 0; i < poolSize; i++) {
                    final int maxQueueSize = ConfigurationManager.getConfigInstance().getInt(_appName + ".max.queue.length", 16384);
                    final EVCacheClientImpl client = new EVCacheClientImpl(_appName, zone, i, maxQueueSize, _readTimeout, memcachedSAInZone);
                    newClients.add(client);
                    if (log.isDebugEnabled()) {
                        log.debug("AppName :" + _appName + "; Zone : " + zone + "; intit : client.getId() : " + client.getId());
                    }
                }
                setupNewClientsByZone(zone, newClients);
            }
            updateMemcachedReadInstancesByZone();
        } catch (Throwable t) {
            log.error("Exception while refreshing the Server list", t);
        }
    }

    /**
     * Discover memcached instances suitable for our use from the Discovery Service.
     *
     * @throws IllegalStateException if an error occurred in the Discovery
     *    service
     */
    private Map<String, List<String>> discoverInstances() throws IOException {
        if (_shutdown || ApplicationInfoManager.getInstance().getInfo().getStatus() == InstanceStatus.DOWN) {
            return Collections.<String, List<String>>emptyMap();
        }

        /* Get a list of EVCACHE instances from the DiscoveryManager */
        final Application app = DiscoveryManager.getInstance().getDiscoveryClient().getApplication(_appName);
        if (app == null) {
            return Collections.<String, List<String>>emptyMap();
        }

        final List<InstanceInfo> appInstances = app.getInstances();
        final Map<String, List<String>> instancesSpecific = new HashMap<String, List<String>>();

        /* Iterate all the discovered instances to find usable ones */
        for (InstanceInfo iInfo : appInstances) {
            final DataCenterInfo dcInfo = iInfo.getDataCenterInfo();
            final Map<String, String> metaInfo = iInfo.getMetadata();

            /* Only AWS instances are usable; bypass all others */
            if (DataCenterInfo.Name.Amazon != dcInfo.getName()) {
                if (log.isErrorEnabled()) {
                    log.error("This is not a AmazonDataCenter. Cannot proceed. DataCenterInfo : " + dcInfo);
                }
                continue;
            }

            /* Don't try to use downed instances */
            if (InstanceStatus.UP != iInfo.getStatus()) {
                if (log.isWarnEnabled()) {
                    log.warn("The Status of the instance in Discovery is not UP. InstanceInfo : " + iInfo);
                }
                continue;
            }

            final AmazonInfo amznInfo = (AmazonInfo) dcInfo; //We checked above if this instance is Amazon so no need to do a instanceof check
            final String zone = (_zoneAffinity.get()) ? amznInfo.get(AmazonInfo.MetaDataKey.availabilityZone) : GLOBAL;
            final String evcachePort = (metaInfo.containsKey("evcache.port")) ? metaInfo.get("evcache.port") : "11211";
            final String hostName = amznInfo.get(AmazonInfo.MetaDataKey.publicHostname);
            if (hostName == null) {
                if (log.isErrorEnabled()) {
                    log.error("The public hostnanme is null, will not be able to add this host to the evcache cluster. AmazonInfo : " + amznInfo);
                }
                continue;
            }

            if (!instancesSpecific.containsKey(zone)) {
                instancesSpecific.put(zone, new ArrayList<String>());
            }
            final List<String> instancesInZone = instancesSpecific.get(zone);
            instancesInZone.add(hostName + ":" + evcachePort);
        }
        return instancesSpecific;
    }

    @Override
    public void run() {
        try {
            refresh();
        } catch (Throwable t) {
            if (log.isDebugEnabled()) {
                log.debug("Error Refreshing EVCache Instance list for " + _appName , t);
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    public void shutdown() {
        if (log.isInfoEnabled()) {
            log.info("EVCacheClientPool for App : " + _appName + " and Zone : " + _zone + " is being shutdown.");
        }
        _shutdown = true;
        _scheduler.shutdown();
        for (List<EVCacheClientImpl> instancesInAZone : memcachedInstancesByZone.values()) {
            for (EVCacheClient client : instancesInAZone) {
                client.shutdown(30, TimeUnit.SECONDS);
                client.getConnectionObserver().shutdown();
            }
        }
        setupMonitoring();
    }

    private void setupMonitoring() {
        try {
            final ObjectName mBeanName = ObjectName.getInstance("com.netflix.evcache:Group=" + _appName + ",SubGroup=pool");
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mbeanServer.isRegistered(mBeanName)) {
                if (log.isInfoEnabled()) {
                    log.info("MBEAN with name " + mBeanName + " has been registered. Will unregister the previous instance and register a new one.");
                }
                mbeanServer.unregisterMBean(mBeanName);
            }
            if (!_shutdown) {
                mbeanServer.registerMBean(this, mBeanName);
            }
        } catch (Exception e) {
            if (log.isDebugEnabled())  log.debug("Exception", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Monitor(name = "Instances", type = DataSourceType.COUNTER)
    public int getInstanceCount() {
        int instances = 0;
        for (String zone : memcachedInstancesByZone.keySet()) {
            instances += memcachedInstancesByZone.get(zone).get(0).getConnectionObserver().getActiveServerCount();
        }
        return instances;
    }

    /**
     * {@inheritDoc}
     */
    public Map<String, String> getInstancesByZone() {
        Map<String, String> instanceMap = new HashMap<String, String>();
        for (String zone : memcachedInstancesByZone.keySet()) {
            final List<EVCacheClientImpl> instanceList =  memcachedInstancesByZone.get(zone);
            instanceMap.put(zone, instanceList.toString());
        }
        return instanceMap;
    }

    /**
     * {@inheritDoc}
     */
    @Monitor(name = "InstanceCountByZone", type = DataSourceType.INFORMATIONAL)
    public Map<String, Integer> getInstanceCountByZone() {
        final Map<String, Integer> instancesByZone = new HashMap<String, Integer>(memcachedInstancesByZone.size() * 2);
        for (String zone : memcachedInstancesByZone.keySet()) {
            instancesByZone.put(zone, Integer.valueOf(memcachedInstancesByZone.get(zone).get(0).getConnectionObserver().getActiveServerCount()));
        }
        return instancesByZone;
    }

    /**
     * {@inheritDoc}
     */
    public Map<String, String> getReadZones() {
        final Map<String, String> instanceMap = new HashMap<String, String>();
        for (String key : memcachedReadInstancesByZone.keySet()) {
            instanceMap.put(key, memcachedReadInstancesByZone.get(key).toString());
        }
        return instanceMap;
    }

    /**
     * {@inheritDoc}
     */
    @Monitor(name = "ReadInstanceCountByZone", type = DataSourceType.INFORMATIONAL)
    public Map<String, Integer> getReadInstanceCountByZone() {
        final Map<String, Integer> instanceMap = new HashMap<String, Integer>();
        for (String key : memcachedReadInstancesByZone.keySet()) {
            instanceMap.put(key, Integer.valueOf(memcachedReadInstancesByZone.get(key).get(0).getConnectionObserver().getActiveServerCount()));
        }
        return instanceMap;
    }

    @Override
    public Map<String, String> getWriteZones() {
        final Map<String, String> instanceMap = new HashMap<String, String>();
        for (String key : memcachedWriteInstancesByZone.keySet()) {
            instanceMap.put(key, memcachedWriteInstancesByZone.get(key).toString());
        }
        return instanceMap;
    }

    /**
     * {@inheritDoc}
     */
    public Map<String, List<EVCacheClientImpl>> getAllInstancesByZone() {
        return Collections.unmodifiableMap(memcachedInstancesByZone);
    }

    /**
     * {@inheritDoc}
     */
    @Monitor(name = "WriteInstanceCountByZone", type = DataSourceType.INFORMATIONAL)
    public Map<String, Integer> getWriteInstanceCountByZone() {
        final Map<String, Integer> instanceMap = new HashMap<String, Integer>();
        for (String key : memcachedWriteInstancesByZone.keySet()) {
            instanceMap.put(key, Integer.valueOf(memcachedWriteInstancesByZone.get(key).get(0).getConnectionObserver().getActiveServerCount()));
        }
        return instanceMap;
    }

    /**
     * {@inheritDoc}
     */
    public void refreshPool() {
        try {
            refresh(true);
        } catch (Throwable t) {
            if (log.isDebugEnabled()) log.debug("Error Refreshing EVCache Instance list from MBean : " + _appName , t);
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean supportsFallback() {
        return memcachedFallbackReadInstances.getSize() > 1;
    }


    /**
     * {@inheritDoc}
     */
    public int getClusterSize() {
        return memcachedInstancesByZone.size();
    }
}
