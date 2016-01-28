package com.netflix.evcache.pool;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ChainedDynamicProperty;
import com.netflix.config.ChainedDynamicProperty.BooleanProperty;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicStringSetProperty;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.metrics.Operation;
import com.netflix.evcache.pool.observer.EVCacheConnectionObserver;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.evcache.util.ServerGroupCircularIterator;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.tag.BasicTagList;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.protocol.binary.EVCacheNodeImpl;

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", "REC_CATCH_EXCEPTION", "MDM_THREAD_YIELD"})
public class EVCacheClientPool implements Runnable, EVCacheClientPoolMBean {

    private static Logger log = LoggerFactory.getLogger(EVCacheClientPool.class);

    private final String _appName;
    private final String _zone;
    private final EVCacheClientPoolManager manager;
    private ServerGroupCircularIterator localServerGroupIterator = null;
    private final DynamicIntProperty _poolSize; //Number of MemcachedClients to each cluster
    private final ChainedDynamicProperty.IntProperty _readTimeout; //Timeout for readOperation
    private final ChainedDynamicProperty.IntProperty _bulkReadTimeout; //Timeout for readOperation
    public static final String DEFAULT_PORT = "11211";

    private final DynamicBooleanProperty enableServerGroup, _retryAcrossAllReplicas;
    private final DynamicBooleanProperty enableDynamicWriteOnlyMode;
//    private final DynamicBooleanProperty throttleRequest;
//    private final DynamicBooleanProperty disableWrites;
//    private final DynamicIntProperty throttlingPercent;
//    private final DynamicIntProperty throttlingTime;
//    private final AtomicLong throttlingCount;
    private long lastReconcileTime = 0;

    private final DynamicIntProperty logOperations;
    private final DynamicStringSetProperty logOperationCalls;


    /* Experimental Properties - Start */
    private final DynamicIntProperty _opQueueMaxBlockTime; //Timeout for adding an operation
    private final DynamicIntProperty _operationTimeout;//Timeout for write operation
    private final DynamicIntProperty  _maxReadQueueSize;
    private final DynamicIntProperty reconcileInterval;

    private final DynamicBooleanProperty _pingServers;
    /* Experimental Properties - End */

    @SuppressWarnings("serial")
    private final Map<ServerGroup, BooleanProperty> writeOnlyFastPropertyMap = new ConcurrentHashMap<ServerGroup, BooleanProperty>() {
        @Override
        public BooleanProperty get(Object _rSet) {
            final ServerGroup rSet = ServerGroup.class.cast(_rSet);
            BooleanProperty isServerGroupInWriteOnlyMode = super.get(rSet);
            if (isServerGroupInWriteOnlyMode != null) return isServerGroupInWriteOnlyMode;

            isServerGroupInWriteOnlyMode = new BooleanProperty(_appName + "." + rSet.getName() + ".EVCacheClientPool.writeOnly",
                    new ChainedDynamicProperty.DynamicBooleanPropertyThatSupportsNull(_appName + "." + rSet.getZone() + ".EVCacheClientPool.writeOnly", Boolean.FALSE));
            put(rSet, isServerGroupInWriteOnlyMode);
            return isServerGroupInWriteOnlyMode;
        };
    };

    private final AtomicLong numberOfModOps = new AtomicLong(0);

    private boolean _shutdown = false;
    private Map<ServerGroup, List<EVCacheClient>> memcachedInstancesByServerGroup = new ConcurrentHashMap<ServerGroup, List<EVCacheClient>>();
    private Map<ServerGroup, List<EVCacheClient>> memcachedReadInstancesByServerGroup = new ConcurrentHashMap<ServerGroup, List<EVCacheClient>>();
    private Map<ServerGroup, List<EVCacheClient>> memcachedWriteInstancesByServerGroup = Collections.synchronizedSortedMap(new TreeMap<ServerGroup, List<EVCacheClient>>());
    private Map<String, ServerGroupCircularIterator> readServerGroupByZone = new ConcurrentHashMap<String, ServerGroupCircularIterator>();
    private ServerGroupCircularIterator memcachedFallbackReadInstances = new ServerGroupCircularIterator(Collections.<ServerGroup> emptySet());
    private final EVCacheNodeList provider; 

    EVCacheClientPool(final String appName, final EVCacheNodeList provider, final EVCacheClientPoolManager manager) {
        this._appName = appName;
        this.provider = provider;
        this.manager = manager;

        String ec2Zone = System.getenv("EC2_AVAILABILITY_ZONE");
        if(ec2Zone == null) ec2Zone = System.getProperty("EC2_AVAILABILITY_ZONE");
        this._zone = (ec2Zone == null) ? "GLOBAL" : ec2Zone;
        final EVCacheConfig config = EVCacheConfig.getInstance();

        this._poolSize = config.getDynamicIntProperty(appName + ".EVCacheClientPool.poolSize", 1);
        this._poolSize.addCallback(new Runnable() {
            public void run() {
                clearState();
                refreshPool();
            }
        });
        this._readTimeout = new ChainedDynamicProperty.IntProperty(appName + ".EVCacheClientPool.readTimeout", EVCacheClientPoolManager.getDefaultReadTimeout());
        this._readTimeout.addCallback(new Runnable() {
            public void run() {
                clearState();
                refreshPool();
            }
        });
        this._bulkReadTimeout = new ChainedDynamicProperty.IntProperty(appName + ".EVCacheClientPool.bulkReadTimeout", _readTimeout);
        this._bulkReadTimeout.addCallback(new Runnable() {
            public void run() {
                clearState();
                refreshPool();
            }
        });
        this.enableDynamicWriteOnlyMode = config.getDynamicBooleanProperty("EVCacheClientPool.enable.dynamic.writeonly", true);

        // TODO : get rid of this once all the clients have upgraded to this
        // jar. This is only for backward compatibility
        this.enableServerGroup = config.getDynamicBooleanProperty(appName + ".enable.replica.set", true);
        this.enableServerGroup.addCallback(new Runnable() {
            public void run() {
                clearState();
                refreshPool();
            }
        });

        this._opQueueMaxBlockTime = config.getDynamicIntProperty(appName + ".operation.QueueMaxBlockTime", 10);
        this._opQueueMaxBlockTime.addCallback(new Runnable() {
            public void run() {
                clearState();
                refreshPool();
            }
        });
        this._operationTimeout = config.getDynamicIntProperty(appName + ".operation.timeout", 2500);
        this._operationTimeout.addCallback(new Runnable() {
            public void run() {
                clearState();
                refreshPool();
            }
        });
        this._maxReadQueueSize = config.getDynamicIntProperty(appName + ".max.read.queue.length", 5);
        this._retryAcrossAllReplicas = config.getDynamicBooleanProperty(_appName + ".retry.all.copies", Boolean.FALSE);

        this.logOperations = config.getDynamicIntProperty(appName + ".log.operation", 0);
        this.reconcileInterval= config.getDynamicIntProperty(appName + ".reconcile.interval", 600000);
        this.logOperationCalls = new DynamicStringSetProperty(appName + ".log.operation.calls", "SET,DEL,GMISS,TMISS,BMISS");

        final Map<String, String> map = new HashMap<String, String>();
        map.put("APP", _appName);

        this._pingServers = config.getDynamicBooleanProperty(appName + ".ping.servers", false);
        setupMonitoring();
        refreshPool();
        if (log.isInfoEnabled()) log.info(toString());
    }

    private void clearState() {
        cleanupMemcachedInstances(true);
        memcachedInstancesByServerGroup.clear();
        memcachedReadInstancesByServerGroup.clear();
        memcachedWriteInstancesByServerGroup.clear();
        readServerGroupByZone.clear();
        memcachedFallbackReadInstances = new ServerGroupCircularIterator(Collections.<ServerGroup> emptySet());
    }

    public EVCacheClient getEVCacheClientForRead() {
        if (memcachedReadInstancesByServerGroup == null || memcachedReadInstancesByServerGroup.isEmpty()) {
            if(log.isDebugEnabled()) log.debug("memcachedReadInstancesByServerGroup : " + memcachedReadInstancesByServerGroup);
            return null;
        }

        try {
            List<EVCacheClient> clients = null;
            if (localServerGroupIterator != null) {
                clients = memcachedReadInstancesByServerGroup.get(localServerGroupIterator.next());
            }

            if(clients == null) {
                final ServerGroup fallbackServerGroup = memcachedFallbackReadInstances.next();
                if (fallbackServerGroup == null) {
                    if(log.isDebugEnabled()) log.debug("fallbackServerGroup is null.");
                    return null;
                }
                clients = memcachedReadInstancesByServerGroup.get(fallbackServerGroup);
            }
            return selectClient(clients);
        } catch(Throwable t) {
            log.error("Exception trying to get an readable EVCache Instances for zone " + _zone, t);
            return null;
        }
    }

    private EVCacheClient selectClient(List<EVCacheClient> clients) {
        if (clients == null) {
            if(log.isDebugEnabled()) log.debug("clients is null returning null!!!");
            return null;
        }
        if (clients.size() == 1) {
            return clients.get(0); //Frequently used scenario
        }
        final long currentVal = numberOfModOps.incrementAndGet();
        final int index = (int) currentVal % clients.size();
        return clients.get(index);
    }

    public EVCacheClient getEVCacheClientForReadExclude(ServerGroup rsetUsed) {
        if (memcachedReadInstancesByServerGroup == null || memcachedReadInstancesByServerGroup.isEmpty()) return null;
        try {
            ServerGroup fallbackServerGroup = memcachedFallbackReadInstances.next(rsetUsed);
            if (fallbackServerGroup == null || fallbackServerGroup.equals(rsetUsed)) {
                return null;
            }
            final List<EVCacheClient> clients = memcachedReadInstancesByServerGroup.get(fallbackServerGroup);
            return selectClient(clients);
        } catch(Throwable t) {
            log.error("Exception trying to get an readable EVCache Instances for zone " + rsetUsed, t);
            return null;
        }
    }

    public List<EVCacheClient> getEVCacheClientsForReadExcluding(ServerGroup serverGroupToExclude) {
        if (memcachedReadInstancesByServerGroup == null || memcachedReadInstancesByServerGroup.isEmpty()) return Collections.<EVCacheClient> emptyList();
        try {
            if (_retryAcrossAllReplicas.get()) {
                List<EVCacheClient> clients = new ArrayList<EVCacheClient>(memcachedReadInstancesByServerGroup.size() - 1);
                for (Iterator<ServerGroup> itr = memcachedReadInstancesByServerGroup.keySet().iterator(); itr.hasNext();) {
                    final ServerGroup serverGroup = itr.next();
                    if (serverGroup.equals(serverGroupToExclude)) continue;

                    final List<EVCacheClient> clientList = memcachedReadInstancesByServerGroup.get(serverGroup);
                    final EVCacheClient client = selectClient(clientList);
                    if (client != null) clients.add(client);
                }
                return clients;
            } else {
                final EVCacheClient client = getEVCacheClientForReadExclude(serverGroupToExclude);
                if (client != null) return Collections.singletonList(client);
            }
        } catch (Throwable t) {
            log.error("Exception trying to get an readable EVCache Instances for zone " + serverGroupToExclude, t);
        }
        return Collections.<EVCacheClient> emptyList();
    }

    public EVCacheClient[] getWriteOnlyEVCacheClients() {
        try {
            int size = memcachedWriteInstancesByServerGroup.size() - memcachedReadInstancesByServerGroup.size();
            if (size == 0) return new EVCacheClient[0];
            final EVCacheClient[] clientArr = new EVCacheClient[size];
            for (ServerGroup rSet : memcachedWriteInstancesByServerGroup.keySet()) {
                if (!memcachedReadInstancesByServerGroup.containsKey(rSet)) {
                    final List<EVCacheClient> clients = memcachedWriteInstancesByServerGroup.get(rSet);
                    if (clients.size() == 1) {
                        clientArr[--size] = clients.get(0); // frequently used
                        // usecase
                    } else {
                        final long currentVal = numberOfModOps.incrementAndGet();
                        final int index = (int) (currentVal % clients.size());
                        clientArr[--size] = (index < 0) ? clients.get(0) : clients.get(index);
                    }
                }
            }
            return clientArr;
        } catch (Throwable t) {
            log.error("Exception trying to get an array of writable EVCache Instances", t);
            return new EVCacheClient[0];
        }
    }

    public EVCacheClient[] getEVCacheClientForWrite() {
        try {
            final EVCacheClient[] clientArr = new EVCacheClient[memcachedWriteInstancesByServerGroup.size()];
            int i = 0;
            for (ServerGroup rSet : memcachedWriteInstancesByServerGroup.keySet()) {
                final List<EVCacheClient> clients = memcachedWriteInstancesByServerGroup.get(rSet);
                if(clients.size() == 1) {
                    clientArr[i++] = clients.get(0); // frequently used usecase
                } else {
                    final long currentVal = numberOfModOps.incrementAndGet();
                    final int index = (int)(currentVal % clients.size());
                    clientArr[i++] = (index < 0) ? clients.get(0) : clients.get(index);
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

    private boolean haveInstancesInServerGroupChanged(ServerGroup rSet, Set<InetSocketAddress> discoveredHostsInServerGroup) {
        final List<EVCacheClient> clients = memcachedInstancesByServerGroup.get(rSet);

        // 1. if we have discovered instances in zone but not in our map then
        // return immediately
        if(clients == null ) return true;

        //2. Do a quick check based on count (active, inactive and discovered)
        for (int i = 0; i < clients.size(); i++) {
            final int size = clients.size();
            final EVCacheClient client = clients.get(i);
            final EVCacheConnectionObserver connectionObserver = client.getConnectionObserver();
            final int activeServerCount = connectionObserver.getActiveServerCount();
            final int inActiveServerCount = connectionObserver.getInActiveServerCount();
            final int sizeInDiscovery = discoveredHostsInServerGroup.size();
            final int sizeInHashing = client.getNodeLocator().getAll().size();
            final BasicTagList tags = BasicTagList.of("ServerGroup", rSet.getName(), "AppName", _appName);
            if (i == 0) {
                EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-PoolSize", tags).set(Long.valueOf(size));
                EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-ActiveConnections", tags).set(Long.valueOf(activeServerCount * size));
                EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-InactiveConnections", tags).set(Long.valueOf(inActiveServerCount * size));
                EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-InDiscovery", tags).set(Long.valueOf(sizeInDiscovery));
                EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-InHashing", tags).set(Long.valueOf(sizeInHashing));

                final List<EVCacheClient> readClients = memcachedReadInstancesByServerGroup.get(rSet);
                if (readClients != null && readClients.size() > 0) {
                    EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-ReadInstanceCount", tags).set(Long.valueOf(readClients.get(0).getConnectionObserver()
                            .getActiveServerCount()));
                }
                final List<EVCacheClient> writeClients = memcachedWriteInstancesByServerGroup.get(rSet);
                if (writeClients != null && writeClients.size() > 0) {
                    EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-WriteInstanceCount", tags).set(Long.valueOf(writeClients.get(0).getConnectionObserver()
                            .getActiveServerCount()));
                }
            }

            if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + rSet + "\n\tActive Count : " + activeServerCount + "\n\tInactive Count : "
                    + inActiveServerCount + "\n\tDiscovery Count : " + sizeInDiscovery);
            final long currentTime = System.currentTimeMillis();
            boolean reconcile = false;
            if(currentTime - lastReconcileTime > reconcileInterval.get()) {
                reconcile = true;
                lastReconcileTime = currentTime;
                EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-Reconcile", tags).set(Long.valueOf(1));
            } else {
                EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-Reconcile", tags).set(Long.valueOf(0));
            }
            final boolean hashingSizeDiff = (sizeInHashing != sizeInDiscovery && sizeInHashing != activeServerCount);
            if (reconcile || activeServerCount != sizeInDiscovery || inActiveServerCount > 0 || hashingSizeDiff ) {
                if(log.isDebugEnabled()) log.debug("\n\t"+ _appName + " & " + rSet + " experienced an issue.\n\tActive Server Count : " + activeServerCount);
                if(log.isDebugEnabled()) log.debug("\n\tInActive Server Count : " + inActiveServerCount + "\n\tDiscovered Instances : " + sizeInDiscovery);

                // 1. If a host is in discovery and we don't have an active or
                // inActive connection to it then we will have to refresh our
                // list. Typical case is we have replaced an existing node or
                // expanded the cluster.
                for (InetSocketAddress instance : discoveredHostsInServerGroup) {
                    if (!connectionObserver.getActiveServers().containsKey(instance) && !connectionObserver.getInActiveServers().containsKey(instance)) {
                        // DynamicCounter.increment("EVCacheClientPool-" +
                        // _appName + "-" + client.getId() + "-different_set");
                        if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + rSet + "; instance : " + instance
                                + " not found and will shutdown the client and init it again.");
                        EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-haveInstancesInServerGroupChanged", tags).set(Long.valueOf(1));
                        return true;
                    }
                }

                // 2. If a host is not in discovery and is 
                // inActive for more than 15 mins then we will have to refresh our
                // list. Typical case is we have replaced an existing node or
                // decreasing the cluster. Replacing an instance should not take more than 15 mins. 
                // Even if it does then we will refresh the client twice which should be ok. 
                // NOTE : For a zombie instance this will mean that it will take 15 mins after detaching and taking it OOS to be removed unless we force a refresh 
                // 12/5/2015 - Should we even do this anymore 
                for (Entry<InetSocketAddress, Long> entry : connectionObserver.getInActiveServers().entrySet()) {
                    if ((currentTime - entry.getValue().longValue()) > 900000 && !discoveredHostsInServerGroup.contains(entry.getKey())) {
                        if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + rSet + "; instance : " + entry.getKey()
                        + " not found in discovery and will shutdown the client and init it again.");
                        EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-haveInstancesInServerGroupChanged", tags).set(Long.valueOf(2));
                        return true;
                    }
                }

                //3. Check to see if there are any inactive connections. If we find inactive connections and this node is not in discovery then we will refresh the client.
                final Collection<MemcachedNode> allNodes = client.getNodeLocator().getAll();
                for (MemcachedNode node : allNodes) {
                    if (node instanceof EVCacheNodeImpl) {
                        final EVCacheNodeImpl evcNode = ((EVCacheNodeImpl) node);
                        //If the connection to a node is not active then we will reconnect the client.
                        if(!evcNode.isActive() && !discoveredHostsInServerGroup.contains(evcNode.getSocketAddress())) {
                            if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + rSet + "; Node : " + node 
                                    + " is not active. Will shutdown the client and init it again.");

                            EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-haveInstancesInServerGroupChanged", tags).set(Long.valueOf(3));
                            return true;
                        }
                    }
                }

                //4. if there is a difference in the number of nodes in the KetamaHashingMap then refresh
                if(hashingSizeDiff) {
                    if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + rSet  
                            + "; PoolSize : " + size + "; ActiveConnections : " + activeServerCount
                            + "; InactiveConnections : " + inActiveServerCount + "; InDiscovery : " + sizeInDiscovery
                            + "; InHashing : " + sizeInHashing + "; hashingSizeDiff : " + hashingSizeDiff + ". Since there is a diff in hashing size will shutdown the client and init it again.");

                    EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-haveInstancesInServerGroupChanged", tags).set(Long.valueOf(4));
                    return true;
                }

                //9. If we have removed all instances or took them OOS in a ServerGroup then shutdown the client
                if(sizeInDiscovery == 0) {
                    if(activeServerCount == 0 || inActiveServerCount > activeServerCount) {
                        if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + rSet
                                + "; Will shutdown the client since there are no active servers and no servers for this ServerGroup in disocvery.");
                        EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-haveInstancesInServerGroupChanged", tags).set(Long.valueOf(9));
                        return true;
                    }
                }
            }
            EVCacheMetricsFactory.getLongGauge("EVCacheClientPool-haveInstancesInServerGroupChanged", tags).set(Long.valueOf(0));
        }
        return false;
    }

    private List<InetSocketAddress> getMemcachedSocketAddressList(final Set<InetSocketAddress> discoveredHostsInZone) {
        final List<InetSocketAddress> memcachedNodesInZone = new ArrayList<InetSocketAddress>();
        for (InetSocketAddress hostAddress : discoveredHostsInZone) {
            memcachedNodesInZone.add(hostAddress);
        }
        return memcachedNodesInZone;
    }


    private void shutdownClientsInZone(List<EVCacheClient> clients) {
        if(clients == null || clients.isEmpty()) return;

        // Shutdown the old clients in 60 seconds, this will give ample time to
        // cleanup anything pending in its queue
        for(EVCacheClient oldClient : clients) {
            // DynamicCounter.increment("EVCacheClientPool-" + _appName + "-" +
            // oldClient.getZone() + "-" + oldClient.getId() + "-shutdown");
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

    private void setupNewClientsByServerGroup(ServerGroup rSet, List<EVCacheClient> newClients) {
        final List<EVCacheClient> currentClients = memcachedInstancesByServerGroup.put(rSet, newClients);

        //if the zone is in write only mode then remove it from the Map
        final BooleanProperty isZoneInWriteOnlyMode = writeOnlyFastPropertyMap.get(rSet);
        if(isZoneInWriteOnlyMode.get().booleanValue()) {
            memcachedReadInstancesByServerGroup.remove(rSet);
        } else {
            memcachedReadInstancesByServerGroup.put(rSet, newClients);
        }
        memcachedWriteInstancesByServerGroup.put(rSet, newClients);

        if(currentClients == null || currentClients.isEmpty()) return;

        // Now since we have replace the old instances shutdown all the old
        // clients
        if (log.isDebugEnabled()) log.debug("Replaced an existing Pool for ServerGroup : " + rSet + "; and app " + _appName + " ;\n\tOldClients : " + currentClients
                + ";\n\tNewClients : " + newClients);
        for(EVCacheClient client : currentClients) {
            if(!client.isShutdown()) {
                if (log.isDebugEnabled()) log.debug("Shutting down in Fallback -> AppName : " + _appName + "; ServerGroup : " + rSet + "; client {" + client + "};");
                try {
                    if(client.getConnectionObserver() != null) {
                        final boolean obsRemoved = client.removeConnectionObserver();
                        if(log.isDebugEnabled()) log.debug("Connection observer removed " + obsRemoved);
                    }
                    final boolean status = client.shutdown(5, TimeUnit.SECONDS);
                    if(log.isDebugEnabled()) log.debug("Shutting down {" + client + "} ; status : " + status);
                } catch(Exception ex) {
                    log.error("Exception while shutting down the old Client", ex);
                }
            }
        }

        // Paranoid Here. Even though we have shutdown the old clients do it
        // again as we noticed issues while shutting down MemcachedNodes
        shutdownClientsInZone(currentClients);
    }

    // Check if a zone has been moved to Write only. If so, remove the app from the read map.
    // Similarly if the app has been moved to Read+Write from write only add it back to the read map.
    private void updateMemcachedReadInstancesByZone() {
        for (ServerGroup rSet : memcachedInstancesByServerGroup.keySet()) {
            final BooleanProperty isZoneInWriteOnlyMode = writeOnlyFastPropertyMap.get(rSet);
            if(isZoneInWriteOnlyMode.get().booleanValue()) {
                if (memcachedReadInstancesByServerGroup.containsKey(rSet)) {
                    DynamicCounter.increment("EVCacheClientPool-WRITE_ONLY-" + rSet);
                    memcachedReadInstancesByServerGroup.remove(rSet);
                }
            } else {
                if (!memcachedReadInstancesByServerGroup.containsKey(rSet)) {
                    DynamicCounter.increment("EVCacheClientPool-READ_ENABLED-" + rSet);
                    memcachedReadInstancesByServerGroup.put(rSet, memcachedInstancesByServerGroup.get(rSet));
                }
            }

            if(enableDynamicWriteOnlyMode.get()) {
                //if we lose over 50% of instances put that zone in writeonly mode.
                final List<EVCacheClient> clients = memcachedReadInstancesByServerGroup.get(rSet);
                if(clients != null && !clients.isEmpty()) {
                    final EVCacheClient client = clients.get(0);
                    if(client != null) {
                        final EVCacheConnectionObserver connectionObserver = client.getConnectionObserver();
                        if(connectionObserver != null) {
                            final int activeServerCount = connectionObserver.getActiveServerCount();
                            final int inActiveServerCount = connectionObserver.getInActiveServerCount();
                            if(inActiveServerCount > activeServerCount) {
                                memcachedReadInstancesByServerGroup.remove(rSet);
                            }
                        }
                    }
                }
            }
        }

        if (memcachedReadInstancesByServerGroup.size() != memcachedFallbackReadInstances.getSize()) {
            memcachedFallbackReadInstances = new ServerGroupCircularIterator(memcachedReadInstancesByServerGroup.keySet());

            Map<String, Set<ServerGroup>> readServerGroupByZoneMap = new ConcurrentHashMap<String, Set<ServerGroup>>();
            for (ServerGroup rSet : memcachedReadInstancesByServerGroup.keySet()) {
                Set<ServerGroup> serverGroupList = readServerGroupByZoneMap.get(rSet.getZone());
                if (serverGroupList == null) {
                    serverGroupList = new HashSet<ServerGroup>();
                    readServerGroupByZoneMap.put(rSet.getZone(), serverGroupList);
                }
                serverGroupList.add(rSet);
            }

            Map<String, ServerGroupCircularIterator> _readServerGroupByZone = new ConcurrentHashMap<String, ServerGroupCircularIterator>();
            for (Entry<String, Set<ServerGroup>> readServerGroupByZoneEntry : readServerGroupByZoneMap.entrySet()) {
                _readServerGroupByZone.put(readServerGroupByZoneEntry.getKey(), new ServerGroupCircularIterator(readServerGroupByZoneEntry.getValue()));
            }
            this.readServerGroupByZone = _readServerGroupByZone;
            localServerGroupIterator = readServerGroupByZone.get(_zone);
        }

    }

    private void cleanupMemcachedInstances(boolean force) {
        pingServers();
        for (Iterator<Entry<ServerGroup, List<EVCacheClient>>> it = memcachedInstancesByServerGroup.entrySet().iterator(); it.hasNext();) {
            final Entry<ServerGroup, List<EVCacheClient>> serverGroupEntry = it.next();
            final List<EVCacheClient> instancesInAServerGroup = serverGroupEntry.getValue();
            boolean removeEntry = false;
            for (EVCacheClient client : instancesInAServerGroup) {
                final EVCacheConnectionObserver connectionObserver = client.getConnectionObserver();
                if(connectionObserver.getActiveServerCount() == 0 && connectionObserver.getInActiveServerCount() > 0) {
                    removeEntry = true;
                }
            }
            if(force || removeEntry) {
                final ServerGroup rSet = serverGroupEntry.getKey();
                memcachedReadInstancesByServerGroup.remove(rSet);
                memcachedWriteInstancesByServerGroup.remove(rSet);
                for (EVCacheClient client : instancesInAServerGroup) {
                    if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + rSet + " has no active servers. Cleaning up this ServerGroup.");
                    client.shutdown(0, TimeUnit.SECONDS);
                    client.getConnectionObserver().shutdown();
                }
                it.remove();
            }
        }
    }

    private synchronized void refresh(boolean force) throws IOException {
        final Operation op = EVCacheMetricsFactory.getOperation("EVCacheClientPool-" + _appName + "-refresh");
        if (log.isDebugEnabled()) log.debug("refresh APP : " + _appName + "; force : " + force);
        try {
            final Map<ServerGroup, Set<InetSocketAddress>> instances = provider.discoverInstances();
            if (log.isDebugEnabled()) log.debug("instances : " + instances);
            // if no instances are found check to see if a clean up is needed
            // and bail immediately.
            if(instances == null || instances.isEmpty()) {
                if (!memcachedInstancesByServerGroup.isEmpty()) cleanupMemcachedInstances(false);
                return;
            }

            for (Entry<ServerGroup, Set<InetSocketAddress>> serverGroupEntry : instances.entrySet()) {
                final ServerGroup rSet = serverGroupEntry.getKey();
                final Set<InetSocketAddress> discoverdInstanceInServerGroup = serverGroupEntry.getValue();
                final String zone = rSet.getZone();
                final Set<InetSocketAddress> discoveredHostsInServerGroup = (discoverdInstanceInServerGroup == null) ? Collections.<InetSocketAddress> emptySet() : discoverdInstanceInServerGroup;
                if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + rSet + "\n\tSize : " + discoveredHostsInServerGroup.size()
                + "\n\tInstances in ServerGroup : " + discoveredHostsInServerGroup);

                if (discoveredHostsInServerGroup.size() == 0 && memcachedInstancesByServerGroup.containsKey(rSet)) {
                    if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + rSet + " has no active servers. Cleaning up this ServerGroup.");
                    final List<EVCacheClient> clients = memcachedInstancesByServerGroup.remove(rSet);
                    memcachedReadInstancesByServerGroup.remove(rSet);
                    memcachedWriteInstancesByServerGroup.remove(rSet);
                    for(EVCacheClient client : clients) {
                        if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + rSet + "\n\tClient : " + client + " will be shutdown in 30 seconds.");
                        client.shutdown(30, TimeUnit.SECONDS);
                        client.getConnectionObserver().shutdown();
                    }
                    continue;
                }

                boolean instanceChangeInServerGroup = force;
                if (instanceChangeInServerGroup) {
                    if (log.isWarnEnabled()) log.warn("FORCE REFRESH :: AppName :" + _appName + "; ServerGroup : " + rSet + "; Changed : "
                            + instanceChangeInServerGroup);
                } else {
                    instanceChangeInServerGroup = haveInstancesInServerGroupChanged(rSet, discoveredHostsInServerGroup);
                    if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + rSet + "\n\tinstanceChangeInServerGroup : " + instanceChangeInServerGroup);
                    if (!instanceChangeInServerGroup) {
                        // quick exit as everything looks fine. No new instances
                        // found and were inactive
                        if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + rSet + "; Changed : " + instanceChangeInServerGroup);
                        continue;
                    }
                }

                // Let us create a list of SocketAddress from the discovered
                // instaces in zone
                final List<InetSocketAddress> memcachedSAInServerGroup = getMemcachedSocketAddressList(discoveredHostsInServerGroup);

                if (memcachedSAInServerGroup.size() > 0) {
                    // now since there is a change with the instances in the
                    // zone. let us go ahead and create a new EVCacheClient with
                    // the new settings
                    final int poolSize = _poolSize.get();
                    final List<EVCacheClient> newClients = new ArrayList<EVCacheClient>(poolSize);
                    for(int i = 0; i < poolSize; i++) {
                        final int maxQueueSize = ConfigurationManager.getConfigInstance().getInt(_appName + ".max.queue.length", 16384);
                        EVCacheClient client;
                        try {
                            client = new EVCacheClient(_appName, zone, i, rSet, memcachedSAInServerGroup, maxQueueSize, _maxReadQueueSize, _readTimeout, _bulkReadTimeout,
                                    _opQueueMaxBlockTime, _operationTimeout, this);
                            newClients.add(client);
                            final int id = client.getId();
                            // DynamicCounter.increment("EVCacheClientPool-" +
                            // rSet.getName() + "-init");
                            if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + rSet + "; intit : client.getId() : " + id);
                            lastReconcileTime = System.currentTimeMillis();
                        } catch (Exception e) {
                            EVCacheMetricsFactory.increment("EVCacheClientPool-" + _appName + "-" + rSet.getName() + "EVCacheClient-INIT_ERROR");
                            log.error("Unable to create EVCacheClient for app - " + _appName + " and Server Group - " + rSet.getName(), e);
                        }
                    }
                    if (newClients.size() > 0) setupNewClientsByServerGroup(rSet, newClients);
                }
            }

            // Check to see if a zone has been removed, if so remove them from
            // the active list
            if (memcachedInstancesByServerGroup.size() > instances.size()) {
                if (log.isDebugEnabled()) log.debug("\n\tAppName :" + _appName + ";\n\tServerGroup Discovered : " + instances.keySet()
                + ";\n\tCurrent ServerGroup in EVCache Client : " + memcachedInstancesByServerGroup.keySet());
                cleanupMemcachedInstances(false);
            }
            updateMemcachedReadInstancesByZone();
            if(_pingServers.get()) pingServers();
        } catch(Throwable t) {
            log.error("Exception while refreshing the Server list", t);
        } finally {
            op.stop();
        }

        if (log.isDebugEnabled()) log.debug("refresh APP : " + _appName + "; DONE");
    }

    public void pingServers() {
        try {
            final Map<ServerGroup, List<EVCacheClient>> allServers = getAllInstancesByZone();
            for (Entry<ServerGroup, List<EVCacheClient>> entry : allServers.entrySet()) {
                final List<EVCacheClient> listOfClients = entry.getValue();
                for(EVCacheClient client : listOfClients) {
                    final Map<SocketAddress, String> versions = client.getVersions();
                    for(Entry<SocketAddress, String> vEntry : versions.entrySet()) {
                        if(log.isDebugEnabled()) log.debug("Host : " + vEntry.getKey() + " : " + vEntry.getValue());
                    }
                }
            }
        } catch(Throwable t) {
            log.error("Error while pinging the servers", t);
        }
    }

    public void serverGroupDisabled(final ServerGroup rSet) {
        if (memcachedInstancesByServerGroup.containsKey(rSet)) {
            if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + rSet + " has no active servers. Cleaning up this ServerGroup.");
            final List<EVCacheClient> clients = memcachedInstancesByServerGroup.remove(rSet);
            memcachedReadInstancesByServerGroup.remove(rSet);
            memcachedWriteInstancesByServerGroup.remove(rSet);
            for(EVCacheClient client : clients) {
                if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + rSet + "\n\tClient : " + client + " will be shutdown in 30 seconds.");
                client.shutdown(30, TimeUnit.SECONDS);
                client.getConnectionObserver().shutdown();
            }
        }
    }

    public void refreshAsync(MemcachedNode node) {
        EVCacheMetricsFactory.increment(_appName, null, "EVCacheClientPool-refreshAsync");
        if (log.isWarnEnabled()) log.warn("Pool is being refresh as the EVCacheNode is not available. " + node.toString());
        Thread t = new Thread() {
            public void run() {
                try {
                    refresh(true);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        };
        t.setName("PoolRefresher-@" + System.currentTimeMillis() + "-by-" + node.getSocketAddress());
        t.start();
    }

    public void run() {
        try {
            refresh();
        } catch(Throwable t) {
            if(log.isDebugEnabled()) log.debug("Error Refreshing EVCache Instance list for " + _appName , t);
        }
    }

    public boolean doFIT() {
        //        final FitContext fitContext = new FitContextImpl();
        //        if (fitContext.shouldInjectFailure(injectionPoint)) {
        //            final InjectedFailure failure = fitContext.retrieveFromContext(injectionPoint);
        //            int delay = failure.getDelay() == null ? 0 : failure.getDelay().intValue();
        //            final int timeout = _readTimeout.get().intValue();
        //
        //            if (delay > 0) {
        //                try {
        //                    if (log.isDebugEnabled()) log.debug("App : " + _appName + "; zone : " + _zone + "; Injected FIT Delay : " + delay + "msec.");
        //                    Thread.sleep(Math.min(delay, timeout));
        //                } catch (InterruptedException e) {
        //                    Thread.currentThread().interrupt();
        //                }
        //
        //                if (delay >= timeout) return true;
        //            }
        //
        //            if (failure.shouldFail()) {
        //                if (log.isDebugEnabled()) log.debug("App : " + _appName + "; zone : " + _zone + "; Injected FIT failure");
        //                return true;
        //            }
        //        }
        return false;
    }

//    public boolean throttleRequest(Call call) {
//        if (doFIT()) return true;
//        if(!throttleRequest.get()) return false;
//        if(disableWrites.get()) {
//            if (call == Call.SET || call == Call.DELETE || call == Call.INCR || call == Call.DECR || call == Call.REPLACE) return true;
//        }
//        final int throttleTime = throttlingTime.get();
//        if(throttleTime > 0) {
//            try {
//                if(log.isDebugEnabled()) log.debug("Throttle Request for " + throttleTime + " msec");
//                Thread.sleep(throttleTime);
//            } catch (InterruptedException e) {
//            }
//            return false;
//        }
//        final int throttlingPcnt = throttlingPercent.get();
//        if(throttlingPcnt == 0) return false;
//
//        final long currentVal = throttlingCount.getAndIncrement();
//        boolean throttle = (currentVal % 100) < throttlingPcnt;
//        if(throttle && log.isDebugEnabled()) log.debug("Throttle Request since this request falls in " + throttlingPcnt + "% of request");
//        return throttle;
//    }

    void shutdown() {
        if(log.isDebugEnabled()) log.debug("EVCacheClientPool for App : " + _appName + " and Zone : " + _zone + " is being shutdown.");
        _shutdown = true;
        for (List<EVCacheClient> instancesInAZone : memcachedInstancesByServerGroup.values()) {
            for(EVCacheClient client : instancesInAZone) {
                client.shutdown(30, TimeUnit.SECONDS);
                client.getConnectionObserver().shutdown();
            }
        }
        setupMonitoring();
    }

    private void setupMonitoring() {
        try {
            final ObjectName mBeanName = ObjectName.getInstance("com.netflix.evcache:Group="+ _appName + ",SubGroup=pool");
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if(mbeanServer.isRegistered(mBeanName)) {
                if(log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanName + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanName);
            }
            if(!_shutdown) {
                mbeanServer.registerMBean(this, mBeanName);
                Monitors.registerObject(this);
            } else {
                Monitors.unregisterObject(this);
            }
        } catch (Exception e) {
            if(log.isDebugEnabled()) log.debug("Exception",e);
        }
    }

    public int getInstanceCount() {
        int instances = 0;
        for (ServerGroup rSet : memcachedInstancesByServerGroup.keySet()) {
            instances += memcachedInstancesByServerGroup.get(rSet).get(0).getConnectionObserver().getActiveServerCount();
        }
        return instances;
    }

    public Map<String, String> getInstancesByZone() {
        Map<String, String> instanceMap = new HashMap<String, String>();
        for (ServerGroup zone : memcachedInstancesByServerGroup.keySet()) {
            final List<EVCacheClient> instanceList = memcachedInstancesByServerGroup.get(zone);
            instanceMap.put(zone.toString(), instanceList.toString());
        }
        return instanceMap;
    }

    public Map<String, Integer> getInstanceCountByZone() {
        final Map<String, Integer> instancesByZone = new HashMap<String, Integer>(memcachedInstancesByServerGroup.size() * 2);
        for (ServerGroup zone : memcachedInstancesByServerGroup.keySet()) {
            instancesByZone.put(zone.getName(), Integer.valueOf(memcachedInstancesByServerGroup.get(zone).get(0).getConnectionObserver().getActiveServerCount()));
        }
        return instancesByZone;
    }

    public Map<String, String> getReadZones() {
        final Map<String, String> instanceMap = new HashMap<String, String>();
        for (ServerGroup key : memcachedReadInstancesByServerGroup.keySet()) {
            instanceMap.put(key.getName(), memcachedReadInstancesByServerGroup.get(key).toString());
        }
        return instanceMap;
    }

    public Map<String, Integer> getReadInstanceCountByZone() {
        final Map<String, Integer> instanceMap = new HashMap<String, Integer>();
        for (ServerGroup key : memcachedReadInstancesByServerGroup.keySet()) {
            instanceMap.put(key.getName(), Integer.valueOf(memcachedReadInstancesByServerGroup.get(key).get(0).getConnectionObserver().getActiveServerCount()));
        }
        return instanceMap;
    }

    public Map<String, String> getWriteZones() {
        final Map<String, String> instanceMap = new HashMap<String, String>();
        for (ServerGroup key : memcachedWriteInstancesByServerGroup.keySet()) {
            instanceMap.put(key.toString(), memcachedWriteInstancesByServerGroup.get(key).toString());
        }
        return instanceMap;
    }

    public Map<ServerGroup, List<EVCacheClient>> getAllInstancesByZone() {
        return Collections.unmodifiableMap(memcachedInstancesByServerGroup);
    }

    Map<ServerGroup, List<EVCacheClient>> getAllInstancesByServerGroup() {
        return memcachedInstancesByServerGroup;
    }

    public Map<String, Integer> getWriteInstanceCountByZone() {
        final Map<String, Integer> instanceMap = new HashMap<String, Integer>();
        for (ServerGroup key : memcachedWriteInstancesByServerGroup.keySet()) {
            instanceMap.put(key.toString(), Integer.valueOf(memcachedWriteInstancesByServerGroup.get(key).get(0).getConnectionObserver().getActiveServerCount()));
        }
        return instanceMap;
    }

    public Map<String, String> getReadServerGroupByZone() {
        final Map<String, String> instanceMap = new HashMap<String, String>();
        for (String key : readServerGroupByZone.keySet()) {
            instanceMap.put(key, readServerGroupByZone.get(key).toString());
        }
        return instanceMap;
    }

    public void refreshPool() {
        try {
            refresh(true);
        } catch(Throwable t) {
            if(log.isDebugEnabled()) log.debug("Error Refreshing EVCache Instance list from MBean : " + _appName , t);
        }
    }

    public String getFallbackServerGroup() {
        return memcachedFallbackReadInstances.toString();
    }

    public boolean supportsFallback() {
        return memcachedFallbackReadInstances.getSize() > 1;
    }

    public boolean isLogEventEnabled() {
        return (logOperations.get() > 0);
    }

    public boolean shouldLogOperation(String key, String op) {
        if(!isLogEventEnabled()) return false;
        if(!logOperationCalls.get().contains(op)) return false;
        return key.hashCode() % 1000 < logOperations.get();
    }


    @Override
    public String getLocalServerGroupCircularIterator() {
        return (localServerGroupIterator == null) ? "NONE" : localServerGroupIterator.toString();
    }

    public String getPoolDetails() {
        return toString();
    }

    @Override
    public String toString() {
        return "\nEVCacheClientPool [\n\t_appName=" + _appName + ",\n\t_zone=" + _zone + ",\n\tlocalServerGroupIterator=" + localServerGroupIterator + ",\n\t_poolSize=" + _poolSize
                + ",\n\t_readTimeout=" + _readTimeout + ",\n\t_bulkReadTimeout=" + _bulkReadTimeout + ",\n\tenableServerGroup=" + enableServerGroup
                + ",\n\tenableDynamicWriteOnlyMode=" + enableDynamicWriteOnlyMode + ",\n\tlogOperations=" + logOperations + ",\n\t_opQueueMaxBlockTime="
                + _opQueueMaxBlockTime + ",\n\t_operationTimeout=" + _operationTimeout + ",\n\t_maxReadQueueSize=" + _maxReadQueueSize // + ",\n\tinjectionPoint=" + injectionPoint
                + ",\n\t_pingServers=" + _pingServers + ",\n\twriteOnlyFastPropertyMap=" + writeOnlyFastPropertyMap + ",\n\tnumberOfModOps=" + numberOfModOps.get()
                + ",\n\t_shutdown=" + _shutdown + ",\n\tmemcachedInstancesByServerGroup=" + memcachedInstancesByServerGroup + ",\n\tmemcachedReadInstancesByServerGroup="
                + memcachedReadInstancesByServerGroup + ",\n\tmemcachedWriteInstancesByServerGroup=" + memcachedWriteInstancesByServerGroup + ",\n\treadServerGroupByZone="
                + readServerGroupByZone + ",\n\tmemcachedFallbackReadInstances=" + memcachedFallbackReadInstances + "\n]";
    }

    public int getPoolSize() {
        return _poolSize.get();
    }

    public DynamicIntProperty getLogOperations() {
        return logOperations;
    }

    public DynamicIntProperty getOpQueueMaxBlockTime() {
        return _opQueueMaxBlockTime;
    }

    public DynamicIntProperty getOperationTimeout() {
        return _operationTimeout;
    }

    public DynamicIntProperty getMaxReadQueueSize() {
        return _maxReadQueueSize;
    }

    public DynamicBooleanProperty getPingServers() {
        return _pingServers;
    }

    public long getNumberOfModOps() {
        return numberOfModOps.get();
    }

    public boolean isShutdown() {
        return _shutdown;
    }

    public String getZone() {
        return this._zone;
    }

    public String getAppName() {
        return this._appName;
    }

    public EVCacheClientPoolManager getEVCacheClientPoolManager() {
        return this.manager;
    }

    public Map<ServerGroup, BooleanProperty> getWriteOnlyFastPropertyMap() {
        return Collections.unmodifiableMap(writeOnlyFastPropertyMap);
    }

    public ChainedDynamicProperty.IntProperty getReadTimeout() {
        return _readTimeout;
    }

    public ChainedDynamicProperty.IntProperty getBulkReadTimeout() {
        return _bulkReadTimeout;
    }

}
