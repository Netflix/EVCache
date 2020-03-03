package com.netflix.evcache.pool;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.netflix.archaius.api.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.observer.EVCacheConnectionObserver;
import com.netflix.evcache.util.CircularIterator;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.evcache.util.ServerGroupCircularIterator;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Tag;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.protocol.binary.EVCacheNodeImpl;

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({ "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", "REC_CATCH_EXCEPTION", "MDM_THREAD_YIELD" })
public class EVCacheClientPool implements Runnable, EVCacheClientPoolMBean {

    private static final Logger log = LoggerFactory.getLogger(EVCacheClientPool.class);

    private final String _appName;
    private final String _zone;
    private final EVCacheClientPoolManager manager;
    private ServerGroupCircularIterator localServerGroupIterator = null;
    private final Property<Integer> _poolSize; // Number of MemcachedClients to each cluster
    private final Property<Integer> _readTimeout; // Timeout for readOperation
    private final Property<Integer> _bulkReadTimeout; // Timeout for readOperation
    public static final String DEFAULT_PORT = "11211";

    private final Property<Boolean> _retryAcrossAllReplicas;
    private long lastReconcileTime = 0;

    private final Property<Integer> logOperations;
    private final Property<Set<String>> logOperationCalls;
    private final Property<Set<String>> cloneWrite;

    private final Property<Integer> _opQueueMaxBlockTime; // Timeout for adding an operation
    private final Property<Integer> _operationTimeout;// Timeout for write operation
    private final Property<Integer> _maxReadQueueSize;
    private final Property<Integer> reconcileInterval;
    private final Property<Integer> _maxRetries;

    private final Property<Boolean> _pingServers;

    private final Property<Boolean> refreshConnectionOnReadQueueFull;
    private final Property<Integer> refreshConnectionOnReadQueueFullSize;

    private final ThreadPoolExecutor asyncRefreshExecutor;
    private final Property<Boolean> _disableAsyncRefresh;
    private final List<Tag> tagList;
//    private final Id poolSizeId;
    //private final Map<String, Counter> counterMap = new ConcurrentHashMap<String, Counter>();
    private final Map<String, Gauge> gaugeMap = new ConcurrentHashMap<String, Gauge>();

    @SuppressWarnings("serial")
    private final Map<ServerGroup, Property<Boolean>> writeOnlyFastPropertyMap = new ConcurrentHashMap<ServerGroup, Property<Boolean>>() {
        @Override
        public Property<Boolean> get(Object _serverGroup) {
            final ServerGroup serverGroup = ServerGroup.class.cast(_serverGroup);
            Property<Boolean> isServerGroupInWriteOnlyMode = super.get(serverGroup);
            if (isServerGroupInWriteOnlyMode != null) return isServerGroupInWriteOnlyMode;

            isServerGroupInWriteOnlyMode = EVCacheConfig.getInstance().
                    getPropertyRepository().get(_appName + "." + serverGroup.getName() + ".EVCacheClientPool.writeOnly", Boolean.class)
                    .orElseGet(_appName + "." + serverGroup.getZone() + ".EVCacheClientPool.writeOnly").orElse(false);
            put(serverGroup, isServerGroupInWriteOnlyMode);
            return isServerGroupInWriteOnlyMode;
        };
    };

    private final AtomicLong numberOfModOps = new AtomicLong(0);

    private boolean _shutdown = false;
    private Map<ServerGroup, List<EVCacheClient>> memcachedInstancesByServerGroup = new ConcurrentHashMap<ServerGroup, List<EVCacheClient>>();
    private Map<ServerGroup, List<EVCacheClient>> memcachedReadInstancesByServerGroup = new ConcurrentHashMap<ServerGroup, List<EVCacheClient>>();
    private Map<ServerGroup, List<EVCacheClient>> memcachedWriteInstancesByServerGroup = new ConcurrentSkipListMap<ServerGroup, List<EVCacheClient>>();
    private final Map<InetSocketAddress, Long> evCacheDiscoveryConnectionLostSet = new ConcurrentHashMap<InetSocketAddress, Long>();
    private Map<String, ServerGroupCircularIterator> readServerGroupByZone = new ConcurrentHashMap<String, ServerGroupCircularIterator>();
    private ServerGroupCircularIterator memcachedFallbackReadInstances = new ServerGroupCircularIterator(Collections.<ServerGroup> emptySet());
    private CircularIterator<EVCacheClient[]> allEVCacheWriteClients = new CircularIterator<EVCacheClient[]>(Collections.<EVCacheClient[]> emptyList());
    private final EVCacheNodeList provider;

    EVCacheClientPool(final String appName, final EVCacheNodeList provider, final ThreadPoolExecutor asyncRefreshExecutor, final EVCacheClientPoolManager manager) {
        this._appName = appName;
        this.provider = provider;
        this.asyncRefreshExecutor = asyncRefreshExecutor;
        this.manager = manager;

        String ec2Zone = System.getenv("EC2_AVAILABILITY_ZONE");
        if (ec2Zone == null) ec2Zone = System.getProperty("EC2_AVAILABILITY_ZONE");
        this._zone = (ec2Zone == null) ? "GLOBAL" : ec2Zone;
        final EVCacheConfig config = EVCacheConfig.getInstance();

        final Consumer<Integer> callback = t -> {
            clearState();
            refreshPool(true, true);
        };
        this._poolSize = config.getPropertyRepository().get(appName + ".EVCacheClientPool.poolSize", Integer.class).orElse(1);
        this._poolSize.subscribe(callback);
        this._readTimeout = config.getPropertyRepository().get(appName + ".EVCacheClientPool.readTimeout", Integer.class).orElse(manager.getDefaultReadTimeout().get());
        this._readTimeout.subscribe(callback);
        this._bulkReadTimeout = config.getPropertyRepository().get(appName + ".EVCacheClientPool.bulkReadTimeout", Integer.class).orElse(_readTimeout.get());
        this._bulkReadTimeout.subscribe(callback);

        this.refreshConnectionOnReadQueueFull = config.getPropertyRepository().get(appName + ".EVCacheClientPool.refresh.connection.on.readQueueFull", Boolean.class).orElseGet("EVCacheClientPool.refresh.connection.on.readQueueFull").orElse(false);
        this.refreshConnectionOnReadQueueFullSize = config.getPropertyRepository().get(appName + ".EVCacheClientPool.refresh.connection.on.readQueueFull.size", Integer.class).orElseGet("EVCacheClientPool.refresh.connection.on.readQueueFull.size").orElse(100);

        this._opQueueMaxBlockTime = config.getPropertyRepository().get(appName + ".operation.QueueMaxBlockTime", Integer.class).orElse(10);
        this._opQueueMaxBlockTime.subscribe(callback);
        this._operationTimeout = config.getPropertyRepository().get(appName + ".operation.timeout", Integer.class).orElse(2500);
        this._operationTimeout.subscribe(callback);
        this._maxReadQueueSize = config.getPropertyRepository().get(appName + ".max.read.queue.length", Integer.class).orElse(50);
        this._retryAcrossAllReplicas = config.getPropertyRepository().get(_appName + ".retry.all.copies", Boolean.class).orElse(false);
        this._disableAsyncRefresh = config.getPropertyRepository().get(_appName + ".disable.async.refresh", Boolean.class).orElse(false);
        this._maxRetries = config.getPropertyRepository().get(_appName + ".max.retry.count", Integer.class).orElse(1);

        Function<String, Set<String>> splitSet = t -> Arrays.stream(t.split(",")).collect(Collectors.toSet());
        this.logOperations = config.getPropertyRepository().get(appName + ".log.operation", Integer.class).orElse(0);
        this.logOperationCalls = config.getPropertyRepository().get(appName + ".log.operation.calls", String.class).orElse("SET,DELETE,GMISS,TMISS,BMISS_ALL,TOUCH,REPLACE").map(splitSet);
        this.reconcileInterval = config.getPropertyRepository().get(appName + ".reconcile.interval", Integer.class).orElse(600000);
        this.cloneWrite = config.getPropertyRepository().get(appName + ".clone.writes.to", String.class).map(splitSet).orElse(Collections.emptySet());
        this.cloneWrite.subscribe(i -> {
            setupClones();
        });

        tagList = new ArrayList<Tag>(1);
        tagList.add(new BasicTag(EVCacheMetricsFactory.CACHE, _appName));

        this._pingServers = config.getPropertyRepository().get(appName + ".ping.servers", Boolean.class).orElseGet("evcache.ping.servers").orElse(false);
        setupMonitoring();
        refreshPool(false, true);
        if (log.isInfoEnabled()) log.info(toString());
    }

    private void setupClones() {
    	for(String cloneApp : cloneWrite.get()) {
    		manager.initEVCache(cloneApp);
    	}
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
            if (log.isDebugEnabled()) log.debug("memcachedReadInstancesByServerGroup : " + memcachedReadInstancesByServerGroup);
            if(asyncRefreshExecutor.getQueue().isEmpty()) refreshPool(true, true);
            return null;
        }

        try {
            List<EVCacheClient> clients = null;
            if (localServerGroupIterator != null) {
                clients = memcachedReadInstancesByServerGroup.get(localServerGroupIterator.next());
            }

            if (clients == null) {
                final ServerGroup fallbackServerGroup = memcachedFallbackReadInstances.next();
                if (fallbackServerGroup == null) {
                    if (log.isDebugEnabled()) log.debug("fallbackServerGroup is null.");
                    return null;
                }
                clients = memcachedReadInstancesByServerGroup.get(fallbackServerGroup);
            }
            return selectClient(clients);
        } catch (Throwable t) {
            log.error("Exception trying to get an readable EVCache Instances for zone {}", _zone, t);
            return null;
        }
    }

    public List<EVCacheClient> getAllEVCacheClientForRead() {
        if (memcachedReadInstancesByServerGroup == null || memcachedReadInstancesByServerGroup.isEmpty()) {
            if (log.isDebugEnabled()) log.debug("memcachedReadInstancesByServerGroup : " + memcachedReadInstancesByServerGroup);
            if(asyncRefreshExecutor.getQueue().isEmpty()) refreshPool(true, true);
            return Collections.<EVCacheClient> emptyList();
        }

        try {
            List<EVCacheClient> clients = null;
            if (localServerGroupIterator != null) {
                clients = memcachedReadInstancesByServerGroup.get(localServerGroupIterator.next());
            }

            if (clients == null) {
                final ServerGroup fallbackServerGroup = memcachedFallbackReadInstances.next();
                if (fallbackServerGroup == null) {
                    if (log.isDebugEnabled()) log.debug("fallbackServerGroup is null.");
                    return Collections.<EVCacheClient> emptyList();
                }
                clients = memcachedReadInstancesByServerGroup.get(fallbackServerGroup);
            }
            return clients;
        } catch (Throwable t) {
            log.error("Exception trying to get readable EVCache Instances for zone ", t);
            return Collections.<EVCacheClient> emptyList();
        }
    }

    private EVCacheClient selectClient(List<EVCacheClient> clients) {
        if (clients == null || clients.isEmpty()) {
            if (log.isDebugEnabled()) log.debug("clients is null returning null and forcing pool refresh!!!");
            if(asyncRefreshExecutor.getQueue().isEmpty()) refreshPool(true, true);
            return null;
        }
        if (clients.size() == 1) {
            return clients.get(0); // Frequently used scenario
        }

        final long currentVal = numberOfModOps.incrementAndGet();
        // Get absolute value of current val to ensure correctness even at 9 quintillion+ requests
        // make sure to truncate after the mod. This allows up to 2^31 clients.
        final int index = Math.abs((int) (currentVal % clients.size()));
        return clients.get(index);
    }

    public EVCacheClient getEVCacheClientForReadExclude(ServerGroup rsetUsed) {
        if (memcachedReadInstancesByServerGroup == null || memcachedReadInstancesByServerGroup.isEmpty()) {
            if (log.isDebugEnabled()) log.debug("memcachedReadInstancesByServerGroup : " + memcachedReadInstancesByServerGroup);
            if(asyncRefreshExecutor.getQueue().isEmpty()) refreshPool(true, true);
            return null;
        }
        try {
            ServerGroup fallbackServerGroup = memcachedFallbackReadInstances.next(rsetUsed);
            if (fallbackServerGroup == null || fallbackServerGroup.equals(rsetUsed)) {
                return null;
            }
            final List<EVCacheClient> clients = memcachedReadInstancesByServerGroup.get(fallbackServerGroup);
            return selectClient(clients);
        } catch (Throwable t) {
            log.error("Exception trying to get an readable EVCache Instances for zone {}", rsetUsed, t);
            return null;
        }
    }

    public EVCacheClient getEVCacheClient(ServerGroup serverGroup) {
        if (memcachedReadInstancesByServerGroup == null || memcachedReadInstancesByServerGroup.isEmpty()) {
            if (log.isDebugEnabled()) log.debug("memcachedReadInstancesByServerGroup : " + memcachedReadInstancesByServerGroup);
            if(asyncRefreshExecutor.getQueue().isEmpty()) refreshPool(true, true);
            return null;
        }

        try {
            List<EVCacheClient> clients = memcachedReadInstancesByServerGroup.get(serverGroup);
            if (clients == null) {
                final ServerGroup fallbackServerGroup = memcachedFallbackReadInstances.next();
                if (fallbackServerGroup == null) {
                    if (log.isDebugEnabled()) log.debug("fallbackServerGroup is null.");
                    return null;
                }
                clients = memcachedReadInstancesByServerGroup.get(fallbackServerGroup);
            }
            return selectClient(clients);
        } catch (Throwable t) {
            log.error("Exception trying to get an readable EVCache Instances for ServerGroup {}", serverGroup, t);
            return null;
        }
    }

    public List<EVCacheClient> getEVCacheClientsForReadExcluding(ServerGroup serverGroupToExclude) {
        if (memcachedReadInstancesByServerGroup == null || memcachedReadInstancesByServerGroup.isEmpty()) {
            if (log.isDebugEnabled()) log.debug("memcachedReadInstancesByServerGroup : " + memcachedReadInstancesByServerGroup);
            if(asyncRefreshExecutor.getQueue().isEmpty()) refreshPool(true, true);
            return Collections.<EVCacheClient> emptyList();
        }
        try {
            if (_retryAcrossAllReplicas.get()) {
                List<EVCacheClient> clients = new ArrayList<EVCacheClient>(memcachedReadInstancesByServerGroup.size() - 1);
                for (Iterator<ServerGroup> itr = memcachedReadInstancesByServerGroup.keySet().iterator(); itr
                        .hasNext();) {
                    final ServerGroup serverGroup = itr.next();
                    if (serverGroup.equals(serverGroupToExclude)) continue;

                    final List<EVCacheClient> clientList = memcachedReadInstancesByServerGroup.get(serverGroup);
                    final EVCacheClient client = selectClient(clientList);
                    if (client != null) clients.add(client);
                }
                return clients;
            } else {
                if(_maxRetries.get() == 1) {
                    final EVCacheClient client = getEVCacheClientForReadExclude(serverGroupToExclude);
                    if (client != null) return Collections.singletonList(client);
                } else {
                    int maxNumberOfPossibleRetries = memcachedReadInstancesByServerGroup.size() - 1;
                    if(maxNumberOfPossibleRetries > _maxRetries.get()) {
                        maxNumberOfPossibleRetries = _maxRetries.get();
                    }
                    final List<EVCacheClient> clients = new ArrayList<EVCacheClient>(_maxRetries.get());
                    for(int i = 0; i < maxNumberOfPossibleRetries; i++) {
                        ServerGroup fallbackServerGroup = memcachedFallbackReadInstances.next(serverGroupToExclude);
                        if (fallbackServerGroup == null ) {
                            return clients;
                        }

                        final List<EVCacheClient> clientList = memcachedReadInstancesByServerGroup.get(fallbackServerGroup);
                        final EVCacheClient client = selectClient(clientList);
                        if (client != null) clients.add(client);
                    }
                    return clients;
                }
            }
        } catch (Throwable t) {
            log.error("Exception trying to get an readable EVCache Instances for zone {}", serverGroupToExclude, t);
        }
        return Collections.<EVCacheClient> emptyList();
    }

    public boolean isInWriteOnly(ServerGroup serverGroup) {
          if (memcachedReadInstancesByServerGroup.containsKey(serverGroup)) {
              return false;
          }
          if(memcachedWriteInstancesByServerGroup.containsKey(serverGroup)) {
              return true;
          }
          return false;
    }

    public EVCacheClient[] getWriteOnlyEVCacheClients() {
        try {
            if((cloneWrite.get().size() == 0)) {
                int size = memcachedWriteInstancesByServerGroup.size() - memcachedReadInstancesByServerGroup.size();
                if (size == 0) return new EVCacheClient[0];
                final EVCacheClient[] clientArr = new EVCacheClient[size];
                for (ServerGroup serverGroup : memcachedWriteInstancesByServerGroup.keySet()) {
                    if (!memcachedReadInstancesByServerGroup.containsKey(serverGroup)) {
                        final List<EVCacheClient> clients = memcachedWriteInstancesByServerGroup.get(serverGroup);
                        if (clients.size() == 1) {
                            clientArr[--size] = clients.get(0); // frequently used use case
                        } else {
                            final long currentVal = numberOfModOps.incrementAndGet();
                            final int index = (int) (currentVal % clients.size());
                            clientArr[--size] = (index < 0) ? clients.get(0) : clients.get(index);
                        }
                    }
                }
                return clientArr;
            } else {
                final List<EVCacheClient> evcacheClientList = new ArrayList<EVCacheClient>();
                for(String cloneApp : cloneWrite.get()) {
                    final EVCacheClient[] clients = manager.getEVCacheClientPool(cloneApp).getWriteOnlyEVCacheClients();
                    if(clients == null || clients.length == 0) continue;
                    for(int i = 0; i < clients.length; i++) {
                        evcacheClientList.add(clients[i]);
                    }
                }
                for (ServerGroup serverGroup : memcachedWriteInstancesByServerGroup.keySet()) {
                    if (!memcachedReadInstancesByServerGroup.containsKey(serverGroup)) {
                        final List<EVCacheClient> clients = memcachedWriteInstancesByServerGroup.get(serverGroup);
                        if (clients.size() == 1) {
                            evcacheClientList.add(clients.get(0)); // frequently used use case
                        } else {
                            final long currentVal = numberOfModOps.incrementAndGet();
                            final int index = (int) (currentVal % clients.size());
                            evcacheClientList.add((index < 0) ? clients.get(0) : clients.get(index));
                        }
                    }
                }
                return evcacheClientList.toArray(new EVCacheClient[0]);
            }
        } catch (Throwable t) {
            log.error("Exception trying to get an array of writable EVCache Instances", t);
            return new EVCacheClient[0];
        }
    }

    EVCacheClient[] getAllWriteClients() {
        try {
            if(allEVCacheWriteClients != null) {
                final EVCacheClient[] clientArray = allEVCacheWriteClients.next();
                if(clientArray != null && clientArray.length > 0 ) {
                    if (log.isDebugEnabled()) log.debug("allEVCacheWriteClients : " + allEVCacheWriteClients);
                    if(asyncRefreshExecutor.getQueue().isEmpty()) refreshPool(true, true);
                    return clientArray;
                }
            }
            final EVCacheClient[] clientArr = new EVCacheClient[memcachedWriteInstancesByServerGroup.size()];
            int i = 0;
            for (ServerGroup serverGroup : memcachedWriteInstancesByServerGroup.keySet()) {
                final List<EVCacheClient> clients = memcachedWriteInstancesByServerGroup.get(serverGroup);
                if (clients.size() == 1) {
                    clientArr[i++] = clients.get(0); // frequently used usecase
                } else {
                    final long currentVal = numberOfModOps.incrementAndGet();
                    final int index = (int) (currentVal % clients.size());
                    clientArr[i++] = (index < 0) ? clients.get(0) : clients.get(index);
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
            if((cloneWrite.get().size() == 0)) {
                return getAllWriteClients();
            } else {
                final List<EVCacheClient> evcacheClientList = new ArrayList<EVCacheClient>();
                final EVCacheClient[] clientArr = getAllWriteClients();
                for(EVCacheClient client : clientArr) {
                    evcacheClientList.add(client);
                }
                for(String cloneApp : cloneWrite.get()) {
                    final EVCacheClient[] cloneWriteArray = manager.getEVCacheClientPool(cloneApp).getAllWriteClients();
                    for(int j = 0; j < cloneWriteArray.length; j++) {
                        evcacheClientList.add(cloneWriteArray[j]);
                    }
                }
                return evcacheClientList.toArray(new EVCacheClient[0]);
            }
        } catch (Throwable t) {
            log.error("Exception trying to get an array of writable EVCache Instances", t);
            return new EVCacheClient[0];
        }
    }

    private void refresh() throws IOException {
        refresh(false);
    }

    protected boolean haveInstancesInServerGroupChanged(ServerGroup serverGroup, Set<InetSocketAddress> discoveredHostsInServerGroup) {
        final List<EVCacheClient> clients = memcachedInstancesByServerGroup.get(serverGroup);

        // 1. if we have discovered instances in zone but not in our map then
        // return immediately
        if (clients == null) return true;
        // 2. Do a quick check based on count (active, inactive and discovered)
        for (int i = 0; i < clients.size(); i++) {
            final int size = clients.size();
            final EVCacheClient client = clients.get(i);
            final EVCacheConnectionObserver connectionObserver = client.getConnectionObserver();
            final int activeServerCount = connectionObserver.getActiveServerCount();
            final int inActiveServerCount = connectionObserver.getInActiveServerCount();
            final int sizeInDiscovery = discoveredHostsInServerGroup.size();
            final int sizeInHashing = client.getNodeLocator().getAll().size();
            if (i == 0) getConfigGauge("sizeInDiscovery", serverGroup).set(Long.valueOf(sizeInDiscovery));
            if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + serverGroup + "\n\tActive Count : " + activeServerCount
                    + "\n\tInactive Count : " + inActiveServerCount + "\n\tDiscovery Count : " + sizeInDiscovery + "\n\tsizeInHashing : " + sizeInHashing);
            if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + serverGroup
                    + "\n\tActive Count : " + activeServerCount + "\n\tInactive Count : "
                    + inActiveServerCount + "\n\tDiscovery Count : " + sizeInDiscovery + "\n\tsizeInHashing : " + sizeInHashing);
            final long currentTime = System.currentTimeMillis();
            boolean reconcile = false;
            if (currentTime - lastReconcileTime > reconcileInterval.get()) {
                reconcile = true;
                lastReconcileTime = currentTime;
                getConfigGauge(EVCacheMetricsFactory.POOL_RECONCILE, serverGroup).set(Long.valueOf(1));
            } else {
                getConfigGauge(EVCacheMetricsFactory.POOL_RECONCILE, serverGroup).set(Long.valueOf(0));
            }
            final boolean hashingSizeDiff = (sizeInHashing != sizeInDiscovery && sizeInHashing != activeServerCount);
            if (reconcile || activeServerCount != sizeInDiscovery || inActiveServerCount > 0 || hashingSizeDiff) {
                if (log.isDebugEnabled()) log.debug("\n\t" + _appName + " & " + serverGroup
                        + " experienced an issue.\n\tActive Server Count : " + activeServerCount);
                if (log.isDebugEnabled()) log.debug("\n\tInActive Server Count : " + inActiveServerCount
                        + "\n\tDiscovered Instances : " + sizeInDiscovery);

                // 1. If a host is in discovery and we don't have an active or
                // inActive connection to it then we will have to refresh our
                // list. Typical case is we have replaced an existing node or
                // expanded the cluster.
                for (InetSocketAddress instance : discoveredHostsInServerGroup) {
                    if (!connectionObserver.getActiveServers().containsKey(instance) && !connectionObserver.getInActiveServers().containsKey(instance)) {
                        if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + serverGroup
                                + "; instance : " + instance
                                + " not found and will shutdown the client and init it again.");
                        getConfigGauge(EVCacheMetricsFactory.POOL_CHANGED, serverGroup).set(Long.valueOf(1));
                        return true;
                    }
                }

                // 2. If a host is not in discovery and is
                // inActive for more than 15 mins then we will have to refresh our
                // list. Typical case is we have replaced an existing node or
                // decreasing the cluster. Replacing an instance should not take
                // more than 20 mins (http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/monitoring-system-instance-status-check.html#types-of-instance-status-checks).
                // Even if it does then we will refresh the client twice which
                // should be ok.
                // NOTE : For a zombie instance this will mean that it will take
                // 15 mins after detaching and taking it OOS to be removed
                // unless we force a refresh
                // 12/5/2015 - Should we even do this anymore
                for (Entry<InetSocketAddress, Long> entry : connectionObserver.getInActiveServers().entrySet()) {
                    if ((currentTime - entry.getValue().longValue()) > 1200000 && !discoveredHostsInServerGroup.contains(entry.getKey())) {
                        if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + serverGroup + "; instance : " + entry.getKey()
                                + " not found in discovery and will shutdown the client and init it again.");
                        getConfigGauge(EVCacheMetricsFactory.POOL_CHANGED, serverGroup).set(Long.valueOf(2));
                        return true;
                    }
                }

                // 3. Check to see if there are any inactive connections. If we
                // find inactive connections and this node is not in discovery
                // then we will refresh the client.
                final Collection<MemcachedNode> allNodes = client.getNodeLocator().getAll();
                for (MemcachedNode node : allNodes) {
                    if (node instanceof EVCacheNodeImpl) {
                        final EVCacheNodeImpl evcNode = ((EVCacheNodeImpl) node);
                        // If the connection to a node is not active then we
                        // will reconnect the client.
                        if (!evcNode.isActive() && !discoveredHostsInServerGroup.contains(evcNode.getSocketAddress())) {
                            if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + serverGroup
                                    + "; Node : " + node + " is not active. Will shutdown the client and init it again.");

                            getConfigGauge(EVCacheMetricsFactory.POOL_CHANGED, serverGroup).set(Long.valueOf(3));
                            return true;
                        }
                    }
                }

                // 4. if there is a difference in the number of nodes in the
                // KetamaHashingMap then refresh
                if (hashingSizeDiff) {
                    if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + serverGroup
                            + "; PoolSize : " + size + "; ActiveConnections : " + activeServerCount
                            + "; InactiveConnections : " + inActiveServerCount + "; InDiscovery : " + sizeInDiscovery
                            + "; InHashing : " + sizeInHashing + "; hashingSizeDiff : " + hashingSizeDiff
                            + ". Since there is a diff in hashing size will shutdown the client and init it again.");

                    getConfigGauge(EVCacheMetricsFactory.POOL_CHANGED, serverGroup).set(Long.valueOf(4));
                    return true;
                }

                // 5. If a host is in not discovery and we have an active connection to it for more than 20 mins then we will refresh
                // Typical case is we have replaced an existing node but it has zombie. We are able to connect to it (hypervisor) but not talk to it
                // or prana has shutdown successfully but not memcached. In such scenario we will refresh the cluster
                for(InetSocketAddress instance : connectionObserver.getActiveServers().keySet()) {
                    if(!discoveredHostsInServerGroup.contains(instance)) {
                        if(!evCacheDiscoveryConnectionLostSet.containsKey(instance)) {
                            evCacheDiscoveryConnectionLostSet.put(instance, Long.valueOf(currentTime));
                            if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + serverGroup
                                    + "; instance : " + instance + " not found in discovery. We will add to our list and monitor it.");
                        } else {
                            long lostDur = (currentTime - evCacheDiscoveryConnectionLostSet.get(instance).longValue());
                            if (lostDur >= 1200000) {
                                if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + serverGroup
                                        + "; instance : " + instance + " not found in discovery for the past 20 mins and will shutdown the client and init it again.");
                                getConfigGauge(EVCacheMetricsFactory.POOL_CHANGED, serverGroup).set(Long.valueOf(5));
                                evCacheDiscoveryConnectionLostSet.remove(instance);
                                return true;
                            } else {
                                if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + serverGroup
                                        + "; instance : " + instance + " not found in discovery for " + lostDur + " msec.");
                            }
                        }
                    }
                }

                // 9. If we have removed all instances or took them OOS in a
                // ServerGroup then shutdown the client
                if (sizeInDiscovery == 0) {
                    if (activeServerCount == 0 || inActiveServerCount > activeServerCount) {
                        if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + serverGroup
                                + "; Will shutdown the client since there are no active servers and no servers for this ServerGroup in disocvery.");
                        getConfigGauge(EVCacheMetricsFactory.POOL_CHANGED, serverGroup).set(Long.valueOf(9));
                        return true;
                    }
                }
            }
            getConfigGauge(EVCacheMetricsFactory.POOL_CHANGED, serverGroup).set(Long.valueOf(0));
        }
        reportPoolConifg();
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
        if (clients == null || clients.isEmpty()) return;

        // Shutdown the old clients in 60 seconds, this will give ample time to
        // cleanup anything pending in its queue
        for (EVCacheClient oldClient : clients) {
            try {
                final boolean obsRemoved = oldClient.removeConnectionObserver();
                if (log.isDebugEnabled()) log.debug("Connection observer removed " + obsRemoved);
                final boolean status = oldClient.shutdown(60, TimeUnit.SECONDS);
                if (log.isDebugEnabled()) log.debug("Shutting down -> Client {" + oldClient.toString() + "}; status : "
                        + status);
            } catch (Exception ex) {
                log.error("Exception while shutting down the old Client", ex);
            }
        }
    }

    private void setupNewClientsByServerGroup(ServerGroup serverGroup, List<EVCacheClient> newClients) {
        final List<EVCacheClient> currentClients = memcachedInstancesByServerGroup.put(serverGroup, newClients);

        // if the zone is in write only mode then remove it from the Map
        final Property<Boolean> isZoneInWriteOnlyMode = writeOnlyFastPropertyMap.get(serverGroup);
        if (isZoneInWriteOnlyMode.get().booleanValue()) {
            memcachedReadInstancesByServerGroup.remove(serverGroup);
        } else {
            memcachedReadInstancesByServerGroup.put(serverGroup, newClients);
        }
        memcachedWriteInstancesByServerGroup.put(serverGroup, newClients);
        setupAllEVCacheWriteClientsArray();

        if (currentClients == null || currentClients.isEmpty()) return;

        // Now since we have replace the old instances shutdown all the old
        // clients
        if (log.isDebugEnabled()) log.debug("Replaced an existing Pool for ServerGroup : " + serverGroup + "; and app "
                + _appName + " ;\n\tOldClients : " + currentClients + ";\n\tNewClients : " + newClients);
        for (EVCacheClient client : currentClients) {
            if (!client.isShutdown()) {
                if (log.isDebugEnabled()) log.debug("Shutting down in Fallback -> AppName : " + _appName
                        + "; ServerGroup : " + serverGroup + "; client {" + client + "};");
                try {
                    if (client.getConnectionObserver() != null) {
                        final boolean obsRemoved = client.removeConnectionObserver();
                        if (log.isDebugEnabled()) log.debug("Connection observer removed " + obsRemoved);
                    }
                    final boolean status = client.shutdown(5, TimeUnit.SECONDS);
                    if (log.isDebugEnabled()) log.debug("Shutting down {" + client + "} ; status : " + status);
                } catch (Exception ex) {
                    log.error("Exception while shutting down the old Client", ex);
                }
            }
        }

        // Paranoid Here. Even though we have shutdown the old clients do it
        // again as we noticed issues while shutting down MemcachedNodes
        shutdownClientsInZone(currentClients);
    }

    // Check if a zone has been moved to Write only. If so, remove the app from
    // the read map.
    // Similarly if the app has been moved to Read+Write from write only add it
    // back to the read map.
    private void updateMemcachedReadInstancesByZone() {
        for (ServerGroup serverGroup : memcachedInstancesByServerGroup.keySet()) {
            final Property<Boolean> isZoneInWriteOnlyMode = writeOnlyFastPropertyMap.get(serverGroup);
            if (isZoneInWriteOnlyMode.get().booleanValue()) {
                if (memcachedReadInstancesByServerGroup.containsKey(serverGroup)) {
                    memcachedReadInstancesByServerGroup.remove(serverGroup);
                }
            } else {
                if (!memcachedReadInstancesByServerGroup.containsKey(serverGroup)) {
                    memcachedReadInstancesByServerGroup.put(serverGroup, memcachedInstancesByServerGroup.get(serverGroup));
                }
            }

            // if we lose over 50% of instances put that zone in writeonly mode.
            final List<EVCacheClient> clients = memcachedReadInstancesByServerGroup.get(serverGroup);
            if (clients != null && !clients.isEmpty()) {
                final EVCacheClient client = clients.get(0);
                if (client != null) {
                    final EVCacheConnectionObserver connectionObserver = client.getConnectionObserver();
                    if (connectionObserver != null) {
                        final int activeServerCount = connectionObserver.getActiveServerCount();
                        final int inActiveServerCount = connectionObserver.getInActiveServerCount();
                        if (inActiveServerCount > activeServerCount) {
                            memcachedReadInstancesByServerGroup.remove(serverGroup);
                            getConfigGauge(EVCacheMetricsFactory.POOL_SERVERGROUP_STATUS, serverGroup).set(Long.valueOf(1));
                        } else {
                            getConfigGauge(EVCacheMetricsFactory.POOL_SERVERGROUP_STATUS, serverGroup).set(Long.valueOf(2));
                        }
                    }
                }
            } else {
                final List<EVCacheClient> clientsWrite = memcachedInstancesByServerGroup.get(serverGroup);
                if (clientsWrite != null && !clientsWrite.isEmpty()) {
                    getConfigGauge(EVCacheMetricsFactory.POOL_SERVERGROUP_STATUS, serverGroup).set(Long.valueOf(0));
                }
            }
        }

        if (memcachedReadInstancesByServerGroup.size() != memcachedFallbackReadInstances.getSize()) {
            memcachedFallbackReadInstances = new ServerGroupCircularIterator(memcachedReadInstancesByServerGroup.keySet());

            Map<String, Set<ServerGroup>> readServerGroupByZoneMap = new ConcurrentHashMap<String, Set<ServerGroup>>();
            for (ServerGroup serverGroup : memcachedReadInstancesByServerGroup.keySet()) {
                Set<ServerGroup> serverGroupList = readServerGroupByZoneMap.get(serverGroup.getZone());
                if (serverGroupList == null) {
                    serverGroupList = new HashSet<ServerGroup>();
                    readServerGroupByZoneMap.put(serverGroup.getZone(), serverGroupList);
                }
                serverGroupList.add(serverGroup);
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
                if (connectionObserver.getActiveServerCount() == 0 && connectionObserver.getInActiveServerCount() > 0) {
                    removeEntry = true;
                }
            }
            if (force || removeEntry) {
                final ServerGroup serverGroup = serverGroupEntry.getKey();
                memcachedReadInstancesByServerGroup.remove(serverGroup);
                memcachedWriteInstancesByServerGroup.remove(serverGroup);
                for (EVCacheClient client : instancesInAServerGroup) {
                    if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + serverGroup + " has no active servers. Cleaning up this ServerGroup.");
                    client.shutdown(0, TimeUnit.SECONDS);
                    client.getConnectionObserver().shutdown();
                }
                it.remove();
                allEVCacheWriteClients = null;
            }

        }
    }

    private synchronized void refresh(boolean force) throws IOException {
        final long start = System.currentTimeMillis();
        if (log.isDebugEnabled()) log.debug("refresh APP : " + _appName + "; force : " + force);
        try {
            final Map<ServerGroup, EVCacheServerGroupConfig> instances = provider.discoverInstances(_appName);
            if (log.isDebugEnabled()) log.debug("instances : " + instances);
            // if no instances are found check to see if a clean up is needed
            // and bail immediately.
            if (instances == null || instances.isEmpty()) {
                if (!memcachedInstancesByServerGroup.isEmpty()) cleanupMemcachedInstances(false);
                return;
            }

            boolean updateAllEVCacheWriteClients = false;
            for (Entry<ServerGroup, EVCacheServerGroupConfig> serverGroupEntry : instances.entrySet()) {
                final ServerGroup serverGroup = serverGroupEntry.getKey();
                final EVCacheServerGroupConfig config = serverGroupEntry.getValue();
                final Set<InetSocketAddress> discoverdInstanceInServerGroup = config.getInetSocketAddress();
                final String zone = serverGroup.getZone();
                final Set<InetSocketAddress> discoveredHostsInServerGroup = (discoverdInstanceInServerGroup == null)
                        ? Collections.<InetSocketAddress> emptySet() : discoverdInstanceInServerGroup;
                if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + serverGroup
                        + "\n\tSize : " + discoveredHostsInServerGroup.size()
                        + "\n\tInstances in ServerGroup : " + discoveredHostsInServerGroup);

                if (discoveredHostsInServerGroup.size() == 0 && memcachedInstancesByServerGroup.containsKey(serverGroup)) {
                    if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + serverGroup
                            + " has no active servers. Cleaning up this ServerGroup.");
                    final List<EVCacheClient> clients = memcachedInstancesByServerGroup.remove(serverGroup);
                    memcachedReadInstancesByServerGroup.remove(serverGroup);
                    memcachedWriteInstancesByServerGroup.remove(serverGroup);
                    setupAllEVCacheWriteClientsArray();
                    for (EVCacheClient client : clients) {
                        if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + serverGroup
                                + "\n\tClient : " + client + " will be shutdown in 30 seconds.");
                        client.shutdown(30, TimeUnit.SECONDS);
                        client.getConnectionObserver().shutdown();
                    }
                    continue;
                }

                boolean instanceChangeInServerGroup = force;
                if (instanceChangeInServerGroup) {
                    if (log.isWarnEnabled()) log.warn("FORCE REFRESH :: AppName :" + _appName + "; ServerGroup : "
                            + serverGroup + "; Changed : " + instanceChangeInServerGroup);
                } else {
                    instanceChangeInServerGroup = haveInstancesInServerGroupChanged(serverGroup, discoveredHostsInServerGroup);
                    if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + serverGroup
                            + "\n\tinstanceChangeInServerGroup : " + instanceChangeInServerGroup);
                    if (!instanceChangeInServerGroup) {
                        // quick exit as everything looks fine. No new instances
                        // found and were inactive
                        if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + serverGroup
                                + "; Changed : " + instanceChangeInServerGroup);
                        continue;
                    }
                }

                // Let us create a list of SocketAddress from the discovered
                // instances in zone
                final List<InetSocketAddress> memcachedSAInServerGroup = getMemcachedSocketAddressList(discoveredHostsInServerGroup);

                if (memcachedSAInServerGroup.size() > 0) {
                    // now since there is a change with the instances in the
                    // zone. let us go ahead and create a new EVCacheClient with
                    // the new settings
                    final int poolSize = _poolSize.get();
                    final List<EVCacheClient> newClients = new ArrayList<EVCacheClient>(poolSize);
                    for (int i = 0; i < poolSize; i++) {
                        final int maxQueueSize = EVCacheConfig.getInstance().getPropertyRepository().get(_appName + ".max.queue.length", Integer.class).orElse(16384).get();
                        EVCacheClient client;
                        try {
                            client = new EVCacheClient(_appName, zone, i, config, memcachedSAInServerGroup, maxQueueSize,
                            		_maxReadQueueSize, _readTimeout, _bulkReadTimeout, _opQueueMaxBlockTime, _operationTimeout, this);
                            newClients.add(client);
                            final int id = client.getId();
                            if (log.isDebugEnabled()) log.debug("AppName :" + _appName + "; ServerGroup : " + serverGroup + "; intit : client.getId() : " + id);
                            lastReconcileTime = System.currentTimeMillis();
                        } catch (Exception e) {
                            incrementFailure(EVCacheMetricsFactory.INTERNAL_POOL_INIT_ERROR, config.getServerGroup());
                            log.error("Unable to create EVCacheClient for app - " + _appName + " and Server Group - " + serverGroup.getName(), e);
                        }
                    }
                    if (newClients.size() > 0) {
                        setupNewClientsByServerGroup(serverGroup, newClients);
                        updateAllEVCacheWriteClients = true;
                    }
                }
            }

            if(updateAllEVCacheWriteClients) {
                setupAllEVCacheWriteClientsArray();
            }

            // Check to see if a zone has been removed, if so remove them from
            // the active list
            if (memcachedInstancesByServerGroup.size() > instances.size()) {
                if (log.isDebugEnabled()) log.debug("\n\tAppName :" + _appName + ";\n\tServerGroup Discovered : " + instances.keySet()
                        + ";\n\tCurrent ServerGroup in EVCache Client : " + memcachedInstancesByServerGroup.keySet());
                cleanupMemcachedInstances(false);
            }
            updateMemcachedReadInstancesByZone();
            updateQueueStats();
            if (_pingServers.get()) pingServers();
        } catch (Throwable t) {
            log.error("Exception while refreshing the Server list", t);
        } finally {
            EVCacheMetricsFactory.getInstance().getPercentileTimer(EVCacheMetricsFactory.INTERNAL_POOL_REFRESH, tagList, Duration.ofMillis(100)).record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        }

        if (log.isDebugEnabled()) log.debug("refresh APP : " + _appName + "; DONE");
    }

    private void setupAllEVCacheWriteClientsArray() {
        final List<EVCacheClient[]> newClients = new ArrayList<EVCacheClient[]>(_poolSize.get());
        try {
            final int serverGroupSize = memcachedWriteInstancesByServerGroup.size();
            for(int ind = 0; ind < _poolSize.get(); ind++) {
                final EVCacheClient[] clientArr = new EVCacheClient[serverGroupSize];
                int i = 0;
                for (ServerGroup serverGroup : memcachedWriteInstancesByServerGroup.keySet()) {
                    final List<EVCacheClient> clients = memcachedWriteInstancesByServerGroup.get(serverGroup);
                    if(clients.size() > ind) {
                    	clientArr[i++] = clients.get(ind); // frequently used usecase
                    } else {
                    	log.warn("Incorrect pool size detected for AppName : " + _appName + "; PoolSize " + _poolSize.get() + "; serverGroup : " + serverGroup + "; ind : " + ind + "; i : " + i);
                    	if(clients.size() > 0) {
                    		clientArr[i++] = clients.get(0);
                    	}
                    }
                }
                newClients.add(clientArr);
            }
            this.allEVCacheWriteClients = new CircularIterator<EVCacheClient[]>(newClients);
        } catch (Throwable t) {
            log.error("Exception trying to create an array of writable EVCache Instances for App : " + _appName, t);
        }
    }

    private void updateQueueStats() {
        for (ServerGroup serverGroup : memcachedInstancesByServerGroup.keySet()) {
            List<EVCacheClient> clients = memcachedInstancesByServerGroup.get(serverGroup);
            for(EVCacheClient client : clients) {
                getStatsGauge(EVCacheMetricsFactory.POOL_WRITE_Q_SIZE, client).set(Long.valueOf(client.getWriteQueueLength()));
                getStatsGauge(EVCacheMetricsFactory.POOL_READ_Q_SIZE, client).set(Long.valueOf(client.getReadQueueLength()));
                if(refreshConnectionOnReadQueueFull.get()) {
                    final Collection<MemcachedNode> allNodes = client.getNodeLocator().getAll();
                    for (MemcachedNode node : allNodes) {
                        if (node instanceof EVCacheNodeImpl) {
                            final EVCacheNodeImpl evcNode = ((EVCacheNodeImpl) node);
                            if(evcNode.getReadQueueSize() >= refreshConnectionOnReadQueueFullSize.get().intValue()) {
                                EVCacheMetricsFactory.getInstance().getCounter(EVCacheMetricsFactory.POOL_REFRESH_QUEUE_FULL, evcNode.getTags()).increment();
                                client.getEVCacheMemcachedClient().reconnectNode(evcNode);
                            }
                        }
                    }
                }
            }
        }
    }

    public void pingServers() {
        try {
            final Map<ServerGroup, List<EVCacheClient>> allServers = getAllInstancesByZone();
            for (Entry<ServerGroup, List<EVCacheClient>> entry : allServers.entrySet()) {
                final List<EVCacheClient> listOfClients = entry.getValue();
                for (EVCacheClient client : listOfClients) {
                    final Map<SocketAddress, String> versions = client.getVersions();
                    for (Entry<SocketAddress, String> vEntry : versions.entrySet()) {
                        if (log.isDebugEnabled()) log.debug("Host : " + vEntry.getKey() + " : " + vEntry.getValue());
                    }
                }
            }
        } catch (Throwable t) {
            log.error("Error while pinging the servers", t);
        }
    }

    public void serverGroupDisabled(final ServerGroup serverGroup) {
        if (memcachedInstancesByServerGroup.containsKey(serverGroup)) {
            if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + serverGroup
                    + " has no active servers. Cleaning up this ServerGroup.");
            final List<EVCacheClient> clients = memcachedInstancesByServerGroup.remove(serverGroup);
            memcachedReadInstancesByServerGroup.remove(serverGroup);
            memcachedWriteInstancesByServerGroup.remove(serverGroup);
            setupAllEVCacheWriteClientsArray();
            for (EVCacheClient client : clients) {
                if (log.isDebugEnabled()) log.debug("\n\tApp : " + _appName + "\n\tServerGroup : " + serverGroup
                        + "\n\tClient : " + client + " will be shutdown in 30 seconds.");
                client.shutdown(30, TimeUnit.SECONDS);
                client.getConnectionObserver().shutdown();
            }
        }
    }

    public void refreshAsync(MemcachedNode node) {
        if (log.isInfoEnabled()) log.info("Pool is being refresh as the EVCacheNode is not available. " + node.toString());
        if(!_disableAsyncRefresh.get()) {
            if (node instanceof EVCacheNodeImpl) {
                final EVCacheNodeImpl evcNode = ((EVCacheNodeImpl) node);
                EVCacheMetricsFactory.getInstance().getCounter(EVCacheMetricsFactory.POOL_REFRESH_ASYNC, evcNode.getTags()).increment();
            }
            boolean force = (System.currentTimeMillis() - lastReconcileTime) > ( manager.getDefaultRefreshInterval().get() * 1000 ) ? true : false;
            if(!force) force = !node.isActive();
            refreshPool(true, force);
        }
    }

    public void run() {
        try {
            refresh();
        } catch (Throwable t) {
            if (log.isDebugEnabled()) log.debug("Error Refreshing EVCache Instance list for " + _appName, t);
        }
    }

    void shutdown() {
        if (log.isDebugEnabled()) log.debug("EVCacheClientPool for App : " + _appName + " and Zone : " + _zone + " is being shutdown.");
        _shutdown = true;
        for (List<EVCacheClient> instancesInAZone : memcachedInstancesByServerGroup.values()) {
            for (EVCacheClient client : instancesInAZone) {
                client.shutdown(30, TimeUnit.SECONDS);
                client.getConnectionObserver().shutdown();
            }
        }
        setupMonitoring();
    }

    private Gauge getConfigGauge(String metric, ServerGroup serverGroup) {
        final String name = (serverGroup == null ? metric : metric + serverGroup.getName() + isInWriteOnly(serverGroup));
        Gauge gauge = gaugeMap.get(name );
        if(gauge != null) return gauge;

        final List<Tag> tags = new ArrayList<Tag>(4);
        tags.add(new BasicTag(EVCacheMetricsFactory.CACHE, _appName));
        tags.add(new BasicTag(EVCacheMetricsFactory.CONFIG_NAME, metric));
        if(serverGroup != null) {
            tags.add(new BasicTag(EVCacheMetricsFactory.SERVERGROUP, serverGroup.getName()));
        }

        final Id id = EVCacheMetricsFactory.getInstance().getId(EVCacheMetricsFactory.INTERNAL_POOL_SG_CONFIG, tags);
        gauge = EVCacheMetricsFactory.getInstance().getRegistry().gauge(id);
        gaugeMap.put(name, gauge);
        return gauge;
    }

    private Gauge getStatsGauge(String metric, EVCacheClient client) {
        final String name = metric + client.getServerGroupName();
        Gauge gauge = gaugeMap.get(name );
        if(gauge != null) return gauge;

        final List<Tag> tags = new ArrayList<Tag>(3);
        tags.add(new BasicTag(EVCacheMetricsFactory.CACHE, _appName));
        tags.add(new BasicTag(EVCacheMetricsFactory.STAT_NAME, metric));
        tags.add(new BasicTag(EVCacheMetricsFactory.CONNECTION_ID, String.valueOf(client.getId())));
        tags.add(new BasicTag(EVCacheMetricsFactory.SERVERGROUP, client.getServerGroupName()));

        final Id id = EVCacheMetricsFactory.getInstance().getId(EVCacheMetricsFactory.INTERNAL_STATS, tags);
        gauge = EVCacheMetricsFactory.getInstance().getRegistry().gauge(id);
        gaugeMap.put(name, gauge);
        return gauge;
    }

    private void incrementFailure(String metric, ServerGroup serverGroup) {

        final List<Tag> tags = new ArrayList<Tag>(3);
        tags.add(new BasicTag(EVCacheMetricsFactory.CACHE, _appName));
        tags.add(new BasicTag(EVCacheMetricsFactory.CONFIG_NAME, metric));
        tags.add(new BasicTag(EVCacheMetricsFactory.SERVERGROUP, serverGroup.getName()));

        EVCacheMetricsFactory.getInstance().increment(EVCacheMetricsFactory.INTERNAL_POOL_INIT_ERROR, tags);
    }

    private void reportPoolConifg() {
        final int size = getPoolSize();
        for(ServerGroup key : memcachedInstancesByServerGroup.keySet()) {
            getConfigGauge("poolSize", key).set(memcachedInstancesByServerGroup.get(key).size());
            final EVCacheClient client = memcachedInstancesByServerGroup.get(key).get(0);
            if(client != null) {
                getConfigGauge("readTimeout", key).set(getReadTimeout().get());
                getConfigGauge("bulkReadTimeout", key).set(getBulkReadTimeout().get());
                getConfigGauge("numberOfServerGoups", key).set(memcachedInstancesByServerGroup.size());
                getConfigGauge("maxReadQueueLength", key).set(_maxReadQueueSize.get());
                getConfigGauge("instanceCount", key).set(client.getMemcachedNodesInZone().size());;
                final EVCacheConnectionObserver connectionObserver = client.getConnectionObserver();
                if(connectionObserver != null) {
                    final int activeServerCount = connectionObserver.getActiveServerCount();
                    final int inActiveServerCount = connectionObserver.getInActiveServerCount();
                    final int sizeInHashing = client.getNodeLocator().getAll().size();
                    getConfigGauge("activeServerCount", key).set(Long.valueOf(activeServerCount));
                    getConfigGauge("activeConnectionCount", key).set(Long.valueOf(activeServerCount * size));
                    getConfigGauge("inActiveServerCount", key).set(Long.valueOf(inActiveServerCount));
                    getConfigGauge("sizeInHashing", key).set(Long.valueOf(sizeInHashing));
                }

                final List<EVCacheClient> readClients = memcachedReadInstancesByServerGroup.get(key);
                if (readClients != null && readClients.size() > 0) {
                    getConfigGauge(EVCacheMetricsFactory.POOL_READ_INSTANCES, key).set(Long.valueOf(readClients.get(0).getConnectionObserver().getActiveServerCount()));
                }
                final List<EVCacheClient> writeClients = memcachedWriteInstancesByServerGroup.get(key);
                if (writeClients != null && writeClients.size() > 0) {
                    getConfigGauge(EVCacheMetricsFactory.POOL_WRITE_INSTANCES, key).set(Long.valueOf(writeClients.get(0).getConnectionObserver().getActiveServerCount()));
                }
            }
        }
    }

    private void setupMonitoring() {
        try {
            final ObjectName mBeanName = ObjectName.getInstance("com.netflix.evcache:Group=" + _appName
                    + ",SubGroup=pool");
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mbeanServer.isRegistered(mBeanName)) {
                if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanName + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanName);
            }
            if (!_shutdown) {
                mbeanServer.registerMBean(this, mBeanName);
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug("Exception", e);
        }
    }

    public int getInstanceCount() {
        int instances = 0;
        for (ServerGroup serverGroup : memcachedInstancesByServerGroup.keySet()) {
            instances += memcachedInstancesByServerGroup.get(serverGroup).get(0).getConnectionObserver().getActiveServerCount();
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
            instanceMap.put(key.getName(), Integer.valueOf(memcachedReadInstancesByServerGroup.get(key).get(0)
                    .getConnectionObserver().getActiveServerCount()));
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
        refreshPool(false, true);
    }

    public void refreshPool(boolean async, boolean force) {
        if (log.isDebugEnabled()) log.debug("Refresh Pool : async : " + async + "; force : " + force);
        try {
            if(async) {
                asyncRefreshExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            refresh(force);
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                });
            } else {
                refresh(force);
            }
        } catch (Throwable t) {
            if (log.isDebugEnabled()) log.debug("Error Refreshing EVCache Instance list from MBean : " + _appName, t);
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
        if (!isLogEventEnabled()) return false;
        if (!logOperationCalls.get().contains(op)) return false;
        return key.hashCode() % 1000 <= logOperations.get();
    }

    @Override
    public String getLocalServerGroupCircularIterator() {
        return (localServerGroupIterator == null) ? "NONE" : localServerGroupIterator.toString();
    }

    @Override
    public String getEVCacheWriteClientsCircularIterator() {
        return (allEVCacheWriteClients == null) ? "NONE" : allEVCacheWriteClients.toString();
    }

    public String getPoolDetails() {
        return toString();
    }

    @Override
    public String toString() {
        return "\nEVCacheClientPool [\n\t_appName=" + _appName + ",\n\t_zone=" + _zone
            + ",\n\tlocalServerGroupIterator=" + localServerGroupIterator + ",\n\t_poolSize=" + _poolSize + ",\n\t_readTimeout=" + _readTimeout
            + ",\n\t_bulkReadTimeout=" + _bulkReadTimeout + ",\n\tlogOperations=" + logOperations + ",\n\t_opQueueMaxBlockTime=" + _opQueueMaxBlockTime
            + ",\n\t_operationTimeout=" + _operationTimeout + ",\n\t_maxReadQueueSize=" + _maxReadQueueSize  + ",\n\t_pingServers=" + _pingServers
            + ",\n\twriteOnlyFastPropertyMap=" + writeOnlyFastPropertyMap + ",\n\tnumberOfModOps=" + numberOfModOps.get() + ",\n\t_shutdown=" + _shutdown
            + ",\n\tmemcachedInstancesByServerGroup=" + memcachedInstancesByServerGroup + ",\n\tmemcachedReadInstancesByServerGroup=" + memcachedReadInstancesByServerGroup
            + ",\n\tmemcachedWriteInstancesByServerGroup=" + memcachedWriteInstancesByServerGroup + ",\n\treadServerGroupByZone=" + readServerGroupByZone
            + ",\n\tmemcachedFallbackReadInstances=" + memcachedFallbackReadInstances + "\n]"
            + ", \n\tallEVCacheWriteClients=" + allEVCacheWriteClients
            + "\n]";
    }

    public int getPoolSize() {
        return _poolSize.get();
    }

    public Property<Integer> getLogOperations() {
        return logOperations;
    }

    public Property<Integer> getOpQueueMaxBlockTime() {
        return _opQueueMaxBlockTime;
    }

    public Property<Integer> getOperationTimeout() {
        return _operationTimeout;
    }

    public Property<Integer> getMaxReadQueueSize() {
        return _maxReadQueueSize;
    }

    public Property<Boolean> getPingServers() {
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

    public Map<ServerGroup, Property<Boolean>> getWriteOnlyFastPropertyMap() {
        return Collections.unmodifiableMap(writeOnlyFastPropertyMap);
    }

    public Property<Integer> getReadTimeout() {
        return _readTimeout;
    }

    public Property<Integer> getBulkReadTimeout() {
        return _bulkReadTimeout;
    }

    /*
     * Block the thread until all the queues are processed or at most 30 seconds.
     * Will return the count of items left in the queues. 0 means none left.
     */
    public int join() {
        int size = 0;
        int counter = 0;
        do {
            for(List<EVCacheClient> clientList : memcachedInstancesByServerGroup.values()) {
                for(EVCacheClient client : clientList) {
                    size +=client.getWriteQueueLength();
                    size +=client.getReadQueueLength();
                }
            }
            if(size > 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    log.error("");
                }
            }
            if(counter++ > 3000) break;
        } while(size > 0);
        return size;
    }

    public long getLastReconcileTime() {
        return lastReconcileTime;
    }

    public Property<Set<String>> getOperationToLog() {
        return logOperationCalls;
    }
}
