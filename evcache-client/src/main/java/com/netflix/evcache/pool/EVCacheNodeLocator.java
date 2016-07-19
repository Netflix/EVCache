package com.netflix.evcache.pool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.EVCacheMemcachedNodeROImpl;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.util.KetamaNodeLocatorConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ChainedDynamicProperty;
import com.netflix.evcache.util.EVCacheConfig;

public class EVCacheNodeLocator implements NodeLocator {

    private static Logger log = LoggerFactory.getLogger(EVCacheNodeLocator.class);
    private TreeMap<Long, MemcachedNode> ketamaNodes;
    private final String appName;
    private final ServerGroup serverGroup;

    private ChainedDynamicProperty.BooleanProperty partialStringHash;
    private ChainedDynamicProperty.StringProperty hashDelimiter;

    private final Collection<MemcachedNode> allNodes;

    private final HashAlgorithm hashingAlgorithm;
    private final KetamaNodeLocatorConfiguration config;

    /**
     * Create a new KetamaNodeLocator using specified nodes and the specifed
     * hash algorithm and configuration.
     *
     * @param nodes
     *            The List of nodes to use in the Ketama consistent hash
     *            continuum
     * @param alg
     *            The hash algorithm to use when choosing a node in the Ketama
     *            consistent hash continuum
     * @param conf
     */
    public EVCacheNodeLocator(String appName, ServerGroup serverGroup, List<MemcachedNode> nodes, HashAlgorithm alg,
            KetamaNodeLocatorConfiguration conf) {
        super();
        this.allNodes = nodes;
        this.hashingAlgorithm = alg;
        this.config = conf;
        this.appName = appName;
        this.serverGroup = serverGroup;

        this.partialStringHash = EVCacheConfig.getInstance().getChainedBooleanProperty("EVCacheNodeLocator." + appName
                + ".hash.on.partial.key", "EVCacheNodeLocator." + appName + "." + serverGroup.getName()
                        + ".hash.on.partial.key", Boolean.FALSE);
        this.hashDelimiter = EVCacheConfig.getInstance().getChainedStringProperty("EVCacheNodeLocator." + appName
                + ".hash.delimiter", "EVCacheNodeLocator." + appName + "." + serverGroup.getName() + ".hash.delimiter",
                ":");

        setKetamaNodes(nodes);
    }

    private EVCacheNodeLocator(String appName, ServerGroup serverGroup, TreeMap<Long, MemcachedNode> smn,
            Collection<MemcachedNode> an, HashAlgorithm alg, KetamaNodeLocatorConfiguration conf) {
        super();
        this.ketamaNodes = smn;
        this.allNodes = an;
        this.hashingAlgorithm = alg;
        this.config = conf;
        this.appName = appName;
        this.serverGroup = serverGroup;

        this.partialStringHash = EVCacheConfig.getInstance().getChainedBooleanProperty("EVCacheNodeLocator." + appName
                + ".hash.on.partial.key", "EVCacheNodeLocator." + appName + "." + serverGroup.getName()
                        + ".hash.on.partial.key", Boolean.FALSE);
        this.hashDelimiter = EVCacheConfig.getInstance().getChainedStringProperty("EVCacheNodeLocator." + appName
                + ".hash.delimiter", "EVCacheNodeLocator." + appName + "." + serverGroup.getName() + ".hash.delimiter",
                ":");
    }

    /*
     * @see net.spy.memcached.NodeLocator#getAll
     */
    public Collection<MemcachedNode> getAll() {
        return allNodes;
    }

    /*
     * @see net.spy.memcached.NodeLocator#getPrimary
     */
    public MemcachedNode getPrimary(String k) {
        if (partialStringHash.get()) {
            final int index = k.indexOf(hashDelimiter.get());
            if (index > 0) {
                k = k.substring(0, index);
            }
        }

        final long _hash = hashingAlgorithm.hash(k);
        Long hash = Long.valueOf(_hash);
        hash = ketamaNodes.ceilingKey(hash);
        if (hash == null) {
            hash = ketamaNodes.firstKey();
        }
        return ketamaNodes.get(hash);
    }

    /*
     * @return Returns the max key in the hashing distribution
     */
    public long getMaxKey() {
        return getKetamaNodes().lastKey().longValue();
    }

    public MemcachedNode getNodeForKey(long _hash) {
        long start = (log.isDebugEnabled()) ? System.nanoTime() : 0;
        try {
            Long hash = Long.valueOf(_hash);
            hash = ketamaNodes.ceilingKey(hash);
            if (hash == null) {
                hash = ketamaNodes.firstKey();
            }
            return ketamaNodes.get(hash);
        } finally {
            if (log.isDebugEnabled()) {
                final long end = System.nanoTime();
                log.debug("getNodeForKey : \t" + (end - start) / 1000);
            }
        }
    }

    public Iterator<MemcachedNode> getSequence(String k) {
        final List<MemcachedNode> allKetamaNodes = new ArrayList<MemcachedNode>(getKetamaNodes().values());
        Collections.shuffle(allKetamaNodes);
        return allKetamaNodes.iterator();
    }

    public NodeLocator getReadonlyCopy() {
        final TreeMap<Long, MemcachedNode> ketamaNaodes = new TreeMap<Long, MemcachedNode>(getKetamaNodes());
        final Collection<MemcachedNode> aNodes = new ArrayList<MemcachedNode>(allNodes.size());

        // Rewrite the values a copy of the map.
        for (Map.Entry<Long, MemcachedNode> me : ketamaNaodes.entrySet()) {
            me.setValue(new EVCacheMemcachedNodeROImpl(me.getValue()));
        }
        // Copy the allNodes collection.
        for (MemcachedNode n : allNodes) {
            aNodes.add(new EVCacheMemcachedNodeROImpl(n));
        }

        return new EVCacheNodeLocator(appName, serverGroup, ketamaNaodes, aNodes, hashingAlgorithm, config);
    }

    /**
     * @return the ketamaNodes
     */
    protected TreeMap<Long, MemcachedNode> getKetamaNodes() {
        return ketamaNodes;
    }

    /**
     * @return the readonly view of ketamaNodes. This is mailnly for admin
     *         purposes
     */
    public Map<Long, MemcachedNode> getKetamaNodeMap() {
        return Collections.<Long, MemcachedNode> unmodifiableMap(ketamaNodes);
    }

    /**
     * Setup the KetamaNodeLocator with the list of nodes it should use.
     *
     * @param nodes
     *            a List of MemcachedNodes for this KetamaNodeLocator to use in
     *            its continuum
     */
    protected final void setKetamaNodes(List<MemcachedNode> nodes) {
        TreeMap<Long, MemcachedNode> newNodeMap = new TreeMap<Long, MemcachedNode>();
        final int numReps = config.getNodeRepetitions();
        for (MemcachedNode node : nodes) {
            // Ketama does some special work with md5 where it reuses chunks.
            if (hashingAlgorithm == DefaultHashAlgorithm.KETAMA_HASH) {
                for (int i = 0; i < numReps / 4; i++) {
                    byte[] digest = DefaultHashAlgorithm.computeMd5(config.getKeyForNode(node, i));
                    for (int h = 0; h < 4; h++) {
                        long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                                | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                                | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                                | (digest[h * 4] & 0xFF);
                        newNodeMap.put(Long.valueOf(k), node);
                        if (log.isDebugEnabled()) log.debug("Adding node " + node + " in position " + k);
                    }
                }
            } else {
                for (int i = 0; i < numReps; i++) {
                    final Long hashL = Long.valueOf(hashingAlgorithm.hash(config.getKeyForNode(node, i)));
                    newNodeMap.put(hashL, node);
                }
            }
        }
        if (log.isDebugEnabled()) log.debug("NewNodeMapSize : " + newNodeMap.size() + "; MapSize : " + (numReps * nodes
                .size()));
        ketamaNodes = newNodeMap;
    }

    @Override
    public void updateLocator(List<MemcachedNode> nodes) {
        setKetamaNodes(nodes);
    }

}