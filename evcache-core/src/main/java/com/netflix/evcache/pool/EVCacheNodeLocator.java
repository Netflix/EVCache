package com.netflix.evcache.pool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.netflix.archaius.api.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.EVCacheMemcachedNodeROImpl;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.util.KetamaNodeLocatorConfiguration;

public class EVCacheNodeLocator implements NodeLocator {

    private static final Logger log = LoggerFactory.getLogger(EVCacheNodeLocator.class);
    private TreeMap<Long, MemcachedNode> ketamaNodes;
    protected final EVCacheClient client;

    private final Property<Boolean> partialStringHash;
    private final Property<String> hashDelimiter;

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
    public EVCacheNodeLocator(EVCacheClient client, List<MemcachedNode> nodes, HashAlgorithm alg, KetamaNodeLocatorConfiguration conf) {
        super();
        this.allNodes = nodes;
        this.hashingAlgorithm = alg;
        this.config = conf;
        this.client = client;

        this.partialStringHash = EVCacheConfig.getInstance().getPropertyRepository().get(client.getAppName() + "." + client.getServerGroupName() + ".hash.on.partial.key", Boolean.class)
                .orElseGet(client.getAppName()+ ".hash.on.partial.key").orElse(false);
        this.hashDelimiter = EVCacheConfig.getInstance().getPropertyRepository().get(client.getAppName() + "." + client.getServerGroupName() + ".hash.delimiter", String.class)
                .orElseGet(client.getAppName() + ".hash.delimiter").orElse(":");


        setKetamaNodes(nodes);
    }

    private EVCacheNodeLocator(EVCacheClient client, TreeMap<Long, MemcachedNode> smn, Collection<MemcachedNode> an, HashAlgorithm alg, KetamaNodeLocatorConfiguration conf) {
        super();
        this.ketamaNodes = smn;
        this.allNodes = an;
        this.hashingAlgorithm = alg;
        this.config = conf;
        this.client = client;

        this.partialStringHash = EVCacheConfig.getInstance().getPropertyRepository().get(client.getAppName() + "." + client.getServerGroupName() + ".hash.on.partial.key", Boolean.class)
                .orElseGet(client.getAppName()+ ".hash.on.partial.key").orElse(false);
        this.hashDelimiter = EVCacheConfig.getInstance().getPropertyRepository().get(client.getAppName() + "." + client.getServerGroupName() + ".hash.delimiter", String.class)
                .orElseGet(client.getAppName() + ".hash.delimiter").orElse(":");
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

        final long hash = hashingAlgorithm.hash(k);

        Map.Entry<Long, MemcachedNode> entry = ketamaNodes.ceilingEntry(hash);
        if (entry == null) {
            entry = ketamaNodes.firstEntry();
        }
        return entry.getValue();
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

        return new EVCacheNodeLocator(client, ketamaNaodes, aNodes, hashingAlgorithm, config);
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
                    final String hashString = config.getKeyForNode(node, i);
                    byte[] digest = DefaultHashAlgorithm.computeMd5(hashString);
                    if (log.isDebugEnabled()) log.debug("digest : " + digest);
                    for (int h = 0; h < 4; h++) {
                        long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                                | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                                | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                                | (digest[h * 4] & 0xFF);
                        newNodeMap.put(Long.valueOf(k), node);
                        if (log.isDebugEnabled()) log.debug("Key : " + hashString + " ; hash : " + k + "; node " + node );
                    }
                }
            } else {
                for (int i = 0; i < numReps; i++) {
                    final Long hashL = Long.valueOf(hashingAlgorithm.hash(config.getKeyForNode(node, i)));
                    newNodeMap.put(hashL, node);
                }
            }
        }
        if (log.isDebugEnabled()) log.debug("NewNodeMapSize : " + newNodeMap.size() + "; MapSize : " + (numReps * nodes.size()));
        if (log.isTraceEnabled()) {
            for(Long key : newNodeMap.keySet()) {
                log.trace("Hash : " + key + "; Node : " + newNodeMap.get(key));
            }
        }
        ketamaNodes = newNodeMap;
    }

    @Override
    public void updateLocator(List<MemcachedNode> nodes) {
        setKetamaNodes(nodes);
    }

    @Override
    public String toString() {
        return "EVCacheNodeLocator [ketamaNodes=" + ketamaNodes + ", EVCacheClient=" + client + ", partialStringHash=" + partialStringHash
                + ", hashDelimiter=" + hashDelimiter + ", allNodes=" + allNodes + ", hashingAlgorithm=" + hashingAlgorithm + ", config=" + config + "]";
    }

}
