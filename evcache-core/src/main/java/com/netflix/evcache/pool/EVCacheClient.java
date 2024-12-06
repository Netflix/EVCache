package com.netflix.evcache.pool;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.function.BiPredicate;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.archaius.api.Property;
import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCache.Call;
import com.netflix.evcache.EVCacheConnectException;
import com.netflix.evcache.EVCacheException;
import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.EVCacheReadQueueException;
import com.netflix.evcache.EVCacheSerializingTranscoder;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.operation.EVCacheFutures;
import com.netflix.evcache.operation.EVCacheItem;
import com.netflix.evcache.operation.EVCacheItemMetaData;
import com.netflix.evcache.operation.EVCacheLatchImpl;
import com.netflix.evcache.pool.observer.EVCacheConnectionObserver;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.evcache.util.KeyHasher;
import com.netflix.evcache.util.KeyHasher.HashingAlgorithm;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Tag;

import net.spy.memcached.CASValue;
import net.spy.memcached.CachedData;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.EVCacheMemcachedClient;
import net.spy.memcached.EVCacheNode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.internal.ListenableFuture;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.Transcoder;
import rx.Scheduler;
import rx.Single;

@SuppressWarnings({"rawtypes", "unchecked"})
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({ "REC_CATCH_EXCEPTION",
        "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE" })
public class EVCacheClient {

    private static final Logger log = LoggerFactory.getLogger(EVCacheClient.class);
    private final ConnectionFactory connectionFactory;
    private final EVCacheMemcachedClient evcacheMemcachedClient;
    private final List<InetSocketAddress> memcachedNodesInZone;
    private EVCacheConnectionObserver connectionObserver = null;
    private boolean shutdown = false;

    private final int id;
    private final String appName;
    private final String zone;
    private final ServerGroup serverGroup;
    private final EVCacheServerGroupConfig config;
    private final int maxWriteQueueSize;

    private final Property<Integer> readTimeout;
    private final Property<Integer> bulkReadTimeout;
    private final Property<Integer> maxReadQueueSize;
    private final Property<Boolean> ignoreInactiveNodes;
    private final Property<Boolean> enableChunking;
    private final Property<Boolean> hashKeyByServerGroup;
    private final Property<Boolean> shouldEncodeHashKey;
    private final Property<Integer> maxDigestBytes;
    private final Property<Integer> maxHashLength;
    private final Property<Integer> chunkSize, writeBlock;
    private final Property<String> encoderBase;

    private final ChunkTranscoder chunkingTranscoder;
    private final EVCacheSerializingTranscoder decodingTranscoder;
    private static final int SPECIAL_BYTEARRAY = (8 << 8);
    private final EVCacheClientPool pool;
//    private Counter addCounter = null;
    private final Property<Boolean> ignoreTouch;
    private List<Tag> tags;
    private final Map<String, Counter> counterMap = new ConcurrentHashMap<String, Counter>();
    private final Property<String> hashingAlgo;
    protected final Counter operationsCounter;
    private final boolean isDuetClient;

    EVCacheClient(String appName, String zone, int id, EVCacheServerGroupConfig config,
            List<InetSocketAddress> memcachedNodesInZone, int maxQueueSize, Property<Integer> maxReadQueueSize,
                  Property<Integer> readTimeout, Property<Integer> bulkReadTimeout,
                  Property<Integer> opQueueMaxBlockTime,
                  Property<Integer> operationTimeout, EVCacheClientPool pool, boolean isDuetClient) throws IOException {
        this.memcachedNodesInZone = memcachedNodesInZone;
        this.id = id;
        this.appName = appName;
        this.zone = zone;
        this.config = config;
        this.serverGroup = config.getServerGroup();
        this.readTimeout = readTimeout;
        this.bulkReadTimeout = bulkReadTimeout;
        this.maxReadQueueSize = maxReadQueueSize;
//        this.operationTimeout = operationTimeout;
        this.pool = pool;
        this.isDuetClient = isDuetClient;

        final List<Tag> tagList = new ArrayList<Tag>(4);
        EVCacheMetricsFactory.getInstance().addAppNameTags(tagList, appName);
        tagList.add(new BasicTag(EVCacheMetricsFactory.CONNECTION_ID, String.valueOf(id)));
        tagList.add(new BasicTag(EVCacheMetricsFactory.SERVERGROUP, serverGroup.getName()));
        this.tags = Collections.<Tag>unmodifiableList(new ArrayList(tagList));

        tagList.add(new BasicTag(EVCacheMetricsFactory.STAT_NAME, EVCacheMetricsFactory.POOL_OPERATIONS));
        operationsCounter = EVCacheMetricsFactory.getInstance().getCounter(EVCacheMetricsFactory.INTERNAL_STATS, tagList);

        this.enableChunking = EVCacheConfig.getInstance().getPropertyRepository().get(this.serverGroup.getName()+ ".chunk.data", Boolean.class).orElseGet(appName + ".chunk.data").orElse(false);
        this.chunkSize = EVCacheConfig.getInstance().getPropertyRepository().get(this.serverGroup.getName() + ".chunk.size", Integer.class).orElseGet(appName + ".chunk.size").orElse(1180);
        this.writeBlock = EVCacheConfig.getInstance().getPropertyRepository().get(appName + "." + this.serverGroup.getName() + ".write.block.duration", Integer.class).orElseGet(appName + ".write.block.duration").orElse(25);
        this.chunkingTranscoder = new ChunkTranscoder();
        this.maxWriteQueueSize = maxQueueSize;
        this.ignoreTouch = EVCacheConfig.getInstance().getPropertyRepository().get(appName + "." + this.serverGroup.getName() + ".ignore.touch", Boolean.class).orElseGet(appName + ".ignore.touch").orElse(false);

        this.connectionFactory = pool.getEVCacheClientPoolManager().getConnectionFactoryProvider().getConnectionFactory(this);
        this.connectionObserver = new EVCacheConnectionObserver(this);
        this.ignoreInactiveNodes = EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".ignore.inactive.nodes", Boolean.class).orElse(true);

        this.evcacheMemcachedClient = new EVCacheMemcachedClient(connectionFactory, memcachedNodesInZone, readTimeout, this);
        this.evcacheMemcachedClient.addObserver(connectionObserver);

        this.decodingTranscoder = new EVCacheSerializingTranscoder(Integer.MAX_VALUE);
        decodingTranscoder.setCompressionThreshold(Integer.MAX_VALUE);

        this.hashKeyByServerGroup = EVCacheConfig.getInstance().getPropertyRepository().get(this.serverGroup.getName() + ".hash.key", Boolean.class).orElse(null);
        this.hashingAlgo = EVCacheConfig.getInstance().getPropertyRepository().get(this.serverGroup.getName() + ".hash.algo", String.class).orElseGet(appName + ".hash.algo").orElse("siphash24");
        this.shouldEncodeHashKey = EVCacheConfig.getInstance().getPropertyRepository().get(this.serverGroup.getName() + ".hash.encode", Boolean.class).orElse(null);
        this.maxDigestBytes = EVCacheConfig.getInstance().getPropertyRepository().get(this.serverGroup.getName() + ".max.digest.bytes", Integer.class).orElse(null);
        this.maxHashLength = EVCacheConfig.getInstance().getPropertyRepository().get(this.serverGroup.getName() + ".max.hash.length", Integer.class).orElse(null);
        this.encoderBase = EVCacheConfig.getInstance().getPropertyRepository().get(this.serverGroup.getName() + ".hash.encoder", String.class).orElse("base64");
        ping();
    }

    
    public void ping() {
        try {
            final Map<SocketAddress, String> versions = getVersions();
            for (Entry<SocketAddress, String> vEntry : versions.entrySet()) {
                if (log.isDebugEnabled()) log.debug("Host : " + vEntry.getKey() + " : " + vEntry.getValue());
            }
        } catch (Throwable t) {
            log.error("Error while pinging the servers", t);
        }
    }
    
    public boolean isDuetClient() {
        return isDuetClient;
    }

    public Boolean shouldEncodeHashKey() {
        return this.shouldEncodeHashKey.get();
    }

    public String getBaseEncoder() {
        return this.encoderBase.get();
    }

    public Integer getMaxDigestBytes() {
        return this.maxDigestBytes.get();
    }

    public Integer getMaxHashLength() {
        return this.maxHashLength.get();
    }

    private boolean validateReadQueueSize(MemcachedNode node, String key, EVCache.Call call) {
        if (!(node instanceof EVCacheNode)) {
            return true;
        }

        final EVCacheNode evcNode = (EVCacheNode) node;
        if (!evcNode.isAvailable(call)) {
            return false;
        }

        final int size = evcNode.getReadQueueSize();
        final boolean canAddToOpQueue = size < (maxReadQueueSize.get() * 2);

        if (!canAddToOpQueue) {
            final String hostName;
            if (evcNode.getSocketAddress() instanceof InetSocketAddress) {
                hostName = ((InetSocketAddress) evcNode.getSocketAddress()).getHostName();
            } else {
                hostName = evcNode.getSocketAddress().toString();
            }

            incrementFailure(EVCacheMetricsFactory.READ_QUEUE_FULL, call, hostName);
            if (log.isDebugEnabled()) {
                log.debug("Read Queue Full on Bulk Operation for app : " + appName
                        + "; zone : " + zone + "; Current Size : " + size
                        + "; Max Size : " + maxReadQueueSize.get() * 2);
            }
        }

        return canAddToOpQueue;
    }

    private void incrementFailure(String metric, EVCache.Call call) {
        incrementFailure(metric, call, null);
    }
        
    private void incrementFailure(String metric, EVCache.Call call, String host) {
        Counter counter = counterMap.get(metric);
        if(counter == null) {
            final List<Tag> tagList = new ArrayList<Tag>(6);
            tagList.addAll(tags);
            if(call != null) {
                tagList.add(new BasicTag(EVCacheMetricsFactory.CALL_TAG, call.name()));
                switch(call) {
                case GET:
                case GETL:
                case GET_AND_TOUCH:
                case ASYNC_GET:
                case  BULK:
                case  GET_ALL:
                    tagList.add(new BasicTag(EVCacheMetricsFactory.CALL_TYPE_TAG, EVCacheMetricsFactory.READ));
                    break;
                default :
                    tagList.add(new BasicTag(EVCacheMetricsFactory.CALL_TYPE_TAG, EVCacheMetricsFactory.WRITE));
                    break;
                }
            }
            tagList.add(new BasicTag(EVCacheMetricsFactory.FAILURE_REASON, metric));
            if(host != null) tagList.add(new BasicTag(EVCacheMetricsFactory.FAILED_HOST, host));
            counter = EVCacheMetricsFactory.getInstance().getCounter(EVCacheMetricsFactory.INTERNAL_FAIL, tagList);
            counterMap.put(metric, counter);
        }
        counter.increment();
    }

    public void reportWrongKeyReturned(String hostName) {
        incrementFailure(EVCacheMetricsFactory.WRONG_KEY_RETURNED, null, hostName);
    }

    private boolean ensureWriteQueueSize(MemcachedNode node, String key, EVCache.Call call) throws EVCacheException {
        if (node instanceof EVCacheNode) {
            final EVCacheNode evcNode = (EVCacheNode) node;
            int i = 0;
            while (true) {
                final int size = evcNode.getWriteQueueSize();
                final boolean canAddToOpQueue = size < maxWriteQueueSize;
                if (log.isDebugEnabled()) log.debug("App : " + appName + "; zone : " + zone + "; key : " + key + "; WriteQSize : " + size);
                if (canAddToOpQueue) break;
                try {
                    Thread.sleep(writeBlock.get());
                } catch (InterruptedException e) {
                    throw new EVCacheException("Thread was Interrupted", e);
                }

                if(i++ > 3) {
                    final String hostName;
                    if(evcNode.getSocketAddress() instanceof InetSocketAddress) {
                        hostName = ((InetSocketAddress)evcNode.getSocketAddress()).getHostName();
                    } else {
                        hostName = evcNode.getSocketAddress().toString();
                    }
                    incrementFailure(EVCacheMetricsFactory.INACTIVE_NODE, call, hostName);
                    if (log.isDebugEnabled()) log.debug("Node : " + evcNode + " for app : " + appName + "; zone : "
                            + zone + " is not active. Will Fail Fast and the write will be dropped for key : " + key);
                    evcNode.shutdown();
                    return false;
                }
            }
        }
        return true;
    }

    private boolean validateNode(String key, boolean _throwException, EVCache.Call call) throws EVCacheException, EVCacheConnectException {
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        // First check if the node is active
        if (node instanceof EVCacheNode) {
            final EVCacheNode evcNode = (EVCacheNode) node;
            final String hostName;
            if(evcNode.getSocketAddress() instanceof InetSocketAddress) {
                hostName = ((InetSocketAddress)evcNode.getSocketAddress()).getHostName();
            } else {
                hostName = evcNode.getSocketAddress().toString();
            }
            if (!evcNode.isAvailable(call)) {
                incrementFailure(EVCacheMetricsFactory.INACTIVE_NODE, call, hostName);
                if (log.isDebugEnabled()) log.debug("Node : " + node + " for app : " + appName + "; zone : " + zone
                        + " is not active. Will Fail Fast so that we can fallback to Other Zone if available.");
                if (_throwException) throw new EVCacheConnectException("Connection for Node : " + node + " for app : " + appName
                        + "; zone : " + zone + " is not active");
                return false;
            }

            final int size = evcNode.getReadQueueSize();
            final boolean canAddToOpQueue = size < maxReadQueueSize.get();
            if (log.isDebugEnabled()) log.debug("Current Read Queue Size - " + size + " for app " + appName + " & zone "
                    + zone + " and node : " + evcNode);
            if (!canAddToOpQueue) {
                incrementFailure(EVCacheMetricsFactory.READ_QUEUE_FULL, call, hostName);
                if (log.isDebugEnabled()) log.debug("Read Queue Full for Node : " + node + "; app : " + appName
                        + "; zone : " + zone + "; Current Size : " + size + "; Max Size : " + maxReadQueueSize.get());
                if (_throwException) throw new EVCacheReadQueueException("Read Queue Full for Node : " + node + "; app : "
                        + appName + "; zone : " + zone + "; Current Size : " + size + "; Max Size : " + maxReadQueueSize.get());
                return false;
            }
        }
        return true;
    }

    private <T> ChunkDetails<T> getChunkDetails(String key) {

        final List<String> firstKeys = new ArrayList<String>(2);
        firstKeys.add(key);
        final String firstKey = key + "_00";
        firstKeys.add(firstKey);
        try {
            final Map<String, CachedData> metadataMap = evcacheMemcachedClient.asyncGetBulk(firstKeys, chunkingTranscoder, null)
                    .getSome(readTimeout.get(), TimeUnit.MILLISECONDS, false, false);
            if (metadataMap.containsKey(key)) {
                return new ChunkDetails(null, null, false, metadataMap.get(key));
            } else if (metadataMap.containsKey(firstKey)) {
                final ChunkInfo ci = getChunkInfo(firstKey, (String) decodingTranscoder.decode(metadataMap.get(firstKey)));
                if (ci == null) return null;

                final List<String> keys = new ArrayList<>();
                for (int i = 1; i < ci.getChunks(); i++) {
                    final String prefix = (i < 10) ? "0" : "";
                    keys.add(ci.getKey() + "_" + prefix + i);
                }
                return new ChunkDetails(keys, ci, true, null);
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    private <T> Single<ChunkDetails<T>> getChunkDetails(String key, Scheduler scheduler) {

        final List<String> firstKeys = new ArrayList<>(2);
        firstKeys.add(key);
        final String firstKey = key + "_00";
        firstKeys.add(firstKey);

        return evcacheMemcachedClient.asyncGetBulk(firstKeys, chunkingTranscoder, null)
            .getSome(readTimeout.get(), TimeUnit.MILLISECONDS, false, false, scheduler)
            .map(metadataMap -> {
                if (metadataMap.containsKey(key)) {
                    return new ChunkDetails(null, null, false, metadataMap.get(key));
                } else if (metadataMap.containsKey(firstKey)) {
                    final ChunkInfo ci = getChunkInfo(firstKey, (String) decodingTranscoder.decode(metadataMap.get(firstKey)));
                    if (ci == null) return null;

                    final List<String> keys = new ArrayList<>();
                    for (int i = 1; i < ci.getChunks(); i++) {
                        final String prefix = (i < 10) ? "0" : "";
                        keys.add(ci.getKey() + "_" + prefix + i);
                    }
                    return new ChunkDetails(keys, ci, true, null);
                } else {
                    return null;
                }
            });
    }

    private <T> T assembleChunks(String key, boolean touch, int ttl, Transcoder<T> tc, boolean hasZF) {
        try {

            final ChunkDetails<T> cd = getChunkDetails(key);
            if (cd == null) return null;
            if (!cd.isChunked()) {
                if (cd.getData() == null) return null;
                final Transcoder<T> transcoder = (tc == null ? (Transcoder<T>) evcacheMemcachedClient.getTranscoder()
                        : tc);
                return transcoder.decode((CachedData) cd.getData());
            } else {
                final List<String> keys = cd.getChunkKeys();
                final ChunkInfo ci = cd.getChunkInfo();

                final Map<String, CachedData> dataMap = evcacheMemcachedClient.asyncGetBulk(keys, chunkingTranscoder, null)
                        .getSome(readTimeout.get(), TimeUnit.MILLISECONDS, false, false);

                if (dataMap.size() != ci.getChunks() - 1) {
                    incrementFailure(EVCacheMetricsFactory.INCORRECT_CHUNKS, null);
                    return null;
                }

                final byte[] data = new byte[(ci.getChunks() - 2) * ci.getChunkSize() + (ci.getLastChunk() == 0 ? ci
                        .getChunkSize() : ci.getLastChunk())];
                int index = 0;
                for (int i = 0; i < keys.size(); i++) {
                    final String _key = keys.get(i);
                    final CachedData _cd = dataMap.get(_key);
                    if (log.isDebugEnabled()) log.debug("Chunk Key " + _key + "; Value : " + _cd);
                    if (_cd == null) continue;

                    final byte[] val = _cd.getData();

                    // If we expect a chunk to be present and it is null then return null immediately.
                    if (val == null) return null;
                    final int len = (i == keys.size() - 1) ? ((ci.getLastChunk() == 0 || ci.getLastChunk() > ci
                            .getChunkSize()) ? ci.getChunkSize() : ci.getLastChunk())
                            : val.length;
                    if (len != ci.getChunkSize() && i != keys.size() - 1) {
                        incrementFailure(EVCacheMetricsFactory.INVALID_CHUNK_SIZE, null);
                        if (log.isWarnEnabled()) log.warn("CHUNK_SIZE_ERROR : Chunks : " + ci.getChunks() + " ; "
                                + "length : " + len + "; expectedLength : " + ci.getChunkSize() + " for key : " + _key);
                    }
                    if (len > 0) {
                        try {
                            System.arraycopy(val, 0, data, index, len);
                        } catch (Exception e) {
                            StringBuilder sb = new StringBuilder();
                            sb.append("ArrayCopyError - Key : " + _key + "; final data Size : " + data.length
                                    + "; copy array size : " + len + "; val size : " + val.length
                                    + "; key index : " + i + "; copy from : " + index + "; ChunkInfo : " + ci + "\n");
                            for (int j = 0; j < keys.size(); j++) {
                                final String skey = keys.get(j);
                                final byte[] sval = (byte[]) dataMap.get(skey).getData();
                                sb.append(skey + "=" + sval.length + "\n");
                            }
                            if (log.isWarnEnabled()) log.warn(sb.toString(), e);
                            throw e;
                        }

                        index += val.length;
                        if (touch) evcacheMemcachedClient.touch(_key, ttl);
                    }
                }

                final boolean checksumPass = checkCRCChecksum(data, ci, hasZF);
                if (!checksumPass) return null;
                final Transcoder<T> transcoder = (tc == null ? (Transcoder<T>) evcacheMemcachedClient.getTranscoder()
                        : tc);
                return transcoder.decode(new CachedData(ci.getFlags(), data, Integer.MAX_VALUE));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    private <T> Single<T> assembleChunks(String key, boolean touch, int ttl, Transcoder<T> tc, boolean hasZF, Scheduler scheduler) {
        return getChunkDetails(key, scheduler).flatMap(cd -> {
            if (cd == null) return Single.just(null);
            if (!cd.isChunked()) {
                if (cd.getData() == null) return Single.just(null);
                final Transcoder<T> transcoder = (tc == null ? (Transcoder<T>) evcacheMemcachedClient.getTranscoder() : tc);
                return Single.just(transcoder.decode((CachedData) cd.getData()));
            } else {
                final List<String> keys = cd.getChunkKeys();
                final ChunkInfo ci = cd.getChunkInfo();

                return evcacheMemcachedClient.asyncGetBulk(keys, chunkingTranscoder, null)
                    .getSome(readTimeout.get(), TimeUnit.MILLISECONDS, false, false, scheduler)
                    .map(dataMap -> {
                        if (dataMap.size() != ci.getChunks() - 1) {
                            incrementFailure(EVCacheMetricsFactory.INCORRECT_CHUNKS, null);
                            return null;
                        }

                        final byte[] data = new byte[(ci.getChunks() - 2) * ci.getChunkSize() + (ci.getLastChunk() == 0 ? ci
                            .getChunkSize() : ci.getLastChunk())];
                        int index = 0;
                        for (int i = 0; i < keys.size(); i++) {
                            final String _key = keys.get(i);
                            final CachedData _cd = dataMap.get(_key);
                            if (log.isDebugEnabled()) log.debug("Chunk Key " + _key + "; Value : " + _cd);
                            if (_cd == null) continue;

                            final byte[] val = _cd.getData();

                            // If we expect a chunk to be present and it is null then return null immediately.
                            if (val == null) return null;
                            final int len = (i == keys.size() - 1) ? ((ci.getLastChunk() == 0 || ci.getLastChunk() > ci
                                .getChunkSize()) ? ci.getChunkSize() : ci.getLastChunk())
                                : val.length;
                            if (len != ci.getChunkSize() && i != keys.size() - 1) {
                                incrementFailure(EVCacheMetricsFactory.INVALID_CHUNK_SIZE, null);
                                if (log.isWarnEnabled()) log.warn("CHUNK_SIZE_ERROR : Chunks : " + ci.getChunks() + " ; "
                                    + "length : " + len + "; expectedLength : " + ci.getChunkSize() + " for key : " + _key);
                            }
                            if (len > 0) {
                                try {
                                    System.arraycopy(val, 0, data, index, len);
                                } catch (Exception e) {
                                    StringBuilder sb = new StringBuilder();
                                    sb.append("ArrayCopyError - Key : " + _key + "; final data Size : " + data.length
                                        + "; copy array size : " + len + "; val size : " + val.length
                                        + "; key index : " + i + "; copy from : " + index + "; ChunkInfo : " + ci + "\n");
                                    for (int j = 0; j < keys.size(); j++) {
                                        final String skey = keys.get(j);
                                        final byte[] sval = (byte[]) dataMap.get(skey).getData();
                                        sb.append(skey + "=" + sval.length + "\n");
                                    }
                                    if (log.isWarnEnabled()) log.warn(sb.toString(), e);
                                    throw e;
                                }

                                System.arraycopy(val, 0, data, index, len);
                                index += val.length;
                                if (touch) evcacheMemcachedClient.touch(_key, ttl);
                            }
                        }

                        final boolean checksumPass = checkCRCChecksum(data, ci, hasZF);
                        if (!checksumPass) return null;
                        final Transcoder<T> transcoder = (tc == null ? (Transcoder<T>) evcacheMemcachedClient.getTranscoder()
                            : tc);
                        return transcoder.decode(new CachedData(ci.getFlags(), data, Integer.MAX_VALUE));
                    });
            }
        });
    }

    private boolean checkCRCChecksum(byte[] data, final ChunkInfo ci, boolean hasZF) {
        if (data == null || data.length == 0) return false;

        final Checksum checksum = new CRC32();
        checksum.update(data, 0, data.length);
        final long currentChecksum = checksum.getValue();
        final long expectedChecksum = ci.getChecksum();
        if (log.isDebugEnabled()) log.debug("CurrentChecksum : " + currentChecksum + "; ExpectedChecksum : "
                + expectedChecksum + " for key : " + ci.getKey());
        if (currentChecksum != expectedChecksum) {
            if (!hasZF) {
                if (log.isWarnEnabled()) log.warn("CHECKSUM_ERROR : Chunks : " + ci.getChunks() + " ; "
                        + "currentChecksum : " + currentChecksum + "; expectedChecksum : " + expectedChecksum
                        + " for key : " + ci.getKey());
                incrementFailure(EVCacheMetricsFactory.CHECK_SUM_ERROR, null);
            }
            return false;
        }
        return true;
    }

    private ChunkInfo getChunkInfo(String firstKey, String metadata) {
        if (metadata == null) return null;
        final String[] metaItems = metadata.split(":");
        if (metaItems.length != 5) return null;
        final String key = firstKey.substring(0, firstKey.length() - 3);

        final ChunkInfo ci = new ChunkInfo(Integer.parseInt(metaItems[0]), Integer.parseInt(metaItems[1]), Integer
                .parseInt(metaItems[2]), Integer.parseInt(metaItems[3]), key, Long
                        .parseLong(metaItems[4]));
        return ci;
    }

    private <T> Map<String, T> assembleChunks(Collection<String> keyList, Transcoder<T> tc, boolean hasZF) {
        final List<String> firstKeys = new ArrayList<>();
        for (String key : keyList) {
            firstKeys.add(key);
            firstKeys.add(key + "_00");
        }
        try {
            final Map<String, CachedData> metadataMap = evcacheMemcachedClient.asyncGetBulk(firstKeys, chunkingTranscoder, null)
                    .getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, false, false);
            if (metadataMap == null) return null;

            final Map<String, T> returnMap = new HashMap<>(keyList.size() * 2);
            for (String key : keyList) {
                if (metadataMap.containsKey(key)) {
                    CachedData val = metadataMap.remove(key);
                    returnMap.put(key, tc.decode(val));
                }
            }

            final List<String> allKeys = new ArrayList<>();
            final Map<ChunkInfo, SimpleEntry<List<String>, byte[]>> responseMap = new HashMap<>();
            for (Entry<String, CachedData> entry : metadataMap.entrySet()) {
                final String firstKey = entry.getKey();
                final String metadata = (String) decodingTranscoder.decode(entry.getValue());
                if (metadata == null) continue;
                final ChunkInfo ci = getChunkInfo(firstKey, metadata);
                if (ci != null) {
                    final List<String> ciKeys = new ArrayList<>();

                    for (int i = 1; i < ci.getChunks(); i++) {
                        final String prefix = (i < 10) ? "0" : "";
                        final String _key = ci.getKey() + "_" + prefix + i;
                        allKeys.add(_key);
                        ciKeys.add(_key);
                    }

                    final byte[] data = new byte[(ci.getChunks() - 2) * ci.getChunkSize() + ci.getLastChunk()];
                    responseMap.put(ci, new SimpleEntry<>(ciKeys, data));
                }
            }

            final Map<String, CachedData> dataMap = evcacheMemcachedClient.asyncGetBulk(allKeys, chunkingTranscoder, null)
                    .getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, false, false);

            for (Entry<ChunkInfo, SimpleEntry<List<String>, byte[]>> entry : responseMap.entrySet()) {
                final ChunkInfo ci = entry.getKey();
                final SimpleEntry<List<String>, byte[]> pair = entry.getValue();
                final List<String> ciKeys = pair.getKey();
                byte[] data = pair.getValue();
                int index = 0;
                for (int i = 0; i < ciKeys.size(); i++) {
                    final String _key = ciKeys.get(i);
                    final CachedData cd = dataMap.get(_key);
                    if (log.isDebugEnabled()) log.debug("Chunk Key " + _key + "; Value : " + cd);
                    if (cd == null) continue;
                    final byte[] val = cd.getData();

                    if (val == null) {
                        data = null;
                        break;
                    }
                    final int len = (i == ciKeys.size() - 1) ? ((ci.getLastChunk() == 0 || ci.getLastChunk() > ci
                            .getChunkSize()) ? ci.getChunkSize() : ci.getLastChunk())
                            : val.length;
                    try {
                        System.arraycopy(val, 0, data, index, len);
                    } catch (Exception e) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("ArrayCopyError - Key : " + _key + "; final data Size : " + data.length
                                + "; copy array size : " + len + "; val size : " + val.length
                                + "; key index : " + i + "; copy from : " + index + "; ChunkInfo : " + ci + "\n");
                        for (int j = 0; j < ciKeys.size(); j++) {
                            final String skey = ciKeys.get(j);
                            final byte[] sval = dataMap.get(skey).getData();
                            sb.append(skey + "=" + sval.length + "\n");
                        }
                        if (log.isWarnEnabled()) log.warn(sb.toString(), e);
                        throw e;
                    }
                    index += val.length;
                }
                final boolean checksumPass = checkCRCChecksum(data, ci, hasZF);
                if (data != null && checksumPass) {
                    final CachedData cd = new CachedData(ci.getFlags(), data, Integer.MAX_VALUE);
                    returnMap.put(ci.getKey(), tc.decode(cd));
                } else {
                    returnMap.put(ci.getKey(), null);
                }
            }
            return returnMap;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    private <T> Single<Map<String, T>> assembleChunks(Collection<String> keyList, Transcoder<T> tc, boolean hasZF, Scheduler scheduler) {
        final List<String> firstKeys = new ArrayList<>();
        for (String key : keyList) {
            firstKeys.add(key);
            firstKeys.add(key + "_00");
        }

        return evcacheMemcachedClient.asyncGetBulk(firstKeys, chunkingTranscoder, null)
            .getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, false, false, scheduler)
            .flatMap(metadataMap -> {
                if (metadataMap == null) return null;

                final Map<String, T> returnMap = new HashMap<>(keyList.size() * 2);
                for (String key : keyList) {
                    if (metadataMap.containsKey(key)) {
                        CachedData val = metadataMap.remove(key);
                        returnMap.put(key, tc.decode(val));
                    }
                }

                final List<String> allKeys = new ArrayList<>();
                final Map<ChunkInfo, SimpleEntry<List<String>, byte[]>> responseMap = new HashMap<>();
                for (Entry<String, CachedData> entry : metadataMap.entrySet()) {
                    final String firstKey = entry.getKey();
                    final String metadata = (String) decodingTranscoder.decode(entry.getValue());
                    if (metadata == null) continue;
                    final ChunkInfo ci = getChunkInfo(firstKey, metadata);
                    if (ci != null) {
                        final List<String> ciKeys = new ArrayList<>();

                        for (int i = 1; i < ci.getChunks(); i++) {
                            final String prefix = (i < 10) ? "0" : "";
                            final String _key = ci.getKey() + "_" + prefix + i;
                            allKeys.add(_key);
                            ciKeys.add(_key);
                        }

                        final byte[] data = new byte[(ci.getChunks() - 2) * ci.getChunkSize() + ci.getLastChunk()];
                        responseMap.put(ci, new SimpleEntry<>(ciKeys, data));
                    }
                }

                return evcacheMemcachedClient.asyncGetBulk(allKeys, chunkingTranscoder, null)
                    .getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, false, false, scheduler)
                    .map(dataMap -> {
                        for (Entry<ChunkInfo, SimpleEntry<List<String>, byte[]>> entry : responseMap.entrySet()) {
                            final ChunkInfo ci = entry.getKey();
                            final SimpleEntry<List<String>, byte[]> pair = entry.getValue();
                            final List<String> ciKeys = pair.getKey();
                            byte[] data = pair.getValue();
                            int index = 0;
                            for (int i = 0; i < ciKeys.size(); i++) {
                                final String _key = ciKeys.get(i);
                                final CachedData cd = dataMap.get(_key);
                                if (log.isDebugEnabled()) log.debug("Chunk Key " + _key + "; Value : " + cd);
                                if (cd == null) continue;
                                final byte[] val = cd.getData();

                                if (val == null) {
                                    data = null;
                                    break;
                                }
                                final int len = (i == ciKeys.size() - 1) ? ((ci.getLastChunk() == 0 || ci.getLastChunk() > ci
                                    .getChunkSize()) ? ci.getChunkSize() : ci.getLastChunk())
                                    : val.length;
                                try {
                                    System.arraycopy(val, 0, data, index, len);
                                } catch (Exception e) {
                                    StringBuilder sb = new StringBuilder();
                                    sb.append("ArrayCopyError - Key : " + _key + "; final data Size : " + data.length
                                        + "; copy array size : " + len + "; val size : " + val.length
                                        + "; key index : " + i + "; copy from : " + index + "; ChunkInfo : " + ci + "\n");
                                    for (int j = 0; j < ciKeys.size(); j++) {
                                        final String skey = ciKeys.get(j);
                                        final byte[] sval = dataMap.get(skey).getData();
                                        sb.append(skey + "=" + sval.length + "\n");
                                    }
                                    if (log.isWarnEnabled()) log.warn(sb.toString(), e);
                                    throw e;
                                }
                                index += val.length;
                            }
                            final boolean checksumPass = checkCRCChecksum(data, ci, hasZF);
                            if (data != null && checksumPass) {
                                final CachedData cd = new CachedData(ci.getFlags(), data, Integer.MAX_VALUE);
                                returnMap.put(ci.getKey(), tc.decode(cd));
                            } else {
                                returnMap.put(ci.getKey(), null);
                            }
                        }

                        return returnMap;
                    });
            });
    }

    private CachedData[] createChunks(CachedData cd, String key) {
        final int cSize = chunkSize.get();
        if ((key.length() + 3) > cSize) throw new IllegalArgumentException("The chunksize " + cSize
                + " is smaller than the key size. Will not be able to proceed. key size = "
                + key.length());
        final int len = cd.getData().length;

        /* the format of headers in memcached */
        // Key size + 1 + Header( Flags (Characters Number) + Key (Characters Numbers) + 2 bytes ( \r\n ) + 4 bytes (2 spaces and 1 \r)) + Chunk Size + CAS Size
        // final int overheadSize = key.length() // Key Size
        // + 1 // Space
        // + 4 // Flags (Characters Number)
        // + 4 // Key (Characters Numbers)
        // + 2 // /r/n
        // + 4 // 2 spaces and 1 \r
        // + 48 // Header Size
        // + 8; // CAS
        final int overheadSize = key.length() + 71 + 3;
        // 3 because we will suffix _00, _01 ... _99; 68 is the size of the memcached header
        final int actualChunkSize = cSize - overheadSize;
        int lastChunkSize = len % actualChunkSize;
        final int numOfChunks = len / actualChunkSize + ((lastChunkSize > 0) ? 1 : 0) + 1;
        final CachedData[] chunkData = new CachedData[numOfChunks];
        if (lastChunkSize == 0) lastChunkSize = actualChunkSize;

        final long sTime = System.nanoTime();
        final Checksum checksum = new CRC32();
        checksum.update(cd.getData(), 0, len);
        final long checkSumValue = checksum.getValue();

        int srcPos = 0;
        if (log.isDebugEnabled()) log.debug("Ths size of data is " + len + " ; we will create " + (numOfChunks - 1)
                + " of " + actualChunkSize + " bytes. Checksum : "
                + checkSumValue + "; Checksum Duration : " + (System.nanoTime() - sTime));
        chunkData[0] = decodingTranscoder.encode(numOfChunks + ":" + actualChunkSize + ":" + lastChunkSize + ":" + cd
                .getFlags() + ":" + checkSumValue);
        for (int i = 1; i < numOfChunks; i++) {
            int lengthOfArray = actualChunkSize;
            if (srcPos + actualChunkSize > len) {
                lengthOfArray = len - srcPos;
            }
            byte[] dest = new byte[actualChunkSize];
            System.arraycopy(cd.getData(), srcPos, dest, 0, lengthOfArray);
            if (actualChunkSize > lengthOfArray) {
                for (int j = lengthOfArray; j < actualChunkSize; j++) {
                    dest[j] = Character.UNASSIGNED;// Adding filler data
                }
            }
            srcPos += lengthOfArray;
            //chunkData[i] = decodingTranscoder.encode(dest);
            chunkData[i] = new CachedData(SPECIAL_BYTEARRAY, dest, Integer.MAX_VALUE);
        }
        EVCacheMetricsFactory.getInstance().getDistributionSummary(EVCacheMetricsFactory.INTERNAL_NUM_CHUNK_SIZE, getTagList()).record(numOfChunks);
        EVCacheMetricsFactory.getInstance().getDistributionSummary(EVCacheMetricsFactory.INTERNAL_CHUNK_DATA_SIZE, getTagList()).record(len);

        return chunkData;
    }

    /**
     * Retrieves all the chunks as is. This is mainly used for debugging.
     *
     * @param key
     * @return Returns all the chunks retrieved.
     * @throws EVCacheReadQueueException
     * @throws EVCacheException
     * @throws Exception
     */
    public Map<String, CachedData> getAllChunks(String key) throws EVCacheReadQueueException, EVCacheException, Exception {
        try {
            final ChunkDetails<Object> cd = getChunkDetails(key);
            if(log.isDebugEnabled()) log.debug("Chunkdetails " + cd);
            if (cd == null) return null;
            if (!cd.isChunked()) {
                Map<String, CachedData> rv = new HashMap<String, CachedData>();
                rv.put(key, (CachedData) cd.getData());
                if(log.isDebugEnabled()) log.debug("Data : " + rv);
                return rv;
            } else {
                final List<String> keys = cd.getChunkKeys();
                if(log.isDebugEnabled()) log.debug("Keys - " + keys);
                final Map<String, CachedData> dataMap = evcacheMemcachedClient.asyncGetBulk(keys, chunkingTranscoder, null)
                        .getSome(readTimeout.get().intValue(), TimeUnit.MILLISECONDS, false, false);

                if(log.isDebugEnabled()) log.debug("Datamap " + dataMap);
                return dataMap;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    public long incr(String key, long by, long defaultVal, int timeToLive) throws EVCacheException {
        return evcacheMemcachedClient.incr(key, by, defaultVal, timeToLive);
    }

    public long decr(String key, long by, long defaultVal, int timeToLive) throws EVCacheException {
        return evcacheMemcachedClient.decr(key, by, defaultVal, timeToLive);
    }

    public <T> CompletableFuture<T> getAsync(String key, Transcoder<T> tc) {
        if(log.isDebugEnabled()) log.debug("fetching data getAsync {}", key);
        return evcacheMemcachedClient
                .asyncGet(key, tc, null)
                .getAsync(readTimeout.get(), TimeUnit.MILLISECONDS);
    }

    public <T> T get(String key, Transcoder<T> tc, boolean _throwException, boolean hasZF, boolean chunked) throws Exception {
        if (chunked) {
            return assembleChunks(key, false, 0, tc, hasZF);
        } else {
            return evcacheMemcachedClient.asyncGet(key, tc, null).get(readTimeout.get(),
                    TimeUnit.MILLISECONDS, _throwException, hasZF);
        }
    }

    public <T> T get(String key, Transcoder<T> tc, boolean _throwException, boolean hasZF) throws Exception {
        if (!validateNode(key, _throwException, Call.GET)) {
            if(ignoreInactiveNodes.get()) {
                incrementFailure(EVCacheMetricsFactory.IGNORE_INACTIVE_NODES, Call.GET);
                return pool.getEVCacheClientForReadExclude(serverGroup).get(key, tc, _throwException, hasZF, enableChunking.get());
            } else {
                return null;
            }
        }
        return get(key, tc, _throwException, hasZF, enableChunking.get());
    }

    public <T> Single<T> get(String key, Transcoder<T> tc, boolean _throwException, boolean hasZF, boolean chunked, Scheduler scheduler)  throws Exception {
        if (chunked) {
            return assembleChunks(key, _throwException, 0, tc, hasZF, scheduler);
        } else {
            return evcacheMemcachedClient.asyncGet(key, tc, null)
                .get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF, scheduler);
        }
    }

    public <T> Single<T> get(String key, Transcoder<T> tc, boolean _throwException, boolean hasZF, Scheduler scheduler) {
        try {
            if (!validateNode(key, _throwException, Call.GET)) {
                if(ignoreInactiveNodes.get()) {
                    incrementFailure(EVCacheMetricsFactory.IGNORE_INACTIVE_NODES, Call.GET);
                    return pool.getEVCacheClientForReadExclude(serverGroup).get(key, tc, _throwException, hasZF, enableChunking.get(), scheduler);
                } else {
                    return Single.just(null);
                }
            }
            return get(key, tc, _throwException, hasZF, enableChunking.get(), scheduler);
        } catch (Throwable e) {
            return Single.error(e);
        }
    }

    public <T> T getAndTouch(String key, Transcoder<T> tc, int timeToLive, boolean _throwException, boolean hasZF) throws Exception {
        EVCacheMemcachedClient _client = evcacheMemcachedClient;
        if (!validateNode(key, _throwException, Call.GET_AND_TOUCH)) {
            if(ignoreInactiveNodes.get()) {
                incrementFailure(EVCacheMetricsFactory.IGNORE_INACTIVE_NODES, Call.GET_AND_TOUCH);
                _client = pool.getEVCacheClientForReadExclude(serverGroup).getEVCacheMemcachedClient();
            } else {
                return null;
            }
        }

        if (tc == null) tc = (Transcoder<T>) getTranscoder();
        final T returnVal;
        if (enableChunking.get()) {
            return assembleChunks(key, false, 0, tc, hasZF);
        } else {
            if(ignoreTouch.get()) {
                returnVal = _client.asyncGet(key, tc, null).get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF);
            } else {
                final CASValue<T> value = _client.asyncGetAndTouch(key, timeToLive, tc).get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF);
                returnVal = (value == null) ? null : value.getValue();
            }
        }
        return returnVal;
    }

    public <T> Single<T> getAndTouch(String key, Transcoder<T> transcoder, int timeToLive, boolean _throwException, boolean hasZF, Scheduler scheduler) {
        try {
            EVCacheMemcachedClient client = evcacheMemcachedClient;
            if (!validateNode(key, _throwException, Call.GET_AND_TOUCH)) {
                if(ignoreInactiveNodes.get()) {
                    incrementFailure(EVCacheMetricsFactory.IGNORE_INACTIVE_NODES, Call.GET_AND_TOUCH);
                    client = pool.getEVCacheClientForReadExclude(serverGroup).getEVCacheMemcachedClient();
                } else {
                    return null;
                }
            }

            final EVCacheMemcachedClient _client = client;
            final Transcoder<T> tc = (transcoder == null) ? (Transcoder<T>) getTranscoder(): transcoder;
            if (enableChunking.get()) {
                return assembleChunks(key, false, 0, tc, hasZF, scheduler);
            } else {
                return _client.asyncGetAndTouch(key, timeToLive, tc)
                    .get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF, scheduler)
                    .map(value -> (value == null) ? null : value.getValue());
            }
        } catch (Throwable e) {
            return Single.error(e);
        }
    }

    public <T> Map<String, T> getBulk(Collection<String> canonicalKeys, Transcoder<T> tc, boolean _throwException,
            boolean hasZF) throws Exception {
        final Map<String, T> returnVal;
        try {
            if (tc == null) tc = (Transcoder<T>) getTranscoder();
            if (enableChunking.get()) {
                returnVal = assembleChunks(canonicalKeys, tc, hasZF);
            } else {
                final BiPredicate<MemcachedNode, String> validator = (node, key) -> validateReadQueueSize(node, key, Call.BULK);
                returnVal = evcacheMemcachedClient.asyncGetBulk(canonicalKeys, tc, null, validator)
                        .getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF);
            }
        } catch (Exception e) {
            if (_throwException) throw e;
            return Collections.<String, T> emptyMap();
        }
        return returnVal;
    }

    public <T> CompletableFuture<Map<String, T>> getAsyncBulk(Collection<String> canonicalKeys, Transcoder<T> tc) {
        final BiPredicate<MemcachedNode, String> validator = (node, key) -> validateReadQueueSize(node, key, Call.COMPLETABLE_FUTURE_GET_BULK);
        if (tc == null) tc = (Transcoder<T>) getTranscoder();
        return evcacheMemcachedClient
                .asyncGetBulk(canonicalKeys, tc, null, validator)
                .getAsyncSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS);

    }

    public <T> Single<Map<String, T>> getBulk(Collection<String> canonicalKeys, final Transcoder<T> transcoder, boolean _throwException,
            boolean hasZF, Scheduler scheduler) {
        try {
            final Transcoder<T> tc = (transcoder == null) ? (Transcoder<T>) getTranscoder() : transcoder;
            if (enableChunking.get()) {
                return assembleChunks(canonicalKeys, tc, hasZF, scheduler);
            } else {
                final BiPredicate<MemcachedNode, String> validator = (node, key) -> validateReadQueueSize(node, key, Call.BULK);
                return evcacheMemcachedClient.asyncGetBulk(canonicalKeys, tc, null, validator)
                    .getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF, scheduler);
            }
        } catch (Throwable e) {
            return Single.error(e);
        }
    }

    public <T> Future<Boolean> append(String key, T value) throws Exception {
        if (enableChunking.get()) throw new EVCacheException(
                "This operation is not supported as chunking is enabled on this EVCacheClient.");
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key, Call.APPEND)) return getDefaultFuture();
        return evcacheMemcachedClient.append(key, value);
    }

    public Future<Boolean> set(String key, CachedData value, int timeToLive) throws Exception {
        return _set(key, value, timeToLive, null);
    }

    public Future<Boolean> set(String key, CachedData cd, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        return _set(key, cd, timeToLive, evcacheLatch);
    }

    @Deprecated
    public <T> Future<Boolean> set(String key, T value, int timeToLive) throws Exception {
        return set(key, value, timeToLive, null);
    }

    @Deprecated
    public <T> Future<Boolean> set(String key, T value, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        final CachedData cd;
        if (value instanceof CachedData) {
            cd = (CachedData) value;
        } else {
            cd = getTranscoder().encode(value);
        }
        return _set(key, cd, timeToLive, evcacheLatch);
    }

    private Future<Boolean> _set(String key, CachedData value, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key, Call.SET)) {
            if (log.isInfoEnabled()) log.info("Node : " + node + " is not active. Failing fast and dropping the write event.");
            final ListenableFuture<Boolean, OperationCompletionListener> defaultFuture = (ListenableFuture<Boolean, OperationCompletionListener>) getDefaultFuture();
            if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(defaultFuture);
            return defaultFuture;
        }

        try {
            final int dataSize = ((CachedData) value).getData().length;

            if (enableChunking.get()) {
                if (dataSize > chunkSize.get()) {
                    final CachedData[] cd = createChunks(value, key);
                    final int len = cd.length;
                    final OperationFuture<Boolean>[] futures = new OperationFuture[len];
                    for (int i = 0; i < cd.length; i++) {
                        final String prefix = (i < 10) ? "0" : "";
                        futures[i] = evcacheMemcachedClient.set(key + "_" + prefix + i, timeToLive, cd[i], null, null);
                    }
                    // ensure we are deleting the unchunked key if it exists.
                    // Ignore return value since it may not exist.
                    evcacheMemcachedClient.delete(key);
                    return new EVCacheFutures(futures, key, appName, serverGroup, evcacheLatch);
                } else {
                    // delete all the chunks if they exist as the
                    // data is moving from chunked to unchunked
                    delete(key);
                    return evcacheMemcachedClient.set(key, timeToLive, value, null, evcacheLatch);
                }
            } else {
                return evcacheMemcachedClient.set(key, timeToLive, value, null, evcacheLatch);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    private Boolean shouldHashKey() {
        return hashKeyByServerGroup.get();
    }

    public HashingAlgorithm getHashingAlgorithm() {
        if (null == shouldHashKey()) {
            // hash key property is not set at the client level
            return null;
        }

        // return NO_HASHING if hashing is explicitly disabled at client level
        return shouldHashKey() ? KeyHasher.getHashingAlgorithmFromString(hashingAlgo.get()) : HashingAlgorithm.NO_HASHING;
    }

    public <T> Future<Boolean> appendOrAdd(String key, CachedData value, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key, Call.APPEND_OR_ADD)) {
            if (log.isInfoEnabled()) log.info("Node : " + node + " is not active. Failing fast and dropping the write event.");
            final ListenableFuture<Boolean, OperationCompletionListener> defaultFuture = (ListenableFuture<Boolean, OperationCompletionListener>) getDefaultFuture();
            if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(defaultFuture);
            return defaultFuture;
        }

        try {
            return evcacheMemcachedClient.asyncAppendOrAdd(key, timeToLive, value, evcacheLatch);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public Future<Boolean> replace(String key, CachedData cd, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        return _replace(key, cd, timeToLive, evcacheLatch);
    }

    @Deprecated
    public <T> Future<Boolean> replace(String key, T value, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        final CachedData cd;
        if (value instanceof CachedData) {
            cd = (CachedData) value;
        } else {
            cd = getTranscoder().encode(value);
        }
        return _replace(key, cd, timeToLive, evcacheLatch);
    }

    private Future<Boolean> _replace(String key, CachedData value, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key, Call.REPLACE)) {
            if (log.isInfoEnabled()) log.info("Node : " + node + " is not active. Failing fast and dropping the replace event.");
            final ListenableFuture<Boolean, OperationCompletionListener> defaultFuture = (ListenableFuture<Boolean, OperationCompletionListener>) getDefaultFuture();
            if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(defaultFuture);
            return defaultFuture;
        }

        try {
            final int dataSize = ((CachedData) value).getData().length;
            if (enableChunking.get() && dataSize > chunkSize.get()) {
                final CachedData[] cd = createChunks(value, key);
                final int len = cd.length;
                final OperationFuture<Boolean>[] futures = new OperationFuture[len];
                for (int i = 0; i < cd.length; i++) {
                    final String prefix = (i < 10) ? "0" : "";
                    futures[i] = evcacheMemcachedClient.replace(key + "_" + prefix + i, timeToLive, cd[i], null, null);
                }
                return new EVCacheFutures(futures, key, appName, serverGroup, evcacheLatch);
            } else {
                return evcacheMemcachedClient.replace(key, timeToLive, value, null, evcacheLatch);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    private Future<Boolean> _add(String key, int exp, CachedData value, EVCacheLatch latch) throws Exception {
        if (enableChunking.get()) throw new EVCacheException("This operation is not supported as chunking is enabled on this EVCacheClient.");

        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key, Call.ADD)) return getDefaultFuture();
        return evcacheMemcachedClient.add(key, exp, value, null, latch);
    }

    @Deprecated
    public <T> Future<Boolean> add(String key, int exp, T value) throws Exception {
        final CachedData cd;
        if (value instanceof CachedData) {
            cd = (CachedData) value;
        } else {
            cd = getTranscoder().encode(value);
        }
        return _add(key, exp, cd, null);
    }

    @Deprecated
    public <T> Future<Boolean> add(String key, int exp, T value, Transcoder<T> tc) throws Exception {
        final CachedData cd;
        if (value instanceof CachedData) {
            cd = (CachedData) value;
        } else {
            if(tc == null) {
                cd = getTranscoder().encode(value);
            } else {
                cd = tc.encode(value);
            }
        }
        return _add(key, exp, cd, null);
    }

    @Deprecated
    public <T> Future<Boolean> add(String key, int exp, T value, final Transcoder<T> tc, EVCacheLatch latch)  throws Exception {
        final CachedData cd;
        if (value instanceof CachedData) {
            cd = (CachedData) value;
        } else {
            if(tc == null) {
                cd = getTranscoder().encode(value);
            } else {
                cd = tc.encode(value);
            }
        }
        return _add(key, exp, cd, latch);
    }

    public Future<Boolean> add(String key, int exp, CachedData value, EVCacheLatch latch)  throws Exception {
        return _add(key, exp, value, latch);
    }

    public <T> Future<Boolean> touch(String key, int timeToLive) throws Exception {
    	return touch(key, timeToLive, null);
    }

    public <T> Future<Boolean> touch(String key, int timeToLive, EVCacheLatch latch) throws Exception {
    	if(ignoreTouch.get()) {
    		final ListenableFuture<Boolean, OperationCompletionListener> sf = new SuccessFuture();
    		if (latch != null && latch instanceof EVCacheLatchImpl && !isInWriteOnly()) ((EVCacheLatchImpl) latch).addFuture(sf);
    		return sf;
    	}
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key, Call.TOUCH)) {
            final ListenableFuture<Boolean, OperationCompletionListener> defaultFuture = (ListenableFuture<Boolean, OperationCompletionListener>) getDefaultFuture();
            if (latch != null && latch instanceof EVCacheLatchImpl && !isInWriteOnly()) ((EVCacheLatchImpl) latch).addFuture(defaultFuture);
            return defaultFuture;
        }

        if (enableChunking.get()) {
            final ChunkDetails<?> cd = getChunkDetails(key);
            if (cd.isChunked()) {
                final List<String> keys = cd.getChunkKeys();
                OperationFuture<Boolean>[] futures = new OperationFuture[keys.size() + 1];
                futures[0] = evcacheMemcachedClient.touch(key + "_00", timeToLive, latch);
                for (int i = 0; i < keys.size(); i++) {
                    final String prefix = (i < 10) ? "0" : "";
                    final String _key = key + "_" + prefix + i;
                    futures[i + 1] = evcacheMemcachedClient.touch(_key, timeToLive, latch);
                }
                return new EVCacheFutures(futures, key, appName, serverGroup, latch);
            } else {
                return evcacheMemcachedClient.touch(key, timeToLive, latch);
            }
        } else {
            return evcacheMemcachedClient.touch(key, timeToLive, latch);
        }
    }

    public <T> Future<T> asyncGet(String key, Transcoder<T> tc, boolean _throwException, boolean hasZF)
            throws Exception {
        if (enableChunking.get()) throw new EVCacheException(
                "This operation is not supported as chunking is enabled on this EVCacheClient.");
        if (!validateNode(key, _throwException, Call.ASYNC_GET)) return null;
        if (tc == null) tc = (Transcoder<T>) getTranscoder();
        return evcacheMemcachedClient.asyncGet(key, tc, null);
    }

    public Future<Boolean> delete(String key) throws Exception {
        return delete(key, null);
    }

    public Future<Boolean> delete(String key, EVCacheLatch latch) throws Exception {
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key, Call.DELETE)) {
            final ListenableFuture<Boolean, OperationCompletionListener> defaultFuture = (ListenableFuture<Boolean, OperationCompletionListener>) getDefaultFuture();
            if (latch != null && latch instanceof EVCacheLatchImpl && !isInWriteOnly()) ((EVCacheLatchImpl) latch).addFuture(defaultFuture);
            return defaultFuture;
        }

        if (enableChunking.get()) {
            final ChunkDetails<?> cd = getChunkDetails(key);
            if (cd == null) {
             // Paranoid delete : cases where get fails and we ensure the first key is deleted just in case
                return evcacheMemcachedClient.delete(key + "_00", latch);
            }
            if (!cd.isChunked()) {
                return evcacheMemcachedClient.delete(key, latch);
            } else {
                final List<String> keys = cd.getChunkKeys();
                OperationFuture<Boolean>[] futures = new OperationFuture[keys.size() + 1];
                futures[0] = evcacheMemcachedClient.delete(key + "_00");
                for (int i = 0; i < keys.size(); i++) {
                    futures[i + 1] = evcacheMemcachedClient.delete(keys.get(i), null);
                }
                return new EVCacheFutures(futures, key, appName, serverGroup, latch);
            }
        } else {
            return evcacheMemcachedClient.delete(key, latch);
        }
    }

    public boolean removeConnectionObserver() {
        try {
            boolean removed = evcacheMemcachedClient.removeObserver(connectionObserver);
            if (removed) connectionObserver = null;
            return removed;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean shutdown(long timeout, TimeUnit unit) {
        if(shutdown) return true;

        shutdown = true;
        try {
            evcacheMemcachedClient.shutdown(timeout, unit);
        } catch(Throwable t) {
            log.error("Exception while shutting down", t);
        }
        return true;
    }

    public EVCacheConnectionObserver getConnectionObserver() {
        return this.connectionObserver;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public String getAppName() {
        return appName;
    }

    public String getZone() {
        return zone;
    }

    public int getId() {
        return id;
    }

    public ServerGroup getServerGroup() {
        return serverGroup;
    }

    public String getServerGroupName() {
        return (serverGroup == null ? "NA" : serverGroup.getName());
    }

    public boolean isShutdown() {
        return this.shutdown;
    }

    public boolean isInWriteOnly(){
        return pool.isInWriteOnly(getServerGroup());
    }

    public Map<SocketAddress, Map<String, String>> getStats(String cmd) {
        return evcacheMemcachedClient.getStats(cmd);
    }

    public Map<SocketAddress, String> execCmd(String cmd, String[] ips) {
        return evcacheMemcachedClient.execCmd(cmd, ips);
    }

    public Map<SocketAddress, String> getVersions() {
        return evcacheMemcachedClient.getVersions();
    }

    public Future<Boolean> flush() {
        return evcacheMemcachedClient.flush();
    }

    public Transcoder<Object> getTranscoder() {
        return evcacheMemcachedClient.getTranscoder();
    }

    public ConnectionFactory getEVCacheConnectionFactory() {
        return this.connectionFactory;
    }

    public NodeLocator getNodeLocator() {
        return this.evcacheMemcachedClient.getNodeLocator();
    }

    static class SuccessFuture implements ListenableFuture<Boolean, OperationCompletionListener> {

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return true;
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return true;
		}

		@Override
		public Boolean get() throws InterruptedException, ExecutionException {
			return Boolean.TRUE;
		}

		@Override
		public Boolean get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			return Boolean.TRUE;
		}

        @Override
        public Future<Boolean> addListener(OperationCompletionListener listener) {
            return this;
        }

        @Override
        public Future<Boolean> removeListener(OperationCompletionListener listener) {
            return this;
        }
    }

    static class DefaultFuture implements ListenableFuture<Boolean, OperationCompletionListener> {
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Boolean get() throws InterruptedException, ExecutionException {
            return Boolean.FALSE;
        }

        @Override
        public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            return Boolean.FALSE;
        }

        @Override
        public Future<Boolean> addListener(OperationCompletionListener listener) {
            return this;
        }

        @Override
        public Future<Boolean> removeListener(OperationCompletionListener listener) {
            return this;
        }
    }

    private Future<Boolean> getDefaultFuture() {
        final Future<Boolean> defaultFuture = new DefaultFuture();
        return defaultFuture;
    }

    public String toString() {
        return "App : " + appName + "; Zone : " + zone + "; Id : " + id + "; " + serverGroup.toString() + "; Nodes : "
                + memcachedNodesInZone.toString();
    }

    public EVCacheMemcachedClient getEVCacheMemcachedClient() {
        return evcacheMemcachedClient;
    }

    public List<InetSocketAddress> getMemcachedNodesInZone() {
        return memcachedNodesInZone;
    }

    public int getMaxWriteQueueSize() {
        return maxWriteQueueSize;
    }

    public Property<Integer> getReadTimeout() {
        return readTimeout;
    }

    public Property<Integer> getBulkReadTimeout() {
        return bulkReadTimeout;
    }

    public Property<Integer> getMaxReadQueueSize() {
        return maxReadQueueSize;
    }

    public Property<Boolean> getEnableChunking() {
        return enableChunking;
    }

    public Property<Integer> getChunkSize() {
        return chunkSize;
    }

    public ChunkTranscoder getChunkingTranscoder() {
        return chunkingTranscoder;
    }

    public EVCacheSerializingTranscoder getDecodingTranscoder() {
        return decodingTranscoder;
    }

    public EVCacheClientPool getPool() {
        return pool;
    }

    public EVCacheServerGroupConfig getEVCacheConfig() {
        return config;
    }

   static class ChunkDetails<T> {

        final List<String> chunkKeys;
        final ChunkInfo chunkInfo;
        final boolean chunked;
        final T data;

        public ChunkDetails(List<String> chunkKeys, ChunkInfo chunkInfo, boolean chunked, T data) {
            super();
            this.chunkKeys = chunkKeys;
            this.chunkInfo = chunkInfo;
            this.chunked = chunked;
            this.data = data;
        }

        public List<String> getChunkKeys() {
            return chunkKeys;
        }

        public ChunkInfo getChunkInfo() {
            return chunkInfo;
        }

        public boolean isChunked() {
            return chunked;
        }

        public T getData() {
            return data;
        }

		@Override
		public String toString() {
			return "ChunkDetails [chunkKeys=" + chunkKeys + ", chunkInfo=" + chunkInfo + ", chunked=" + chunked
					+ ", data=" + data + "]";
		}

    }

    static class ChunkInfo {

        final int chunks;
        final int chunkSize;
        final int lastChunk;
        final int flags;
        final String key;
        final long checksum;

        public ChunkInfo(int chunks, int chunkSize, int lastChunk, int flags, String firstKey, long checksum) {
            super();
            this.chunks = chunks;
            this.chunkSize = chunkSize;
            this.lastChunk = lastChunk;
            this.flags = flags;
            this.key = firstKey;
            this.checksum = checksum;
        }

        public int getChunks() {
            return chunks;
        }

        public int getChunkSize() {
            return chunkSize;
        }

        public int getLastChunk() {
            return lastChunk;
        }

        public int getFlags() {
            return flags;
        }

        public String getKey() {
            return key;
        }

        public long getChecksum() {
            return checksum;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{\"chunks\":\"");
            builder.append(chunks);
            builder.append("\",\"chunkSize\":\"");
            builder.append(chunkSize);
            builder.append("\",\"lastChunk\":\"");
            builder.append(lastChunk);
            builder.append("\",\"flags\":\"");
            builder.append(flags);
            builder.append("\",\"key\":\"");
            builder.append(key);
            builder.append("\",\"checksum\":\"");
            builder.append(checksum);
            builder.append("\"}");
            return builder.toString();
        }
    }

    public int getWriteQueueLength() {
        final Collection<MemcachedNode> allNodes = evcacheMemcachedClient.getNodeLocator().getAll();
        int size = 0;
        for(MemcachedNode node : allNodes) {
            if(node instanceof EVCacheNode) {
                size += ((EVCacheNode)node).getWriteQueueSize();
            }
        }
        return size;
    }

    public int getReadQueueLength() {
        final Collection<MemcachedNode> allNodes = evcacheMemcachedClient.getNodeLocator().getAll();
        int size = 0;
        for(MemcachedNode node : allNodes) {
            if(node instanceof EVCacheNode) {
                size += ((EVCacheNode)node).getReadQueueSize();
            }
        }
        return size;
    }

    public List<Tag> getTagList() {
        return tags;
    }

    public Counter getOperationCounter() {
        return operationsCounter;
    }


    /**
     * Return the keys upto the limit. The key will be cannoicalized key( or hashed Key).<br>
     * <B> The keys are read into memory so make sure you have enough memory to read the specified number of keys<b>
     * @param limit - The number of keys that need to fetched from each memcached clients.
     * @return - the List of keys.
     */
    public List<String> getAllKeys(final int limit) {
        final List<String> keyList = new ArrayList<String>(limit);
        byte[] array = new byte[EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".all.keys.reader.buffer.size.bytes", Integer.class).orElse(4*1024*1024).get()];
        final int waitInSec = EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".all.keys.reader.wait.duration.sec", Integer.class).orElse(60).get();
        for(InetSocketAddress address : memcachedNodesInZone) {
            //final List<String> keyList = new ArrayList<String>(limit);
            Socket socket = null;
            PrintWriter printWriter = null;
            BufferedInputStream bufferedReader = null;
            try {
                socket = new Socket(address.getHostName(), address.getPort());
                printWriter = new PrintWriter(socket.getOutputStream(), true);
                printWriter.print("lru_crawler metadump all \r\n");
                printWriter.print("quit \r\n");
                printWriter.flush();

                bufferedReader = new BufferedInputStream(socket.getInputStream());
                while(isDataAvailableForRead(bufferedReader, waitInSec, TimeUnit.SECONDS, socket)) {
                    int read = bufferedReader.read(array);
                    if (log.isDebugEnabled()) log.debug("Number of bytes read = " +read);
                    if(read > 0) {
                        StringBuilder b = new StringBuilder();
                        boolean start = true;
                        for (int i = 0; i < read; i++) {
                            if(array[i] == ' ') {
                                start = false;
                                if(b.length() > 4) keyList.add(URLDecoder.decode(b.substring(4), StandardCharsets.UTF_8.name()));
                                b = new StringBuilder();
                            }
                            if(start) b.append((char)array[i]);
                            if(array[i] == '\n') {
                                start = true;
                            }
                            if(keyList.size() >= limit) {
                                if (log.isDebugEnabled()) log.debug("Record Limit reached. Will break and return");
                                return keyList;
                            }
                        }
                    } else if (read < 0 ){
                        break;
                    }
                }
            } catch (Exception e) {
                if(socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e1) {
                        log.error("Error closing socket", e1);
                    }
                }
                log.error("Exception", e);
            }
            finally {
                if(bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (IOException e1) {
                        log.error("Error closing bufferedReader", e1);
                    }
                }
                if(printWriter != null) {
                    try {
                        printWriter.close();
                    } catch (Exception e1) {
                        log.error("Error closing socket", e1);
                    }
                }
                if(socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        if (log.isDebugEnabled()) log.debug("Error closing socket", e);
                    }
                }
            }
        }
        return keyList;
    }

    private boolean isDataAvailableForRead(BufferedInputStream bufferedReader, long timeout, TimeUnit unit, Socket socket) throws IOException {
        long expiry = System.currentTimeMillis() + unit.toMillis(timeout);
        int tryCount = 0;
        while(expiry > System.currentTimeMillis()) {
            if(log.isDebugEnabled()) log.debug("For Socket " + socket + " number of bytes available = " + bufferedReader.available() + " and try number is " + tryCount);
            if(bufferedReader.available() > 0) {
                return true;
            }
            if(tryCount++ < 5) {
                try {
                    if(log.isDebugEnabled()) log.debug("Sleep for 100 msec");
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            } else {
                return false;
            }
        }
        return false;
    }


    public EVCacheItemMetaData metaDebug(String key) throws Exception {
        final EVCacheItemMetaData obj = evcacheMemcachedClient.metaDebug(key);
        if(log.isDebugEnabled()) log.debug("EVCacheItemMetaData : " + obj);
        return obj;
    }

    public <T> EVCacheItem<T> metaGet(String key, Transcoder<T> tc, boolean _throwException, boolean hasZF) throws Exception {
        final EVCacheItem<T> obj = evcacheMemcachedClient.asyncMetaGet(key, tc, null).get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF);
        if (log.isDebugEnabled()) log.debug("EVCacheItem : " + obj);
        return obj;
    }


    public void addTag(String tagName, String tagValue) {
        final Tag tag = new BasicTag(tagName, tagValue);
        if(tags.contains(tag)) return;
        final List<Tag> tagList = new ArrayList<Tag>(tags);
        tagList.add(tag);
        this.tags = Collections.<Tag>unmodifiableList(new ArrayList(tagList));
        
    }
}
