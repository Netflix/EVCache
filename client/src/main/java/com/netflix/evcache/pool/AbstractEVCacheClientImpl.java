package com.netflix.evcache.pool;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.CASValue;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

import com.netflix.config.DynamicIntProperty;
import com.netflix.evcache.EVCacheTranscoder;
import com.netflix.evcache.pool.observer.EVCacheConnectionObserver;

/**
 * An implementation of EVCacheClient.
 */
@SuppressWarnings("unchecked")
public abstract class AbstractEVCacheClientImpl implements EVCacheClient {

    /**
     * The {@link ConnectionFactory} that will be used by this client to create {@link MemcachedClient}
     * and various other objects needed to interact with the memcached server.
     */
    protected final ConnectionFactory connectionFactory;

    /**
     * The id of this client.
     */
    protected final int id;

    /**
     * The appName associated with this client.
     */
    protected final String appName;

    /**
     * The zone this client belongs too.
     */
    protected final String zone;

    /**
     * The timeout for reading data from memcached server. This is a {@link DynamicIntProperty} and can be changed at runtime.
     */
    protected final DynamicIntProperty readTimeout;

    /**
     * flag to indicate if the client is in shutdown mode.
     */
    protected boolean shutdown;

    /**
     * The {@link MemcachedClient} that performs all the operation on behalf of this EVCacheClient.
     */
    protected MemcachedClient client;


    /**
     * Creates an instance of the {@link EVCacheClient} (to be invoked by subclass) with the given appName, zone, id and readTimeout.
     * This creates a default connection factory {@link BinaryConnectionFactory} with the given maxQueueSize.
     *
     * @param appName - The name of the EVCache app.
     * @param zone - The zone this client belongs to.
     * @param id - The id of this client.
     * @param maxQueueSize - Max number of items allowed in the queue
     * @param readTimeout - The timeout for all read operations
     */
    protected AbstractEVCacheClientImpl(String appName, String zone, int id, int maxQueueSize, DynamicIntProperty readTimeout) {
        this(appName, zone, id, readTimeout,
                new BinaryConnectionFactory(maxQueueSize, DefaultConnectionFactory.DEFAULT_READ_BUFFER_SIZE, HashAlgorithm.KETAMA_HASH));
    }

    /**
     * Creates an instance of the EVCacheClientImpl (to be invoked by subclass) with the given appName, zone, id, readTimeout
     * and {@link ConnectionFactory}.
     *
     * @param appName - The name of the EVCache app.
     * @param zone - The zone this client belongs to.
     * @param id - The id of this client.
     * @param readTimeout - The timeout for all read operations
     */
    protected AbstractEVCacheClientImpl(String appName, String zone, int id, DynamicIntProperty readTimeout, ConnectionFactory connectionFactory) {
        this.id = id;
        this.appName = appName;
        this.zone = zone;
        this.readTimeout = readTimeout;
        this.connectionFactory = connectionFactory;
    }


    /**
     * {@inheritDoc}
     */
    public boolean shutdown(long timeout, TimeUnit unit) {
        shutdown = true;
        return client.shutdown(timeout, unit);
    }


    /**
     * {@inheritDoc}
     */
    public <T> T get(String key, EVCacheTranscoder<T> tc) throws Exception {
        return asyncGet(key, tc).get(readTimeout.get(), TimeUnit.MILLISECONDS);
    }


    /**
     * {@inheritDoc}
     */
    public <T> T getAndTouch(String key, EVCacheTranscoder<T> tc, int timeToLive)
            throws Exception {
        final CASValue<T> value;
        if (tc == null) {
            value = (CASValue<T>) client.asyncGetAndTouch(key, timeToLive).get(readTimeout.get(), TimeUnit.MILLISECONDS);
        } else {
            value = client.asyncGetAndTouch(key, timeToLive, tc).get(readTimeout.get(), TimeUnit.MILLISECONDS);
        }
        return (value == null) ? null : value.getValue();
    }


    /**
     * {@inheritDoc}
     */
    public <T> Future<CASValue<T>> asyncGetAndTouch(String key, EVCacheTranscoder<T> tc, int timeToLive)
            throws Exception {
        if (tc == null) {
            return (Future<CASValue<T>>) client.asyncGetAndTouch(key, timeToLive, (Transcoder<T>) client.getTranscoder());
        } else {
            return client.asyncGetAndTouch(key, timeToLive, tc);
        }
    }


    /**
     * {@inheritDoc}
     */
    public <T> Future<Boolean> touch(String key, int timeToLive) throws Exception {
        return client.touch(key, timeToLive);
    }


    /**
     * {@inheritDoc}
     */
    public <T> Future<Boolean> set(String key, EVCacheTranscoder<T> tc, T value, int timeToLive) throws Exception {
        if (tc == null) {
            return client.set(key, timeToLive, value);
        } else {
            return client.set(key, timeToLive, value, tc);
        }
    }


    /**
     * {@inheritDoc}
     */
    public <T> Future<T> asyncGet(String key, EVCacheTranscoder<T> tc) throws Exception {
        if (tc == null) {
            return (Future<T>) client.asyncGet(key);
        } else {
            return client.asyncGet(key, tc);
        }
    }


    /**
     * {@inheritDoc}
     */
    public <T> Map<String, T> getBulk(Collection<String> canonicalKeys, EVCacheTranscoder<T> tc) throws Exception {
        if (tc == null) {
            return (Map<String, T>) client.asyncGetBulk(canonicalKeys).get(readTimeout.get(), TimeUnit.MILLISECONDS);
        } else {
            return client.asyncGetBulk(canonicalKeys, tc).get(readTimeout.get(), TimeUnit.MILLISECONDS);
        }
    }


    /**
     * {@inheritDoc}
     */
    public Future<Boolean> delete(String key) throws Exception {
        return client.delete(key);
    }


    /**
     * {@inheritDoc}
     */
    public String getAppName() {
        return appName;
    }


    /**
     * {@inheritDoc}
     */
    public String getZone() {
        return zone;
    }


    /**
     * {@inheritDoc}
     */
    public int getId() {
        return id;
    }


    /**
     * {@inheritDoc}
     */
    public boolean isShutdown() {
        return this.shutdown;
    }


    /**
     * {@inheritDoc}
     */
    public Map<SocketAddress, Map<String, String>> getStats(String cmd) {
        return client.getStats(cmd);
    }


    /**
     * {@inheritDoc}
     */
    public Map<SocketAddress, String> getVersions() {
        return client.getVersions();
    }


    /**
     * {@inheritDoc}
     */
    public boolean removeConnectionObserver() {
        return true;
    }


    /**
     * {@inheritDoc}
     */
    public EVCacheConnectionObserver getConnectionObserver() {
        return null;
    }

    public int getReadTimeout() {
        return readTimeout.get();
    }


    /**
     * The String representation of this instance.
     */
    public String toString() {
        return "connectionFactory=" + connectionFactory.toString() + ", client=" + client.toString()
                + ", shutdown=" + shutdown + ", id=" + id + ", appName=" + appName + ", zone=" + zone;
    }
}
