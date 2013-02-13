package com.netflix.evcache.pool;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.CASValue;

import com.netflix.evcache.EVCacheTranscoder;
import com.netflix.evcache.pool.observer.EVCacheConnectionObserver;

/**
 * A client for the EVCache Cluster. The client manages all the communication with the server.
 * The client handles all the operations and if needed performs any fallback operations if a requests fails.
 */
public interface EVCacheClient {

    /**
     * Retrieve the value for the given key using the specified EVCacheTranscoder.
     *
     * @param key to get. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @return the Value for the given key from the cache (null if there is none).
     * @throws Exception in the rare circumstance where queue is too full to accept any more requests or
     *          issues during deserialization or any IO Related issues
     */
    <T> T get(String key, EVCacheTranscoder<T> tc) throws Exception;

    /**
     * Get the value for the given single key and reset its expiration to the given timeToLive value.
     *
     * @param key the key to get. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @param timeToLive the new expiration of this object i.e. less than 30 days in seconds or the exact expiry time as UNIX time
     * @return the result from the cache (null if there is none)
     * @throws Exception that occurred while executing this call on the
     */
    <T> T getAndTouch(String key, EVCacheTranscoder<T> tc, int timeToLive) throws Exception;

    /**
     * Touch the given key to reset its expiration time with the default transcoder.

     * @param key to get. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @param timeToLive the new expiration of this object i.e. less than 30 days in seconds or the exact expiry time as UNIX time
     * @return Boolean wrapped in a future indicating if the operation was successful or not
     * @throws Exception that occurred while executing this call on the
     */
    <T> Future<Boolean> touch(String key, int timeToLive) throws Exception;

    /**
     * Set an value in the EVCache. If a value already exits it will be overwritten
     *
     * @param key the key under which this object should be added. Ensure the key is properly encoded and does not contain whitespace
     *          or control characters.
     * @param value the object to store
     * @param tc the EVCacheTranscoder to serialize the data
     * @param timeToLive the expiration of this object i.e. less than 30 days in seconds or the exact expire time as UNIX EPOC time
     * @return A Future representing the processing of this operation
     * @throws Exception in the rare circumstance where queue is too full to accept any more requests or
     *          issues Serializing the value or any IO Related issues
     */
    <T> Future<Boolean> set(String key, EVCacheTranscoder<T> tc, T value, int timeToLive) throws Exception;

    /**
     * Retrieve the value for the given key asynchronously using the specified EVCacheTranscoder to deserialize it.
     *
     * @param key to get. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @param tc the EVCacheTranscoder to deserialize the data
     * @return the Value for the given key encapsulated in a Future object.
     *          NOTE : You can block on this future to get the value. If you specify a timeout then after the elapsed time
     *          the operation is cancelled. If you call get again on the same future object it will return immediately returning null.
     * @throws Exception in the rare circumstance where queue is too full to accept any more requests or
     *          issues during deserialization or any IO Related issues
     */
    <T> Future<T> asyncGet(String key, EVCacheTranscoder<T> tc) throws Exception;

    /**
     * Get the value for the given key asynchronously and reset its expiration time.
     *
     * @param key to get.
     * @param tc the EVCacheTranscoder to deserialize the data
     * @param timeToLive the expiration of this object i.e. less than 30 days in seconds or the exact expire time as UNIX EPOC time
     * @return A Future encapsulating the CASValue.
     *          NOTE : You can block on this future to get the value. If you specify a timeout then after the elapsed time
     *          the operation is cancelled. If you call get again on the same future object it will return immediately returning null.
     * @throws Exception
     */
    <T> Future<CASValue<T>> asyncGetAndTouch(String key, EVCacheTranscoder<T> tc, int timeToLive) throws Exception;

    /**
     * Retrieve the value for a set of keys, using a specified EVCacheTranscoder for deserialization.
     *
     * @param keys keys to which we need the values from EVCache
     * @param tc the EVCachetranscoder to use for deserialization
     * @return a map of the values (for each value that exists). If the value for the given key does not exist then null is returned for that key
     * @throws Exception in the rare circumstance where queue is too full to accept any more requests or
     *          issues during deserialization or any IO Related issues
     */
    <T> Map<String, T> getBulk(Collection<String> keys, EVCacheTranscoder<T> tc) throws Exception;

    /**
     * Remove a given key from EVCache.
     *
     * @param key the key corresponding to the relation to be removed. Ensure the key is properly encoded and does not contain whitespace
     *             or control characters.
     * @return A Futures representing the outcome of processing this operation
     * @throws Exception in the rare circumstance where queue is too full to accept any more requests or any IO Related issues
     */
    Future<Boolean> delete(String key) throws Exception;

    /**
     * Remove the connection Observer object from the client.
     *
     * @return true if successful else false
     */
    boolean removeConnectionObserver();

    /**
     * Returns the Observer used for this client.
     *
     * @return the current Observer if one is set else null
     */
    EVCacheConnectionObserver getConnectionObserver();

    /**
     * shutdown the client after waiting for the given timeout to ensure the queues are drained properly and any pending operations are finished.
     *
     * @param timeout the time to shutdown
     * @param unit the TimeUnit for the timeout
     * @return result if greceful shutdown was successful was successful or not
     */
    boolean shutdown(long timeout, TimeUnit unit);


    /**
     * returns true if the Client has been shut down else false.
     */
    boolean isShutdown();

    /**
     * Performs the given stats operation across all the EVCache servers in the cluster and returns its value.
     * This is mainly used for admin purpose
     *
     * @param cmd - the stats command to be executed.
     * @return A Map of EVCache server SocketAddress to the stats command output. The value is a Map of key value pairs
     */
    Map<SocketAddress, Map<String, String>> getStats(String cmd);

    /**
     * Returns the Memcachec version running on the EVCache Server.
     * @return Map of server SocketAddress to its memcached version
     */
    Map<SocketAddress, String> getVersions();

    /**
     * The name of this EVCache cluster.
     * @return the name
     */
    String getAppName();

    /**
     * The zone (if any) for the EVCache cluster to which this Client is connected to.
     *
     * @return the zone name if any else null
     */
    String getZone();

    /**
     * The Id of this client in the pool.
     *
     * @return the zone name if any else null
     */
    int getId();

    /**
     * The timeout for all read operations.
     *
     * @return the timeout in milliseconds
     */
    int getReadTimeout();
}
