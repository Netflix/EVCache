package com.netflix.evcache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import javax.annotation.Nullable;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.evcache.EVCacheLatch.Policy;
import com.netflix.evcache.operation.EVCacheItem;
import com.netflix.evcache.operation.EVCacheItemMetaData;
import com.netflix.evcache.pool.EVCacheClientPoolManager;

import net.spy.memcached.transcoders.Transcoder;

/**
 * An abstract interface for interacting with an Ephemeral Volatile Cache.
 *
 * <h3>Example</h3>
 * <p>
 * To create an instance of EVCache with AppName="EVCACHE", cachePrefix="Test"
 * and DefaultTTL="3600"
 *
 * <b>Dependency Injection (Guice) Approach</b> <blockquote>
 *
 * <pre>
 * {@literal @}Inject
 * public MyClass(EVCache.Builder builder,....) {
 *      EVCache myCache =  builder.setAppName("EVCACHE").setCachePrefix("Test").setDefaultTTL(3600).build();
 * }
 * </pre>
 *
 * </blockquote>
 *
 * Below is an example to set value="John Doe" for key="name" <blockquote>
 *
 * <pre>
 * myCache.set("name", "John Doe");
 * </pre>
 *
 * </blockquote>
 *
 *
 * To read the value for key="name" <blockquote>
 *
 * <pre>
 * String value = myCache.get("name");
 * </pre>
 *
 * </blockquote>
 *
 * </p>
 *
 * @author smadappa
 */
public interface EVCache {
    // TODO: Remove Async methods (Project rx) and rename  COMPLETABLE_* with ASYNC_*
    public static enum Call {
        GET, GETL, GET_AND_TOUCH, ASYNC_GET, BULK, SET, DELETE, INCR, DECR, TOUCH, APPEND, PREPEND, REPLACE, ADD, APPEND_OR_ADD, GET_ALL, META_GET, META_SET, META_DEBUG,
        COMPLETABLE_FUTURE_GET, COMPLETABLE_FUTURE_GET_BULK
    };

    /**
     * Set an object in the EVCACHE (using the default Transcoder) regardless of
     * any existing value.
     *
     * The <code>timeToLive</code> value passed to memcached is as specified in
     * the defaultTTL value for this cache
     *
     * @param key
     *            the key under which this object should be added. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param T
     *            the object to store
     * @return Array of futures representing the processing of this operation
     *         across all replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues with Serializing the value or any
     *             IO Related issues
     */
    <T> Future<Boolean>[] set(String key, T value) throws EVCacheException;

    /**
     * Set an object in the EVCACHE (using the default Transcoder) regardless of
     * any existing value.
     *
     * The <code>timeToLive</code> value is passed to memcached exactly as
     * given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time a.k.a EPOC
     * time (number of seconds since January 1, 1970, as a 32-bit int value), or
     * a number of seconds starting from current time. In the latter case, this
     * number of seconds may not exceed 60*60*24*30 (number of seconds in 30
     * days); if the number sent by a client is larger than that, the server
     * will consider it to be real Unix time value rather than an offset from
     * current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param T
     *            the object to store
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] set(String key, T value, int timeToLive) throws EVCacheException;

    /**
     * Set an object in the EVCACHE using the given Transcoder regardless of any
     * existing value.
     *
     * The <code>timeToLive</code> value is passed to memcached exactly as
     * given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time a.k.a EPOC
     * time (number of seconds since January 1, 1970, as a 32-bit int value), or
     * a number of seconds starting from current time. In the latter case, this
     * number of seconds may not exceed 60*60*24*30 (number of seconds in 30
     * days); if the number sent by a client is larger than that, the server
     * will consider it to be real Unix time value rather than an offset from
     * current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param T
     *            the object to store
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] set(String key, T value, Transcoder<T> tc) throws EVCacheException;



    /**
     * Set an object in the EVCACHE using the given Transcoder regardless of any existing value using the default TTL and Transcoder.
     *
     * The <code>timeToLive</code> value is passed to memcached exactly as given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time aka EPOC time (number of seconds since January 1, 1970, as a 32-bit int value), or a number of seconds starting from current time. In the latter case, this number of seconds may not exceed 60*60*24*30 (number of seconds in 30 days); if the number sent by a client is larger than that, the server will consider it to be real Unix time value rather than an offset from current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added.
     *            Ensure the key is properly encoded and does not
     *            contain whitespace or control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param T
     *            the object to store
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can then be used to await until the count down has reached to 0 or the specified time has elapsed.
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept any more requests or issues Serializing the value or any IO Related issues
     */
    <T> EVCacheLatch set(String key, T value, EVCacheLatch.Policy policy) throws EVCacheException;


    /**
     * Set an object in the EVCACHE using the given Transcoder regardless of any existing value with the given TTL.
     *
     * The <code>timeToLive</code> value is passed to memcached exactly as given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time a.k.a EPOC time (number of seconds since January 1, 1970, as a 32-bit int value), or a number of seconds starting from current time. In the latter case, this number of seconds may not exceed 60*60*24*30 (number of seconds in 30 days); if the number sent by a client is larger than that, the server will consider it to be real Unix time value rather than an offset from current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the key is properly encoded and does not contain whitespace or control characters.  The max length of the key (including prefix)
     *            is 250 characters.
     * @param T
     *            the object to store
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in seconds or the exact expiry time as UNIX time
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can then be used to await until the count down has reached to 0 or the specified time has elapsed.
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept any more requests or issues Serializing the value or any IO Related issues
     */
    <T> EVCacheLatch set(String key, T value, int timeToLive, EVCacheLatch.Policy policy) throws EVCacheException;

    /**
     * Set an object in the EVCACHE using the given Transcoder regardless of any existing value using the given Transcoder.
     *
     * The <code>timeToLive</code> value is passed to memcached exactly as given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time aka EPOC time (number of seconds since January 1, 1970, as a 32-bit int value), or a number of seconds starting from current time. In the latter case, this number of seconds may not exceed 60*60*24*30 (number of seconds in 30 days); if the number sent by a client is larger than that, the server will consider it to be real Unix time value rather than an offset from current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the key is properly encoded and does not contain whitespace or control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param T
     *            the object to store
     * @param tc
     *            the Transcoder to serialize the data
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can then be used to await until the count down has reached to 0 or the specified time has elapsed.
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept any more requests or issues Serializing the value or any IO Related issues
     */
    <T> EVCacheLatch set(String key, T value, Transcoder<T> tc, EVCacheLatch.Policy policy) throws EVCacheException;

    /**
     * Set an object in the EVCACHE using the given Transcoder regardless of any
     * existing value.
     *
     * The <code>timeToLive</code> value is passed to memcached exactly as
     * given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time aka EPOC time
     * (number of seconds since January 1, 1970, as a 32-bit int value), or a
     * number of seconds starting from current time. In the latter case, this
     * number of seconds may not exceed 60*60*24*30 (number of seconds in 30
     * days); if the number sent by a client is larger than that, the server
     * will consider it to be real Unix time value rather than an offset from
     * current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param T
     *            the object to store
     * @param tc
     *            the Transcoder to serialize the data
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can
     *            then be used to await until the count down has reached to 0 or
     *            the specified time has elapsed.
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> EVCacheLatch set(String key, T value, Transcoder<T> tc, int timeToLive, EVCacheLatch.Policy policy)
            throws EVCacheException;

    /**
     * Replace an existing object in the EVCACHE using the default Transcoder &
     * default TTL. If the object does not exist in EVCACHE then the value is
     * not replaced.
     *
     * @param key
     *            the key under which this object should be replaced. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param T
     *            the object to replace
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can
     *            then be used to await until the count down has reached to 0 or
     *            the specified time has elapsed.
     *
     * @return EVCacheLatch which will encompasses the Operation. You can block
     *         on the Operation based on the policy to ensure the required
     *         criteria is met. The Latch can also be queried to get details on
     *         status of the operations
     *
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> EVCacheLatch replace(String key, T value, EVCacheLatch.Policy policy) throws EVCacheException;

    /**
     * Replace an existing object in the EVCACHE using the given Transcoder &
     * default TTL. If the object does not exist in EVCACHE then the value is
     * not replaced.
     *
     * @param key
     *            the key under which this object should be replaced. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param T
     *            the object to replace
     * @param tc
     *            the Transcoder to serialize the data
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can
     *            then be used to await until the count down has reached to 0 or
     *            the specified time has elapsed.
     *
     * @return EVCacheLatch which will encompasses the Operation. You can block
     *         on the Operation based on the policy to ensure the required
     *         criteria is met. The Latch can also be queried to get details on
     *         status of the operations
     *
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> EVCacheLatch replace(String key, T value, Transcoder<T> tc, EVCacheLatch.Policy policy) throws EVCacheException;

    /**
     * Replace an existing object in the EVCACHE using the given Transcoder. If
     * the object does not exist in EVCACHE then the value is not replaced.
     *
     * The <code>timeToLive</code> value is passed to memcached exactly as
     * given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time aka EPOC time
     * (number of seconds since January 1, 1970, as a 32-bit int value), or a
     * number of seconds starting from current time. In the latter case, this
     * number of seconds may not exceed 60*60*24*30 (number of seconds in 30
     * days); if the number sent by a client is larger than that, the server
     * will consider it to be real Unix time value rather than an offset from
     * current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be replaced. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param T
     *            the object to replace
     * @param tc
     *            the Transcoder to serialize the data
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can
     *            then be used to await until the count down has reached to 0 or
     *            the specified time has elapsed.
     *
     * @return EVCacheLatch which will encompasses the Operation. You can block
     *         on the Operation based on the policy to ensure the required
     *         criteria is met. The Latch can also be queried to get details on
     *         status of the operations
     *
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> EVCacheLatch replace(String key, T value, Transcoder<T> tc, int timeToLive, EVCacheLatch.Policy policy)
            throws EVCacheException;

    /**
     * Set an object in the EVCACHE using the given {@link Transcoder}regardless of any
     * existing value.
     *
     * The <code>timeToLive</code> value is passed to memcached exactly as
     * given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time aka EPOC time
     * (number of seconds since January 1, 1970, as a 32-bit int value), or a
     * number of seconds starting from current time. In the latter case, this
     * number of seconds may not exceed 60*60*24*30 (number of seconds in 30
     * days); if the number sent by a client is larger than that, the server
     * will consider it to be real Unix time value rather than an offset from
     * current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param T
     *            the object to store
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] set(String key, T value, Transcoder<T> tc, int timeToLive) throws EVCacheException;

    /**
     * Remove a current key value relation from the Cache.
     *
     * @param key
     *            the non-null key corresponding to the relation to be removed.
     *            Ensure the key is properly encoded and does not contain
     *            whitespace or control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @return Array of futures representing the processing of this operation
     *         across all the replicas. If the future returns true then the key
     *         was deleted from Cache, if false then the key was not found thus
     *         not deleted. Note: In effect the outcome was what was desired.
     *         Note: If the null is returned then the operation timed out and
     *         probably the key was not deleted. In such scenario retry the
     *         operation.
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or any IO Related issues
     */
    Future<Boolean>[] delete(String key) throws EVCacheException;

    /**
     * Remove a current key value relation from the Cache.
     *
     * @param key
     *            the non-null key corresponding to the relation to be removed.
     *            Ensure the key is properly encoded and does not contain
     *            whitespace or control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can
     *            then be used to await until the count down has reached to 0 or
     *            the specified time has elapsed.
     *
     * @return EVCacheLatch which will encompasses the Operation. You can block
     *         on the Operation based on the policy to ensure the required
     *         criteria is met. The Latch can also be queried to get details on
     *         status of the operations
     *
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or any IO Related issues
     */
    <T> EVCacheLatch delete(String key, EVCacheLatch.Policy policy) throws EVCacheException;

    /**
     * Retrieve the value for the given key.
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @return the Value for the given key from the cache (null if there is
     *         none).
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     *
     *             Note: If the data is replicated by zone, then we can get the
     *             value from the zone local to the client. If we cannot find
     *             this value then null is returned. This is transparent to the
     *             users.
     */
    <T> T get(String key) throws EVCacheException;

    /**
     * Async Retrieve the value for the given key.
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @return the Value for the given key from the cache (null if there is
     *         none).
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     *
     *             Note: If the data is replicated by zone, then we can get the
     *             value from the zone local to the client. If we cannot find
     *             this value then null is returned. This is transparent to the
     *             users.
     */
    <T> CompletableFuture<T> getAsync(String key) throws EVCacheException;
    /**
     * Retrieve the value for the given a key using the specified Transcoder for
     * deserialization.
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param tc
     *            the Transcoder to deserialize the data
     * @return the Value for the given key from the cache (null if there is
     *         none).
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     *
     *             Note: If the data is replicated by zone, then we can get the
     *             value from the zone local to the client. If we cannot find
     *             this value then null is returned. This is transparent to the
     *             users.
     */
    <T> T get(String key, Transcoder<T> tc) throws EVCacheException;

    /**
     * Async Retrieve the value for the given a key using the specified Transcoder for
     * deserialization.
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param tc
     *            the Transcoder to deserialize the data
     * @return the Completable Future of value for the given key from the cache (null if there is
     *         none).
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     *
     *             Note: If the data is replicated by zone, then we can get the
     *             value from the zone local to the client. If we cannot find
     *             this value then null is returned. This is transparent to the
     *             users.
     */
    <T> CompletableFuture<T> getAsync(String key, Transcoder<T> tc) throws EVCacheException;

    /**
     * Retrieve the meta data for the given a key 
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @return the metadata for the given key from the cache (null if there is
     *         none).
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues due IO
     *             Related issues
     *
     *             Note: If the data is replicated by zone, then we get the metadata
     *             from the zone local to the client. If we cannot find
     *             the value then we try other zones, If all are unsuccessful then null is returned. 
     */
    default EVCacheItemMetaData metaDebug(String key) throws EVCacheException {
    	throw new EVCacheException("Default implementation. If you are implementing EVCache interface you need to implement this method.");
    }


    /**
     * Retrieve the value & its metadata for the given a key using the specified Transcoder for
     * deserialization.
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param tc
     *            the Transcoder to deserialize the data
     * @return the Value for the given key from the cache (null if there is
     *         none) and its metadata all encapsulated in EVCacheItem.
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     *
     *             Note: If the data is replicated by zone, then we can get the
     *             value from the zone local to the client. If we cannot find
     *             this value we retry other zones, if still not found, then null is returned. 
     */
    default <T> EVCacheItem<T> metaGet(String key, Transcoder<T> tc) throws EVCacheException {
    	throw new EVCacheException("Default implementation. If you are implementing EVCache interface you need to implement this method.");
    }


    /**
     * Retrieve the value for the given a key using the specified Transcoder for
     * deserialization.
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters. The max length of the key (including prefix)
     *            is 250 characters.
     * @param tc
     *            the Transcoder to deserialize the data
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can then be used to await until the count down has reached to 0 or the specified time has elapsed.
     *
     * @return the Value for the given key from the cache (null if there is
     *         none).
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     *
     *             Note: If the data is replicated by zone, then we can get the
     *             value from the zone local to the client. If we cannot find
     *             this value then null is returned. This is transparent to the
     *             users.
     */
    <T> T get(String key, Transcoder<T> tc, Policy policy) throws EVCacheException;

    /**
     * Get with a single key and reset its expiration.
     *
     * @param key
     *            the key to get. Ensure the key is properly encoded and does
     *            not contain whitespace or control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param timeToLive
     *            the new expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @return the result from the cache (null if there is none)
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> T getAndTouch(String key, int timeToLive) throws EVCacheException;

    /**
     * Get with a single key and reset its expiration.
     *
     * @param key
     *            the key to get. Ensure the key is properly encoded and does
     *            not contain whitespace or control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param timeToLive
     *            the new expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @param tc
     *            the Transcoder to deserialize the data
     * @return the result from the cache (null if there is none)
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> T getAndTouch(String key, int timeToLive, Transcoder<T> tc) throws EVCacheException;

    /**
     * Retrieve the value of a set of keys.
     *
     * @param keys
     *            the keys for which we need the values. Ensure each key is properly encoded and does
     *            not contain whitespace or control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @return a map of the values (for each value that exists). If the Returned
     *         map contains the key but the value in null then the key does not
     *         exist in the cache. if a key is missing then we were not able to
     *         retrieve the data for that key due to some exception
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> Map<String, T> getBulk(String... keys) throws EVCacheException;

    /**
     * Async Retrieve the value of a set of keys.
     *
     * @param keys
     *            the keys for which we need the values. Ensure each key is properly encoded and does
     *            not contain whitespace or control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @return a map of the values (for each value that exists). If the Returned
     *         map contains the key but the value in null then the key does not
     *         exist in the cache. if a key is missing then we were not able to
     *         retrieve the data for that key due to some exception
     */
    <T> CompletableFuture<Map<String, T>> getAsyncBulk(String... keys);

    /**
     * Retrieve the value for a set of keys, using a specified Transcoder for
     * deserialization.
     *
     * @param keys
     *            keys to which we need the values.Ensure each key is properly encoded and does
     *            not contain whitespace or control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param tc
     *            the transcoder to use for deserialization
     * @return a map of the values (for each value that exists). If the Returned
     *         map contains the key but the value in null then the key does not
     *         exist in the cache. if a key is missing then we were not able to
     *         retrieve the data for that key due to some exception
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys) throws EVCacheException;


    /**
     * Async Retrieve the value for a set of keys, using a specified Transcoder for
     * deserialization. In Beta testing (To be used by gateway team)
     *
     * @param keys
     *            keys to which we need the values.Ensure each key is properly encoded and does
     *            not contain whitespace or control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param tc
     *            the transcoder to use for deserialization
     * @return a map of the values (for each value that exists). If the Returned
     *         map contains the key but the value in null then the key does not
     *         exist in the cache. if a key is missing then we were not able to
     *         retrieve the data for that key due to some exception
     */
    <T> CompletableFuture<Map<String, T>> getAsyncBulk(Collection<String> keys, Transcoder<T> tc);

    /**
     * Retrieve the value for the collection of keys, using the default
     * Transcoder for deserialization.
     *
     * @param keys
     *            The collection of keys for which we need the values. Ensure each key is properly encoded and does
     *            not contain whitespace or control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @return a map of the values (for each value that exists). If the Returned
     *         map contains the key but the value in null then the key does not
     *         exist in the cache. if a key is missing then we were not able to
     *         retrieve the data for that key due to some exception
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> Map<String, T> getBulk(Collection<String> keys) throws EVCacheException;

    /**
     * Retrieve the value for the collection of keys, using the specified
     * Transcoder for deserialization.
     *
     * @param keys
     *            The collection of keys for which we need the values. Ensure each key is properly encoded and does
     *            not contain whitespace or control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param tc
     *            the transcoder to use for deserialization
     * @return a map of the values (for each value that exists). If the Returned
     *         map contains the key but the value in null then the key does not
     *         exist in the cache. if a key is missing then we were not able to
     *         retrieve the data for that key due to some exception
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> Map<String, T> getBulk(Collection<String> keys, Transcoder<T> tc) throws EVCacheException;

    /**
     * Retrieve the value for the collection of keys, using the specified
     * Transcoder for deserialization.
     *
     * @param keys
     *            The collection of keys for which we need the values. Ensure each key is properly encoded and does
     *            not contain whitespace or control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param tc
     *            the transcoder to use for deserialization
     * @param timeToLive
     *            the new expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @return a map of the values (for each value that exists). If the value of
     *         the given key does not exist then null is returned. Only the keys
     *         whose value are not null and exist in the returned map are set to
     *         the new TTL as specified in timeToLive.
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> Map<String, T> getBulkAndTouch(Collection<String> keys, Transcoder<T> tc, int timeToLive)
            throws EVCacheException;

    /**
     * Increment the given counter, returning the new value.
     *
     * @param key
     *            the key. Ensure the key is
     *            properly encoded and does not contain whitespace or control
     *            characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param by
     *            the amount to increment
     * @param def
     *            the default value (if the counter does not exist)
     * @param exp
     *            the expiration of this object
     * @return the new value, or -1 if we were unable to increment or add
     * @throws EVCacheException
     *             in the circumstance where timeout is exceeded or queue is
     *             full
     *
     */
    public long incr(String key, long by, long def, int exp) throws EVCacheException;

    /**
     * Decrement the given counter, returning the new value.
     *
     * @param key
     *            the key. Ensure the key is
     *            properly encoded and does not contain whitespace or control
     *            characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param by
     *            the amount to decrement
     * @param def
     *            the default value (if the counter does not exist)
     * @param exp
     *            the expiration of this object
     * @return the new value, or -1 if we were unable to decrement or add
     * @throws EVCacheException
     *             in the circumstance where timeout is exceeded or queue is
     *             full
     *
     */
    public long decr(String key, long by, long def, int exp) throws EVCacheException;

    /**
     * Append the given value to the existing value in EVCache. You cannot
     * append if the key does not exist in EVCache. If the value has not changed
     * then false will be returned.
     *
     * @param key
     *            the key under which this object should be appended. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters.  The max length of the key (including prefix)
     *            is 200 characters.
     * @param T
     *            the value to be appended
     * @param tc
     *            the transcoder the will be used for serialization
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     *
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the circumstance where queue is too full to accept any
     *             more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] append(String key, T value, Transcoder<T> tc, int timeToLive) throws EVCacheException;

    /**
     * Append the given value to the existing value in EVCache. You cannot
     * append if the key does not exist in EVCache. If the value has not changed
     * or does not exist then false will be returned.
     *
     * @param key
     *            the key under which this object should be appended. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param T
     *            the value to be appended
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     *
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the circumstance where queue is too full to accept any
     *             more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] append(String key, T value, int timeToLive) throws EVCacheException;

    /**
     * Add the given value to EVCache. You cannot add if the key already exist in EVCache.
     *
     * @param key
     *            the key which this object should be added to. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param T
     *            the value to be added
     * @param tc
     *            the transcoder the will be used for serialization
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can then be used to await until the count down has reached to 0 or the specified time has elapsed.
     *
     *
     * @return EVCacheLatch which will encompasses the Operation. You can block
     *         on the Operation to ensure all adds are successful. If there are any partial success
     *         The client will try and fix the Data.
     *
     *
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> EVCacheLatch add(String key, T value, Transcoder<T> tc, int timeToLive, Policy policy) throws EVCacheException;


    /**
     * Touch the given key and reset its expiration time.
     *
     * @param key
     *            the key to touch.  Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param ttl
     *            the new expiration time in seconds
     *
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] touch(String key, int ttl) throws EVCacheException;


    /**
     * Touch the given key and reset its expiration time.
     *
     * @param key
     *            the key to touch.  Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param ttl
     *            the new expiration time in seconds
     *
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can
     *            then be used to await until the count down has reached to 0 or
     *            the specified time has elapsed.
     *
     * @return EVCacheLatch which will encompasses the Operation. You can block
     *         on the Operation based on the policy to ensure the required
     *         criteria is met. The Latch can also be queried to get details on
     *         status of the operations
     *
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or any IO Related issues
     */
    <T> EVCacheLatch touch(String key, int ttl, EVCacheLatch.Policy policy) throws EVCacheException;

    /**
     * Append the given value to the existing value in EVCache. If the Key does not exist the the key will added.
     *
     *
     * @param key
     *            the key under which this object should be appended or Added. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param T
     *            the value to be appended
     * @param tc
     *            the transcoder the will be used for serialization
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     *
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the circumstance where queue is too full to accept any
     *             more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] appendOrAdd(String key, T value, Transcoder<T> tc, int timeToLive) throws EVCacheException;


    /**
     * Append the given value to the existing value in EVCache. If the Key does not exist the the key will added.
     *
     *
     * @param key
     *            the key under which this object should be appended or Added. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters. The max length of the key (including prefix)
     *            is 200 characters.
     * @param T
     *            the value to be appended
     * @param tc
     *            the transcoder the will be used for serialization
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     *
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can then be used to await until the count down has reached to 0 or the specified time has elapsed.
     *
     * @return EVCacheLatch which will encompasses the Operation. You can block
     *         on the Operation based on the policy to ensure the required
     *         criteria is met. The Latch can also be queried to get details on
     *         status of the operations
     *
     * @throws EVCacheException
     *             in the circumstance where queue is too full to accept any
     *             more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> EVCacheLatch appendOrAdd(String key, T value, Transcoder<T> tc, int timeToLive, Policy policy) throws EVCacheException;

    /**
     * The {@code appName} that will be used by this {@code EVCache}.
     *
     * @param The
     *            name of the EVCache App cluster.
     * @return this {@code Builder} object
     */
    String getAppName();


    /**
     * The {@code cachePrefix} that will be used by this {@code EVCache}.
     *
     * @param The
     *            name of the EVCache App cluster.
     * @return this {@code Builder} object
     */
    String getCachePrefix();

    /**
     * A Builder that builds an EVCache based on the specified App Name, cache
     * Name, TTl and Transcoder.
     *
     * @author smadappa
     */
    public class Builder {
        private static final Logger logger = LoggerFactory.getLogger(EVCacheImpl.class);

        private String _appName;
        private String _cachePrefix = null;
        private int _ttl = 900;
        private Transcoder<?> _transcoder = null;
        private boolean _serverGroupRetry = true;
        private boolean _enableExceptionThrowing = false;
        private List<Customizer> _customizers = new ArrayList<>();

        @Inject
        private EVCacheClientPoolManager _poolManager;

        /**
         * Customizers allow post-processing of the Builder. This affords a way for libraries to
         * perform customization.
         */
        @FunctionalInterface
        public interface Customizer {
            void customize(final String cacheName, final Builder builder);
        }

        public static class Factory {
            public Builder createInstance(String appName) {
                return Builder.forApp(appName);
            }
        }

        public static Builder forApp(final String appName) {
            return new Builder().setAppName(appName);
        }

        public Builder() {
        }

        public Builder withConfigurationProperties(
            final EVCacheClientPoolConfigurationProperties configurationProperties) {
          return this
              .setCachePrefix(configurationProperties.getKeyPrefix())
              .setDefaultTTL(configurationProperties.getTimeToLive())
              .setRetry(configurationProperties.getRetryEnabled())
              .setExceptionThrowing(configurationProperties.getExceptionThrowingEnabled());
        }

        /**
         * The {@code appName} that will be used by this {@code EVCache}.
         *
         * @param The
         *            name of the EVCache App cluster.
         * @return this {@code Builder} object
         */
        public Builder setAppName(String appName) {
            if (appName == null) throw new IllegalArgumentException("param appName cannot be null.");
            this._appName = appName.toUpperCase(Locale.US);
            if (!_appName.startsWith("EVCACHE")) logger.warn("Make sure the app you are connecting to is EVCache App");
            return this;
        }

        /**
         * Adds {@code cachePrefix} to the key. This ensures there are no cache
         * collisions if the same EVCache app is used across multiple use cases.
         * If the cache is not shared we recommend to set this to
         * <code>null</code>. Default is <code>null</code>.
         *
         * @param cacheName.
         *            The cache prefix cannot contain colon (':') in it.
         * @return this {@code Builder} object
         */
        public Builder setCachePrefix(String cachePrefix) {
            if (_cachePrefix != null && _cachePrefix.indexOf(':') != -1) throw new IllegalArgumentException(
                    "param cacheName cannot contain ':' character.");
            this._cachePrefix = cachePrefix;
            return this;
        }

        /**
         * @deprecated Please use {@link #setCachePrefix(String)}
         * @see #setCachePrefix(String)
         *
         *      Adds {@code cacheName} to the key. This ensures there are no
         *      cache collisions if the same EVCache app is used for across
         *      multiple use cases.
         *
         * @param cacheName
         * @return this {@code Builder} object
         */
        public Builder setCacheName(String cacheName) {
            return setCachePrefix(cacheName);
        }

        /**
         * The default Time To Live (TTL) for items in {@link EVCache} in
         * seconds. You can override the value by passing the desired TTL with
         * {@link EVCache#set(String, Object, int)} operations.
         *
         * @param ttl. Default is 900 seconds.
         * @return this {@code Builder} object
         */
        public Builder setDefaultTTL(int ttl) {
            if (ttl < 0) throw new IllegalArgumentException("Time to Live cannot be less than 0.");
            this._ttl = ttl;
            return this;
        }

      /**
       * The default Time To Live (TTL) for items in {@link EVCache} in
       * seconds. You can override the value by passing the desired TTL with
       * {@link EVCache#set(String, Object, int)} operations.
       *
       * @param ttl. Default is 900 seconds.
       * @return this {@code Builder} object
       */
      public Builder setDefaultTTL(@Nullable final Duration ttl) {
            if (ttl == null) {
                return this;
            }

            return setDefaultTTL((int) ttl.getSeconds());
        }

        @VisibleForTesting
        Transcoder<?> getTranscoder() {
          return this._transcoder;
        }

        /**
         * The default {@link Transcoder} to be used for serializing and
         * de-serializing items in {@link EVCache}.
         *
         * @param transcoder
         * @return this {@code Builder} object
         */
        public <T> Builder setTranscoder(Transcoder<T> transcoder) {
            this._transcoder = transcoder;
            return this;
        }

        /**
         * @deprecated Please use {@link #enableRetry()}
         *
         *             Will enable retries across Zone (Server Group).
         *
         * @return this {@code Builder} object
         */
        public <T> Builder enableZoneFallback() {
            this._serverGroupRetry = true;
            return this;
        }

      /**
       * Will enable or disable retry across Server Group for cache misses and exceptions
       * if there are multiple Server Groups for the given EVCache App and
       * data is replicated across them. This ensures the Hit Rate continues
       * to be unaffected whenever a server group loses instances.
       *
       * By Default retry is enabled.
       *
       * @param enableRetry whether retries are to be enabled
       * @return this {@code Builder} object
       */
      public Builder setRetry(boolean enableRetry) {
            this._serverGroupRetry = enableRetry;

            return this;
        }

        /**
         * Will enable retry across Server Group for cache misses and exceptions
         * if there are multiple Server Groups for the given EVCache App and
         * data is replicated across them. This ensures the Hit Rate continues
         * to be unaffected whenever a server group loses instances.
         *
         * By Default retry is enabled.
         *
         * @return this {@code Builder} object
         */
        public <T> Builder enableRetry() {
            this._serverGroupRetry = true;
            return this;
        }

        /**
         * Will disable retry across Server Groups. This means if the data is
         * not found in one server group null is returned.
         *
         * @return this {@code Builder} object
         */
        public <T> Builder disableRetry() {
            this._serverGroupRetry = false;
            return this;
        }

        /**
         * @deprecated Please use {@link #disableRetry()}
         *
         *             Will disable retry across Zone (Server Group).
         *
         * @return this {@code Builder} object
         */
        public <T> Builder disableZoneFallback() {
            this._serverGroupRetry = false;
            return this;
        }

      /**
       * By Default exceptions are not propagated and null values are
       * returned. By enabling exception propagation we return the
       * {@link EVCacheException} whenever the operations experience them.
       *
       * @param enableExceptionThrowing whether exception throwing is to be enabled
       * @return this {@code Builder} object
       */
      public Builder setExceptionThrowing(boolean enableExceptionThrowing) {
            this._enableExceptionThrowing = enableExceptionThrowing;

            return this;
        }

        /**
         * By Default exceptions are not propagated and null values are
         * returned. By enabling exception propagation we return the
         * {@link EVCacheException} whenever the operations experience them.
         *
         * @return this {@code Builder} object
         */
        public <T> Builder enableExceptionPropagation() {
            this._enableExceptionThrowing = true;
            return this;
        }

      /**
       * Adds customizers to be applied by {@code customize}.
       *
       * @param customizers List of {@code Customizer}s
       * @return this {@code Builder} object
       */
        public Builder addCustomizers(@Nullable final List<Customizer> customizers) {
            this._customizers.addAll(customizers);

            return this;
        }


      /**
       * Applies {@code Customizer}s added through {@code addCustomizers} to {@this}.
       *
       * @return this {@code Builder} object
       */
      public Builder customize() {
        _customizers.forEach(customizer -> {
          customizeWith(customizer);
        });

        return this;
      }

      /**
       * Customizes {@this} with the {@code customizer}.
       *
       * @param customizer {@code Customizer} or {@code Consumer<String, Builder>} to be applied to {@code this}.
       * @return this {@code Builder} object
       */
      public Builder customizeWith(final Customizer customizer) {
            customizer.customize(this._appName, this);

            return this;
        }

        protected EVCache newImpl(String appName, String cachePrefix, int ttl, Transcoder<?> transcoder, boolean serverGroupRetry, boolean enableExceptionThrowing, EVCacheClientPoolManager poolManager) {
          return new EVCacheImpl(appName, cachePrefix, ttl, transcoder, serverGroupRetry, enableExceptionThrowing, poolManager);
        }

        /**
         * Returns a newly created {@code EVCache} based on the contents of the
         * {@code Builder}.
         */
        @SuppressWarnings("deprecation")
        public EVCache build() {
            if (_poolManager == null) {
                _poolManager = EVCacheClientPoolManager.getInstance();
                if (logger.isDebugEnabled()) logger.debug("_poolManager - " + _poolManager + " through getInstance");
            }

            if (_appName == null) {
                throw new IllegalArgumentException("param appName cannot be null.");
            }

            if(_cachePrefix != null) {
                for(int i = 0; i < _cachePrefix.length(); i++) {
                    if(Character.isWhitespace(_cachePrefix.charAt(i))){
                        throw new IllegalArgumentException("Cache Prefix ``" + _cachePrefix  + "`` contains invalid character at position " + i );
                    }
                }
            }

            customize();

            return newImpl(_appName, _cachePrefix, _ttl, _transcoder, _serverGroupRetry, _enableExceptionThrowing, _poolManager);
        }
    }
}
