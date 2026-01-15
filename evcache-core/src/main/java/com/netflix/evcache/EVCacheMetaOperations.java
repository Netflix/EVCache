package com.netflix.evcache;

import java.util.Collection;
import java.util.Map;

import com.netflix.evcache.EVCacheLatch.Policy;
import com.netflix.evcache.operation.EVCacheItem;
import net.spy.memcached.protocol.ascii.MetaGetBulkOperation;
import net.spy.memcached.protocol.ascii.MetaSetOperation;
import net.spy.memcached.protocol.ascii.MetaDeleteOperation;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Additional meta protocol operations for EVCache.
 * These methods leverage the advanced capabilities of memcached's meta protocol.
 */
public interface EVCacheMetaOperations {

    /**
     * Advanced set operation using meta protocol with CAS, conditional operations,
     * and atomic features across all replicas.
     *
     * @param builder Meta set configuration builder
     * @param policy Latch policy for coordinating across replicas
     * @return EVCacheLatch for tracking operation completion
     * @throws EVCacheException if operation fails
     */
    default EVCacheLatch metaSet(MetaSetOperation.Builder builder, Policy policy) throws EVCacheException {
        throw new EVCacheException("Default implementation. If you are implementing EVCache interface you need to implement this method.");
    }

    /**
     * Retrieve values and metadata for multiple keys using meta protocol.
     * Following EVCache bulk operation conventions.
     *
     * @param keys Collection of keys to retrieve
     * @param tc Transcoder for deserialization
     * @return Map of key to EVCacheItem containing data and metadata
     * @throws EVCacheException if operation fails
     */
    default <T> Map<String, EVCacheItem<T>> metaGetBulk(Collection<String> keys, Transcoder<T> tc) throws EVCacheException {
        throw new EVCacheException("Default implementation. If you are implementing EVCache interface you need to implement this method.");
    }

    /**
     * Retrieve values and metadata for multiple keys using meta protocol with custom configuration.
     *
     * @param keys Collection of keys to retrieve  
     * @param config Configuration for meta get bulk behavior
     * @param tc Transcoder for deserialization
     * @return Map of key to EVCacheItem containing data and metadata
     * @throws EVCacheException if operation fails
     */
    default <T> Map<String, EVCacheItem<T>> metaGetBulk(Collection<String> keys, MetaGetBulkOperation.Config config, Transcoder<T> tc) throws EVCacheException {
        throw new EVCacheException("Default implementation. If you are implementing EVCache interface you need to implement this method.");
    }

    /**
     * Retrieve values and metadata for multiple keys using meta protocol.
     * Varargs convenience method.
     *
     * @param keys Keys to retrieve
     * @return Map of key to EVCacheItem containing data and metadata  
     * @throws EVCacheException if operation fails
     */
    default <T> Map<String, EVCacheItem<T>> metaGetBulk(String... keys) throws EVCacheException {
        throw new EVCacheException("Default implementation. If you are implementing EVCache interface you need to implement this method.");
    }

    /**
     * Retrieve values and metadata for multiple keys using meta protocol with custom transcoder.
     * Varargs convenience method.
     *
     * @param tc Transcoder for deserialization
     * @param keys Keys to retrieve
     * @return Map of key to EVCacheItem containing data and metadata
     * @throws EVCacheException if operation fails
     */
    default <T> Map<String, EVCacheItem<T>> metaGetBulk(Transcoder<T> tc, String... keys) throws EVCacheException {
        throw new EVCacheException("Default implementation. If you are implementing EVCache interface you need to implement this method.");
    }

    /**
     * Advanced delete operation using meta protocol with CAS and conditional operations.
     * Supports both deletion and invalidation (marking as stale).
     *
     * @param builder Meta delete configuration builder
     * @param policy Latch policy for coordinating across replicas
     * @return EVCacheLatch for tracking operation completion
     * @throws EVCacheException if operation fails
     */
    default EVCacheLatch metaDelete(MetaDeleteOperation.Builder builder, Policy policy) throws EVCacheException {
        throw new EVCacheException("Default implementation. If you are implementing EVCache interface you need to implement this method.");
    }

}