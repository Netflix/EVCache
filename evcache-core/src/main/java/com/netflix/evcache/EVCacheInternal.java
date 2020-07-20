package com.netflix.evcache;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import net.spy.memcached.transcoders.Transcoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public interface EVCacheInternal extends EVCache {
    <T> EVCacheLatch addOrSetToWriteOnly(boolean replaceItem, String key, T value, Transcoder<T> tc, int timeToLive, EVCacheLatch.Policy policy) throws EVCacheException;

    <T> EVCacheLatch addOrSet(boolean replaceItem, String key, T value, Transcoder<T> tc, int timeToLive, EVCacheLatch.Policy policy, List<String> serverGroups) throws EVCacheException;

    <T> EVCacheLatch addOrSet(boolean replaceItem, String key, T value, Transcoder<T> tc, int timeToLive, EVCacheLatch.Policy policy, String serverGroup) throws EVCacheException;

    <T> EVCacheLatch addOrSet(boolean replaceItem, String key, T value, Transcoder<T> tc, int timeToLive, EVCacheLatch.Policy policy, String serverGroupName, String destinationIp) throws EVCacheException;

    // does not work for auto hashed keys
    boolean isKeyHashed(String appName, String serverGroup);

    public class Builder {
        private static final Logger logger = LoggerFactory.getLogger(EVCacheInternalImpl.class);

        private String _appName;
        private String _cachePrefix = null;
        private int _ttl = 900;
        private Transcoder<?> _transcoder = null;
        private boolean _serverGroupRetry = true;
        private boolean _enableExceptionThrowing = false;
        private List<EVCacheInternal.Builder.Customizer> _customizers = new ArrayList<>();

        @Inject
        private EVCacheClientPoolManager _poolManager;

        /**
         * Customizers allow post-processing of the Builder. This affords a way for libraries to
         * perform customization.
         */
        @FunctionalInterface
        public interface Customizer {
            void customize(final String cacheName, final EVCacheInternal.Builder builder);
        }

        public static class Factory {
            public EVCacheInternal.Builder createInstance(String appName) {
                return EVCacheInternal.Builder.forApp(appName);
            }
        }

        public static EVCacheInternal.Builder forApp(final String appName) {
            return new EVCacheInternal.Builder().setAppName(appName);
        }

        public Builder() {
        }

        public EVCacheInternal.Builder withConfigurationProperties(
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
         * @param appName
         *          name of the EVCache App cluster.
         * @return this {@code Builder} object
         */
        public EVCacheInternal.Builder setAppName(String appName) {
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
         * @param cachePrefix
         *            The cache prefix cannot contain colon (':') in it.
         * @return this {@code Builder} object
         */
        public EVCacheInternal.Builder setCachePrefix(String cachePrefix) {
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
        public EVCacheInternal.Builder setCacheName(String cacheName) {
            return setCachePrefix(cacheName);
        }

        /**
         * The default Time To Live (TTL) for items in {@link EVCache} in
         * seconds. You can override the value by passing the desired TTL with
         * {@link EVCache#set(String, Object, int)} operations.
         *
         * @param ttl
         *          Default is 900 seconds.
         * @return this {@code Builder} object
         */
        public EVCacheInternal.Builder setDefaultTTL(int ttl) {
            if (ttl < 0) throw new IllegalArgumentException("Time to Live cannot be less than 0.");
            this._ttl = ttl;
            return this;
        }

        /**
         * The default Time To Live (TTL) for items in {@link EVCache} in
         * seconds. You can override the value by passing the desired TTL with
         * {@link EVCache#set(String, Object, int)} operations.
         *
         * @param ttl Default is 900 seconds.
         * @return this {@code Builder} object
         */
        public EVCacheInternal.Builder setDefaultTTL(@Nullable final Duration ttl) {
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
        public <T> EVCacheInternal.Builder setTranscoder(Transcoder<T> transcoder) {
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
        public <T> EVCacheInternal.Builder enableZoneFallback() {
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
        public EVCacheInternal.Builder setRetry(boolean enableRetry) {
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
        public <T> EVCacheInternal.Builder enableRetry() {
            this._serverGroupRetry = true;
            return this;
        }

        /**
         * Will disable retry across Server Groups. This means if the data is
         * not found in one server group null is returned.
         *
         * @return this {@code Builder} object
         */
        public <T> EVCacheInternal.Builder disableRetry() {
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
        public <T> EVCacheInternal.Builder disableZoneFallback() {
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
        public EVCacheInternal.Builder setExceptionThrowing(boolean enableExceptionThrowing) {
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
        public <T> EVCacheInternal.Builder enableExceptionPropagation() {
            this._enableExceptionThrowing = true;
            return this;
        }

        /**
         * Adds customizers to be applied by {@code customize}.
         *
         * @param customizers List of {@code Customizer}s
         * @return this {@code Builder} object
         */
        public EVCacheInternal.Builder addCustomizers(@Nullable final List<EVCacheInternal.Builder.Customizer> customizers) {
            this._customizers.addAll(customizers);

            return this;
        }


        /**
         * Applies {@code Customizer}s added through {@code addCustomizers} to {@this}.
         *
         * @return this {@code Builder} object
         */
        public EVCacheInternal.Builder customize() {
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
        public EVCacheInternal.Builder customizeWith(final EVCacheInternal.Builder.Customizer customizer) {
            customizer.customize(this._appName, this);

            return this;
        }

        /**
         * Returns a newly created {@code EVCache} based on the contents of the
         * {@code Builder}.
         */
        @SuppressWarnings("deprecation")
        public EVCacheInternal build() {
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

            return new EVCacheInternalImpl(
                    _appName, _cachePrefix, _ttl, _transcoder, _serverGroupRetry, _enableExceptionThrowing, _poolManager);
        }
    }
}