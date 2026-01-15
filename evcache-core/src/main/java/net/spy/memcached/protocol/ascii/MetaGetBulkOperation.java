package net.spy.memcached.protocol.ascii;

import java.util.Collection;

import com.netflix.evcache.operation.EVCacheItem;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;

/**
 * Operation for performing bulk meta get operations using memcached meta protocol.
 * Returns multiple keys with their metadata in a single efficient operation.
 */
public interface MetaGetBulkOperation extends Operation {

    /**
     * Callback interface for meta get bulk operations.
     */
    interface Callback extends OperationCallback {
        
        /**
         * Called when an item is found with data and metadata.
         * 
         * @param key The key that was retrieved
         * @param item The EVCacheItem containing data and metadata
         */
        void gotData(String key, EVCacheItem<Object> item);
        
        /**
         * Called when a key is not found in cache.
         * 
         * @param key The key that was not found
         */
        void keyNotFound(String key);
        
        /**
         * Called when the bulk operation is complete.
         * 
         * @param totalRequested Total number of keys requested
         * @param found Number of keys found
         * @param notFound Number of keys not found
         */
        void bulkComplete(int totalRequested, int found, int notFound);
    }
    
    /**
     * Configuration for meta get bulk operations.
     */
    class Config {
        private final Collection<String> keys;
        private boolean includeTtl = true;
        private boolean includeCas = true;
        private boolean includeSize = false;
        private boolean includeLastAccess = false;
        private boolean serveStale = false;
        private int maxStaleTime = 60; // seconds
        
        public Config(Collection<String> keys) {
            this.keys = keys;
        }
        
        public Collection<String> getKeys() { return keys; }
        public boolean isIncludeTtl() { return includeTtl; }
        public boolean isIncludeCas() { return includeCas; }
        public boolean isIncludeSize() { return includeSize; }
        public boolean isIncludeLastAccess() { return includeLastAccess; }
        public boolean isServeStale() { return serveStale; }
        public int getMaxStaleTime() { return maxStaleTime; }
        
        public Config includeTtl(boolean include) { this.includeTtl = include; return this; }
        public Config includeCas(boolean include) { this.includeCas = include; return this; }
        public Config includeSize(boolean include) { this.includeSize = include; return this; }
        public Config includeLastAccess(boolean include) { this.includeLastAccess = include; return this; }
        public Config serveStale(boolean serve) { this.serveStale = serve; return this; }
        public Config maxStaleTime(int seconds) { this.maxStaleTime = seconds; return this; }
    }
}