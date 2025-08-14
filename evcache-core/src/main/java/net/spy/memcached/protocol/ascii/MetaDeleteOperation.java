package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;

/**
 * Meta Delete operation interface for advanced delete operations using memcached meta protocol.
 * Supports CAS-based conditional deletes, invalidation without deletion, and metadata retrieval.
 */
public interface MetaDeleteOperation extends Operation {

    /**
     * Operation callback for meta delete requests.
     */
    public interface Callback extends OperationCallback {
        /**
         * Callback for successful delete operation with metadata.
         *
         * @param key the key that was deleted/invalidated
         * @param deleted true if the item was deleted, false if invalidated or not found
         */
        void deleteComplete(String key, boolean deleted);
        
        /**
         * Callback for metadata returned during delete operation.
         *
         * @param key the key being deleted
         * @param flag the metadata flag
         * @param data the metadata value
         */
        void gotMetaData(String key, char flag, String data);
    }

    /**
     * Delete mode for different delete behaviors.
     */
    public enum DeleteMode {
        DELETE(""),         // Standard delete
        INVALIDATE("I");    // Invalidate (mark stale) instead of delete
        
        private final String flag;
        
        DeleteMode(String flag) {
            this.flag = flag;
        }
        
        public String getFlag() {
            return flag;
        }
    }

    /**
     * Builder for constructing meta delete operations with various options.
     */
    public static class Builder {
        private String key;
        private long cas = 0;
        private DeleteMode mode = DeleteMode.DELETE;
        private boolean returnCas = false;
        private boolean returnTtl = false;
        private boolean returnSize = false;
        private boolean quiet = false;
        
        public Builder key(String key) {
            this.key = key;
            return this;
        }
        
        public Builder cas(long cas) {
            this.cas = cas;
            return this;
        }
        
        public Builder mode(DeleteMode mode) {
            this.mode = mode;
            return this;
        }
        
        public Builder returnCas(boolean returnCas) {
            this.returnCas = returnCas;
            return this;
        }
        
        public Builder returnTtl(boolean returnTtl) {
            this.returnTtl = returnTtl;
            return this;
        }
        
        public Builder returnSize(boolean returnSize) {
            this.returnSize = returnSize;
            return this;
        }
        
        public Builder quiet(boolean quiet) {
            this.quiet = quiet;
            return this;
        }
        
        public String getKey() { return key; }
        public long getCas() { return cas; }
        public DeleteMode getMode() { return mode; }
        public boolean isReturnCas() { return returnCas; }
        public boolean isReturnTtl() { return returnTtl; }
        public boolean isReturnSize() { return returnSize; }
        public boolean isQuiet() { return quiet; }
        
        /**
         * Build a MetaDeleteOperation.Builder instance with current configuration.
         * This returns the builder itself since the builder pattern is being used directly.
         * 
         * @return this builder instance
         */
        public Builder build() {
            return this;
        }
    }
}