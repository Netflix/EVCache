package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;

/**
 * Meta Set operation interface for advanced set operations using memcached meta protocol.
 * Supports CAS, conditional sets, TTL modification, and atomic operations.
 */
public interface MetaSetOperation extends Operation {

    /**
     * Operation callback for meta set requests.
     */
    public interface Callback extends OperationCallback {
        /**
         * Callback for successful set operation with metadata.
         *
         * @param key the key that was set
         * @param cas the CAS value returned (if requested)
         * @param stored true if the item was stored, false otherwise
         */
        void setComplete(String key, long cas, boolean stored);
        
        /**
         * Callback for metadata returned during set operation.
         *
         * @param key the key being set
         * @param flag the metadata flag
         * @param data the metadata value
         */
        void gotMetaData(String key, char flag, String data);
    }

    /**
     * Meta set mode for different set behaviors.
     */
    public enum SetMode {
        SET("MS"),          // Standard set (Mode Set)
        ADD("ME"),          // Only add if not exists (Mode Exclusive)
        REPLACE("MR"),      // Only replace if exists (Mode Replace)
        APPEND("MA"),       // Append to existing value (Mode Append)
        PREPEND("MP");      // Prepend to existing value (Mode Prepend)

        private final String flag;
        
        SetMode(String flag) {
            this.flag = flag;
        }
        
        public String getFlag() {
            return flag;
        }
    }

    /**
     * Builder for constructing meta set operations with various options.
     */
    public static class Builder {
        private String key;
        private byte[] value;
        private int flags = 0;
        private int expiration = 0;
        private long cas = 0;
        private long recasid = 0;  // E flag: client-provided CAS to set after operation
        private SetMode mode = SetMode.SET;
        private boolean returnCas = false;
        private boolean returnTtl = false;
        private boolean markStale = false;
        
        public Builder key(String key) {
            this.key = key;
            return this;
        }
        
        public Builder value(byte[] value) {
            this.value = value;
            return this;
        }
        
        public Builder flags(int flags) {
            this.flags = flags;
            return this;
        }
        
        public Builder expiration(int expiration) {
            this.expiration = expiration;
            return this;
        }
        
        public Builder cas(long cas) {
            this.cas = cas;
            return this;
        }

        public Builder recasid(long recasid) {
            this.recasid = recasid;
            return this;
        }

        public Builder mode(SetMode mode) {
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
        
        public Builder markStale(boolean markStale) {
            this.markStale = markStale;
            return this;
        }
        
        public String getKey() { return key; }
        public byte[] getValue() { return value; }
        public int getFlags() { return flags; }
        public int getExpiration() { return expiration; }
        public long getCas() { return cas; }
        public long getRecasid() { return recasid; }
        public SetMode getMode() { return mode; }
        public boolean isReturnCas() { return returnCas; }
        public boolean isReturnTtl() { return returnTtl; }
        public boolean isMarkStale() { return markStale; }
        
        /**
         * Build a MetaSetOperation.Builder instance with current configuration.
         * This returns the builder itself since the builder pattern is being used directly.
         * 
         * @return this builder instance
         */
        public Builder build() {
            return this;
        }
    }
}